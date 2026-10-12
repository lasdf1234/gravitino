/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Selects the nearest attached maintenance policy of a given task type for a metadata object.
 *
 * <p>Resolution walks the metadata object and its ancestors from nearest to outermost (for a table:
 * table → schema → catalog). At each level, only policies attached on that object are considered
 * ({@code inherited == false} from {@link PolicyDispatcher#listPolicyInfosForMetadataObject}).
 * Among matches at the same level, the policy with the lexicographically smallest name wins. Direct
 * policy–object bindings can be layered in once that storage exists.
 */
public class NearestMaintenancePolicyResolver {

  private final PolicyDispatcher policyDispatcher;

  /**
   * Creates a resolver.
   *
   * @param policyDispatcher policy dispatcher
   */
  public NearestMaintenancePolicyResolver(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = Preconditions.checkNotNull(policyDispatcher, "policyDispatcher");
  }

  /**
   * Returns the nearest enabled policy entity for the maintenance task type.
   *
   * @param metalake metalake name
   * @param metadataObject metadata object (typically a table)
   * @param taskType maintenance task type
   * @return matching policy when found
   */
  public Optional<PolicyEntity> resolveNearest(
      String metalake, MetadataObject metadataObject, TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(metalake, "metalake cannot be null");
    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    Preconditions.checkNotNull(taskType, "taskType cannot be null");

    for (MetadataObject level : resolutionOrder(metadataObject)) {
      Optional<PolicyEntity> match = firstDirectMatch(metalake, level, taskType);
      if (match.isPresent()) {
        return match;
      }
    }
    return Optional.empty();
  }

  private Optional<PolicyEntity> firstDirectMatch(
      String metalake, MetadataObject level, TableMaintenanceTaskType taskType) {
    PolicyEntity[] effective = policyDispatcher.listPolicyInfosForMetadataObject(metalake, level);
    List<PolicyEntity> matches = new ArrayList<>();
    for (PolicyEntity policy : effective) {
      if (matchesTaskType(policy, taskType) && !policy.inherited().orElse(false)) {
        matches.add(policy);
      }
    }
    if (matches.isEmpty()) {
      return Optional.empty();
    }
    matches.sort(Comparator.comparing(PolicyEntity::name));
    return Optional.of(matches.get(0));
  }

  private static List<MetadataObject> resolutionOrder(MetadataObject metadataObject) {
    List<MetadataObject> order = new ArrayList<>();
    order.add(metadataObject);
    order.addAll(MetadataObjectUtil.getParentMetadataObjects(metadataObject));
    return order;
  }

  private static boolean matchesTaskType(PolicyEntity policy, TableMaintenanceTaskType taskType) {
    return policy.enabled() && policy.policyType() == taskType.builtInType();
  }
}
