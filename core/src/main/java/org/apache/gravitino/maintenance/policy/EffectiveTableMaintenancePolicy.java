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
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Effective scheduling view for a table and maintenance task type.
 *
 * <p>Carries the nearest policy plus the schedule and resolved {@code minIntervalMs} used by the
 * TMS scheduler. Job submit options stay on the policy content and are read at {@code runJob} time.
 */
public final class EffectiveTableMaintenancePolicy {

  private final PolicyEntity policy;
  private final TableMaintenanceTaskType taskType;
  @Nullable private final TableMaintenanceSchedule schedule;
  private final long minIntervalMs;

  /**
   * Creates an effective policy view.
   *
   * @param policy nearest policy entity
   * @param taskType maintenance task type
   * @param schedule evaluate triggers from policy content, or null when unset
   * @param minIntervalMs resolved minimum interval
   */
  public EffectiveTableMaintenancePolicy(
      PolicyEntity policy,
      TableMaintenanceTaskType taskType,
      @Nullable TableMaintenanceSchedule schedule,
      long minIntervalMs) {
    this.policy = Preconditions.checkNotNull(policy, "policy");
    this.taskType = Preconditions.checkNotNull(taskType, "taskType");
    this.schedule = schedule;
    this.minIntervalMs = minIntervalMs;
  }

  /**
   * @return nearest policy entity
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return maintenance task type
   */
  public TableMaintenanceTaskType taskType() {
    return taskType;
  }

  /**
   * @return schedule from policy content, or null when unset
   */
  @Nullable
  public TableMaintenanceSchedule schedule() {
    return schedule;
  }

  /**
   * @return resolved minimum interval between runs
   */
  public long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * Resolves effective TMS policy for a metadata object.
   *
   * @param resolver nearest-policy resolver
   * @param metalake metalake name
   * @param metadataObject metadata object
   * @param taskType task type
   * @param config gravitino server configuration
   * @return effective policy when a matching policy exists
   */
  public static Optional<EffectiveTableMaintenancePolicy> resolve(
      NearestMaintenancePolicyResolver resolver,
      String metalake,
      MetadataObject metadataObject,
      TableMaintenanceTaskType taskType,
      Config config) {
    return resolver
        .resolveNearest(metalake, metadataObject, taskType)
        .map(
            policy -> {
              TableMaintenancePolicyFields fields =
                  TableMaintenancePolicyContentSupport.maintenanceFields(policy.content());
              TableMaintenanceSchedule schedule = fields == null ? null : fields.schedule();
              Long policyInterval = fields == null ? null : fields.minIntervalMs();
              long interval =
                  Configs.resolveMaintenanceMinIntervalMs(config, taskType, policyInterval);
              return new EffectiveTableMaintenancePolicy(policy, taskType, schedule, interval);
            });
  }
}
