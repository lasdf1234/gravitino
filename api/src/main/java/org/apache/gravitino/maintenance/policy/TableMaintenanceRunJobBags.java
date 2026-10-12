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

import java.util.Map;
import java.util.Objects;

/**
 * Runtime {@code jobOptions} and {@code updateOptions} bags passed to TMS {@code runJob}.
 *
 * <p>Built from nearest effective policy peers plus cluster-static auth overlays from {@code
 * gravitino.conf} ({@link MaintenanceAuthConfigKeys}).
 */
public final class TableMaintenanceRunJobBags {

  private final Map<String, String> jobOptions;
  private final Map<String, String> updateOptions;

  private TableMaintenanceRunJobBags(
      Map<String, String> jobOptions, Map<String, String> updateOptions) {
    this.jobOptions = jobOptions;
    this.updateOptions = updateOptions;
  }

  /**
   * Merges policy peers with cluster auth overlays for {@code runJob}.
   *
   * <p>Policy options are the farther map; cluster auth is nearer and wins on duplicate keys.
   *
   * @param policyJobOptions non-sensitive options from effective policy {@code jobOptions}
   * @param policyUpdateOptions non-sensitive options from effective policy {@code updateOptions}
   * @param gravitinoAuth entries under {@link MaintenanceAuthConfigKeys#GRAVITINO_AUTH_PREFIX}
   * @param ircAuth entries under {@link MaintenanceAuthConfigKeys#IRC_AUTH_PREFIX}
   * @return merged bags for the job template
   */
  public static TableMaintenanceRunJobBags overlayAtRunJob(
      Map<String, String> policyJobOptions,
      Map<String, String> policyUpdateOptions,
      Map<String, String> gravitinoAuth,
      Map<String, String> ircAuth) {
    return new TableMaintenanceRunJobBags(
        TableMaintenancePolicies.mergeOptions(policyJobOptions, ircAuth),
        TableMaintenancePolicies.mergeOptions(policyUpdateOptions, gravitinoAuth));
  }

  /**
   * @return Spark / IRC {@code jobOptions} for {@code runJob}
   */
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  /**
   * @return Gravitino updater {@code updateOptions} for {@code runJob}
   */
  public Map<String, String> updateOptions() {
    return updateOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableMaintenanceRunJobBags)) {
      return false;
    }
    TableMaintenanceRunJobBags that = (TableMaintenanceRunJobBags) o;
    return Objects.equals(jobOptions, that.jobOptions)
        && Objects.equals(updateOptions, that.updateOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobOptions, updateOptions);
  }

  @Override
  public String toString() {
    return "TableMaintenanceRunJobBags{jobOptions="
        + jobOptions
        + ", updateOptions="
        + updateOptions
        + '}';
  }
}
