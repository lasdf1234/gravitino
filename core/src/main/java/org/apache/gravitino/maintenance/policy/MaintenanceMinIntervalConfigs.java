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
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Server config entries for TMS per-type {@code minIntervalMs} cooldowns in {@code gravitino.conf}.
 *
 * <p>Used when a maintenance policy omits {@code minIntervalMs}. Resolution order is policy content
 * → these server keys → {@link TableMaintenanceTaskType#defaultMinIntervalMs()}.
 */
public final class MaintenanceMinIntervalConfigs {

  /** Cooldown between finished compaction runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS =
      new ConfigBuilder(MinIntervalMsResolver.configKey(TableMaintenanceTaskType.COMPACTION))
          .doc(
              "Minimum interval in milliseconds between finished compaction runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(TableMaintenanceTaskType.COMPACTION.defaultMinIntervalMs());

  /**
   * Cooldown between finished manifest-rewrite runs when the policy omits {@code minIntervalMs}.
   */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_MANIFEST_REWRITE_MIN_INTERVAL_MS =
      new ConfigBuilder(MinIntervalMsResolver.configKey(TableMaintenanceTaskType.MANIFEST_REWRITE))
          .doc(
              "Minimum interval in milliseconds between finished manifest-rewrite runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(TableMaintenanceTaskType.MANIFEST_REWRITE.defaultMinIntervalMs());

  /** Cooldown between finished snapshot-expiry runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_SNAPSHOT_EXPIRY_MIN_INTERVAL_MS =
      new ConfigBuilder(MinIntervalMsResolver.configKey(TableMaintenanceTaskType.SNAPSHOT_EXPIRY))
          .doc(
              "Minimum interval in milliseconds between finished snapshot-expiry runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(TableMaintenanceTaskType.SNAPSHOT_EXPIRY.defaultMinIntervalMs());

  /** Cooldown between finished orphan-cleanup runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_ORPHAN_CLEANUP_MIN_INTERVAL_MS =
      new ConfigBuilder(MinIntervalMsResolver.configKey(TableMaintenanceTaskType.ORPHAN_CLEANUP))
          .doc(
              "Minimum interval in milliseconds between finished orphan-cleanup runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(TableMaintenanceTaskType.ORPHAN_CLEANUP.defaultMinIntervalMs());

  private MaintenanceMinIntervalConfigs() {}

  /**
   * Returns the {@code minIntervalMs} config entry for a maintenance task type.
   *
   * @param taskType maintenance task type
   * @return config entry
   */
  public static ConfigEntry<Long> forTaskType(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    switch (taskType) {
      case COMPACTION:
        return MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS;
      case MANIFEST_REWRITE:
        return MAINTENANCE_TASK_MANIFEST_REWRITE_MIN_INTERVAL_MS;
      case SNAPSHOT_EXPIRY:
        return MAINTENANCE_TASK_SNAPSHOT_EXPIRY_MIN_INTERVAL_MS;
      case ORPHAN_CLEANUP:
        return MAINTENANCE_TASK_ORPHAN_CLEANUP_MIN_INTERVAL_MS;
      default:
        throw new IllegalArgumentException("Unknown maintenance task type: " + taskType);
    }
  }

  /**
   * Resolves minimum interval: policy content, then server config entry (with code default).
   *
   * @param config gravitino server config
   * @param taskType maintenance task type
   * @param policyMinIntervalMs value from effective policy content, or null
   * @return resolved interval in milliseconds
   */
  public static long resolve(
      Config config, TableMaintenanceTaskType taskType, @Nullable Long policyMinIntervalMs) {
    Preconditions.checkNotNull(config, "config cannot be null");
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    if (policyMinIntervalMs != null) {
      return policyMinIntervalMs;
    }
    return config.get(forTaskType(taskType));
  }
}
