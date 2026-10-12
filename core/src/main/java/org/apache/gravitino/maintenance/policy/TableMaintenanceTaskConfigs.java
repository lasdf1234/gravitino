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
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Server configuration entries for per-task {@code minIntervalMs} (§7.3).
 *
 * <p>Keys follow {@code gravitino.maintenance.task.<type>.minIntervalMs}. Defaults match {@link
 * TableMaintenanceTaskType#defaultMinIntervalMs()}.
 */
public final class TableMaintenanceTaskConfigs {

  /** Compaction cooldown when policy content omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> COMPACTION_MIN_INTERVAL_MS =
      buildMinIntervalEntry(TableMaintenanceTaskType.COMPACTION);

  /** Manifest-rewrite cooldown when policy content omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MANIFEST_REWRITE_MIN_INTERVAL_MS =
      buildMinIntervalEntry(TableMaintenanceTaskType.MANIFEST_REWRITE);

  /** Snapshot-expiry cooldown when policy content omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> SNAPSHOT_EXPIRY_MIN_INTERVAL_MS =
      buildMinIntervalEntry(TableMaintenanceTaskType.SNAPSHOT_EXPIRY);

  /** Orphan-cleanup cooldown when policy content omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> ORPHAN_CLEANUP_MIN_INTERVAL_MS =
      buildMinIntervalEntry(TableMaintenanceTaskType.ORPHAN_CLEANUP);

  private static final Map<TableMaintenanceTaskType, ConfigEntry<Long>> ENTRIES;

  static {
    EnumMap<TableMaintenanceTaskType, ConfigEntry<Long>> entries =
        new EnumMap<>(TableMaintenanceTaskType.class);
    entries.put(TableMaintenanceTaskType.COMPACTION, COMPACTION_MIN_INTERVAL_MS);
    entries.put(TableMaintenanceTaskType.MANIFEST_REWRITE, MANIFEST_REWRITE_MIN_INTERVAL_MS);
    entries.put(TableMaintenanceTaskType.SNAPSHOT_EXPIRY, SNAPSHOT_EXPIRY_MIN_INTERVAL_MS);
    entries.put(TableMaintenanceTaskType.ORPHAN_CLEANUP, ORPHAN_CLEANUP_MIN_INTERVAL_MS);
    ENTRIES = Collections.unmodifiableMap(entries);
  }

  private TableMaintenanceTaskConfigs() {}

  /**
   * Returns the {@code minIntervalMs} config entry for a task type.
   *
   * @param taskType maintenance task type
   * @return config entry
   */
  public static ConfigEntry<Long> entryFor(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    return ENTRIES.get(taskType);
  }

  /**
   * Resolves minimum interval: policy content, then registered server config (with code default).
   *
   * @param config gravitino server config
   * @param taskType maintenance task type
   * @param policyMinIntervalMs value from effective policy content, or null
   * @return resolved interval in milliseconds
   */
  public static long resolveMinIntervalMs(
      Config config, TableMaintenanceTaskType taskType, @Nullable Long policyMinIntervalMs) {
    Preconditions.checkNotNull(config, "config cannot be null");
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    if (policyMinIntervalMs != null) {
      return policyMinIntervalMs;
    }
    return config.get(ENTRIES.get(taskType));
  }

  private static ConfigEntry<Long> buildMinIntervalEntry(TableMaintenanceTaskType taskType) {
    return new ConfigBuilder(MinIntervalMsResolver.configKey(taskType))
        .doc(
            "Minimum interval in milliseconds between finished runs for "
                + taskType.configKey()
                + " when the policy omits minIntervalMs")
        .version(ConfigConstants.VERSION_2_0_0)
        .longConf()
        .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
        .createWithDefault(taskType.defaultMinIntervalMs());
  }
}
