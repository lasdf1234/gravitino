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
package org.apache.gravitino.maintenance.scheduler.job;

import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.policy.Policy;

/**
 * Shared TMS job template name and jobConf keys for cron and commit submits.
 *
 * <p>TMS always submits {@link #ICEBERG_MAINTENANCE_TEMPLATE}; standalone {@code builtin-iceberg-*}
 * templates remain for CLI / optimizer.
 */
public final class MaintenanceJobTemplates {

  /**
   * Built-in template name for the Iceberg maintenance Spark job. Must match {@code
   * IcebergMaintenanceJob.NAME} ({@code builtin-iceberg-maintenance-task}).
   */
  public static final String ICEBERG_MAINTENANCE_TEMPLATE =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /**
   * JobConf key for the ordered maintenance work list (JSON array). Each element carries table,
   * {@code policy_type}, {@code policy_id}, {@code action_id}, procedure {@code params}, and gate
   * {@code rules}. Used by both cron (one or many tables) and commit-chain submits.
   */
  public static final String TASKS_KEY = "tasks";

  /**
   * Deprecated flat dispatch key retained for schedule-time parameter maps and tests. Runtime Spark
   * submits use {@link #TASKS_KEY} instead. Values are {@link Policy.BuiltInType#policyType()}.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /**
   * Deprecated flat policy-id list; runtime Spark submits embed {@code policy_id} on each {@link
   * #TASKS_KEY} element.
   */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /** JobConf key for overlaid updater options (JSON object string). */
  public static final String UPDATER_OPTIONS_JOB_CONF_KEY = "updater_options";

  /**
   * Schedule-time per-task parameter key for policy {@code rules()} (JSON map of {@code
   * policy_type} → rules object). At Spark submit this is projected into each {@link #TASKS_KEY}
   * element's {@code rules} object.
   */
  public static final String RULES_KEY = "rules";

  private MaintenanceJobTemplates() {}

  /**
   * @param builtInType policy type
   * @return whether TMS Iceberg maintenance jobs handle this type
   */
  public static boolean isIcebergMaintenance(Policy.BuiltInType builtInType) {
    if (builtInType == null) {
      return false;
    }
    switch (builtInType) {
      case ICEBERG_COMPACTION:
      case ICEBERG_REWRITE_MANIFESTS:
      case ICEBERG_SNAPSHOT_EXPIRATION:
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return true;
      default:
        return false;
    }
  }
}
