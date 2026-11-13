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
package org.apache.gravitino.maintenance.jobs.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.jobs.MaintenanceActionHttpClient;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TMS Spark job: collect before-metrics → gate → execute procedure → report after-metrics → finish
 * the maintenance action.
 *
 * <p>Started only from the built-in template (cron or commit-chain). JobConf carries a {@code
 * tasks} JSON array: each element is one work item with {@code table_id}, {@code maintenance_type},
 * {@code policy_id}, {@code action_id}, {@code params} (including {@code catalog_name}/{@code
 * table_identifier} plus procedure flags), and gate {@code rules}. Cron snapshot-expiry batches and
 * commit chains share this shape.
 *
 * <p>{@code metricsJson} carries only metric fields (no {@code maintenanceType} / {@code
 * snapshotId}; those live on the action/policy and {@code table_snapshot_metrics.snapshot_id}).
 * Snapshot drift skips only compaction and manifest rewrite. Orphan cleanup reports after-metrics
 * only.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /** Built-in template name used by TMS submit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /** Bump when template behavior / args / class wiring changes. */
  private static final String VERSION = "v2";

  /** JobConf / CLI: JSON array of per-work-item task objects. */
  public static final String TASKS_KEY = "tasks";

  /**
   * Maps schedule/jobConf underscore param keys onto the hyphenated names {@link
   * IcebergMaintenanceAction} reads from {@code argsByName}.
   */
  private static final Map<String, String> PARAM_KEY_TO_ARG =
      ImmutableMap.<String, String>builder()
          .put("strategy", "strategy")
          .put("sort_order", "sort-order")
          .put("where_clause", "where")
          .put("options", "options")
          .put("use_caching", "use-caching")
          .put("spec_id", "spec-id")
          .put("older_than", "older-than")
          .put("retain_last", "retain-last")
          .put("stream_results", "stream-results")
          .put("location", "location")
          .put("dry_run", "dry-run")
          .build();

  /** Values accepted in {@code maintenance_type}. */
  public enum MaintenanceType {
    REWRITE_DATA_FILES("rewrite_data_files"),
    REWRITE_MANIFESTS("rewrite_manifests"),
    EXPIRE_SNAPSHOTS("expire_snapshots"),
    REMOVE_ORPHAN_FILES("remove_orphan_files");

    private final String wireName;

    MaintenanceType(String wireName) {
      this.wireName = wireName;
    }

    /**
     * @return jobConf wire value
     */
    public String wireName() {
      return wireName;
    }

    /**
     * @return the built-in job that implements this maintenance type
     */
    public IcebergMaintenanceAction action() {
      return ACTIONS.get(this);
    }

    /**
     * @param raw one maintenance type wire name
     * @return parsed type
     */
    public static MaintenanceType fromWire(@Nullable String raw) {
      if (raw == null || raw.trim().isEmpty()) {
        throw new IllegalArgumentException("maintenance_type is required");
      }
      String normalized = raw.trim().toLowerCase(Locale.ROOT);
      for (MaintenanceType type : values()) {
        if (type.wireName.equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "Unknown maintenance_type value '"
              + raw
              + "'. Expected one of: rewrite_data_files, rewrite_manifests, expire_snapshots,"
              + " remove_orphan_files");
    }
  }

  private static final Map<MaintenanceType, IcebergMaintenanceAction> ACTIONS = createActions();

  private static Map<MaintenanceType, IcebergMaintenanceAction> createActions() {
    Map<MaintenanceType, IcebergMaintenanceAction> actions = new EnumMap<>(MaintenanceType.class);
    actions.put(MaintenanceType.REWRITE_DATA_FILES, new IcebergRewriteDataFilesJob());
    actions.put(MaintenanceType.REWRITE_MANIFESTS, new IcebergRewriteManifestsJob());
    actions.put(MaintenanceType.EXPIRE_SNAPSHOTS, new IcebergExpireSnapshotsJob());
    actions.put(MaintenanceType.REMOVE_ORPHAN_FILES, new IcebergRemoveOrphanFilesJob());
    return Collections.unmodifiableMap(actions);
  }

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "TMS Iceberg maintenance (before metrics, gate, procedure, after metrics, finish)")
        .withExecutable(resolveExecutable(IcebergMaintenanceJob.class))
        .withClassName(IcebergMaintenanceJob.class.getName())
        .withArguments(templateArguments())
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Spark entry after template expansion.
   *
   * @param args named jobConf arguments
   */
  public static void main(String[] args) {
    int code = run(args);
    if (code != 0) {
      System.exit(code);
    }
  }

  /**
   * @param args named jobConf arguments
   * @return {@code 0} on success, {@code 1} on failure
   */
  static int run(String[] args) {
    Map<String, String> argsByName = IcebergJobUtils.parseArguments(args);

    List<MaintenanceTask> tasks;
    Map<String, String> updaterOptions;
    try {
      tasks = parseTasks(IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("tasks")));
      updaterOptions =
          parseUpdaterOptions(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("updater-options")));
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOG.error("Invalid TMS maintenance arguments: {}", e.getMessage());
      return 1;
    }

    String appLabel = appLabel(tasks);
    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Iceberg Maintenance Task (" + appLabel + ")");
    String sparkConfJson =
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spark-conf"));
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      Map<String, String> sparkConfigs =
          new HashMap<>(IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson));
      ensureIcebergCatalogImpl(sparkConfigs);
      sparkConfigs.forEach(sparkBuilder::config);
    }

    try (SparkSession spark = sparkBuilder.getOrCreate()) {
      IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);
      try (MaintenanceActionHttpClient client =
          MaintenanceActionHttpClient.fromUpdaterOptions(updaterOptions)) {
        Set<String> finishedKeys = new HashSet<>();
        List<Long> actionIds = tasks.stream().map(t -> t.actionId).collect(Collectors.toList());
        try {
          for (MaintenanceTask task : tasks) {
            String key = task.tableId + ":" + task.policyId + ":" + task.actionId;
            try {
              runMaintenanceForTask(spark, client, task);
            } finally {
              finishMaintenanceAction(client, task.actionId, task.tableId, task.policyId);
              finishedKeys.add(key);
            }
          }
          return 0;
        } catch (Exception e) {
          LOG.error("TMS Iceberg maintenance failed for action_ids {}", actionIds, e);
          return 1;
        } finally {
          for (MaintenanceTask task : tasks) {
            String key = task.tableId + ":" + task.policyId + ":" + task.actionId;
            if (!finishedKeys.contains(key)) {
              finishMaintenanceAction(client, task.actionId, task.tableId, task.policyId);
            }
          }
        }
      }
    }
  }

  private static String appLabel(List<MaintenanceTask> tasks) {
    List<String> types =
        tasks.stream().map(t -> t.type.wireName).distinct().collect(Collectors.toList());
    if (types.size() == 1 && tasks.size() == 1) {
      return types.get(0);
    }
    if (types.size() == 1) {
      return "batch:" + types.get(0) + ":" + tasks.size();
    }
    return "chain:" + String.join(",", types);
  }

  /**
   * Shared S0 → gate → procedure → S1 pipeline. Types without S0 (orphan cleanup) skip the before
   * half and still report after-metrics.
   */
  private static void runMaintenanceForTask(
      SparkSession spark, MaintenanceActionHttpClient client, MaintenanceTask task)
      throws Exception {
    IcebergMaintenanceAction action = task.type.action();
    TableSnapshotMetrics before = null;
    if (action.hasBeforeMetrics()) {
      before = action.collectBeforeMetrics(spark, task.catalogName, task.tableIdentifier);
      if (!reportBeforeMetricsOrStop(client, task.actionId, task.tableId, task.policyId, before)) {
        LOG.info(
            "TMS type {} stopped for action_id {} table {} policy {}: before-metrics failed"
                + " (abandoned or unbound)",
            task.type.wireName,
            task.actionId,
            task.tableId,
            task.policyId);
        return;
      }
      long headSnapshotId =
          IcebergJobUtils.requireCurrentSnapshotId(spark, task.catalogName, task.tableIdentifier);
      if (action.shouldCheckSnapshotDrift() && before.snapshotId() != headSnapshotId) {
        LOG.info(
            "TMS type {} skipped for action_id {} table {} policy {}: snapshot drifted"
                + " (sampled={} head={})",
            task.type.wireName,
            task.actionId,
            task.tableId,
            task.policyId,
            before.snapshotId(),
            headSnapshotId);
        return;
      }
      if (!MaintenanceTriggerGate.shouldMaintain(task.type, task.rules, before.metricsJson())) {
        LOG.info(
            "TMS type {} skipped for action_id {} table {} policy {}: trigger-expr skip",
            task.type.wireName,
            task.actionId,
            task.tableId,
            task.policyId);
        return;
      }
    }

    TableSnapshotMetrics after =
        action.executeAndCollectAfter(
            spark, task.catalogName, task.tableIdentifier, task.procedureArgs, before);
    reportAfterMetrics(client, task.actionId, task.tableId, task.policyId, after);
  }

  /**
   * Snapshot drift skip applies only to compaction and manifest rewrite (inventory-sensitive).
   * Expiry and orphan cleanup always proceed past HEAD drift.
   */
  @VisibleForTesting
  static boolean shouldCheckSnapshotDrift(MaintenanceType type) {
    return type.action().shouldCheckSnapshotDrift();
  }

  @VisibleForTesting
  static List<MaintenanceTask> parseTasks(@Nullable String tasksJson) {
    if (tasksJson == null || tasksJson.trim().isEmpty()) {
      throw new IllegalArgumentException("tasks JSON is required (non-empty array)");
    }
    try {
      List<Map<String, Object>> raw =
          JsonUtils.anyFieldMapper()
              .readValue(tasksJson, new TypeReference<List<Map<String, Object>>>() {});
      if (raw == null || raw.isEmpty()) {
        throw new IllegalArgumentException("tasks must not be empty");
      }
      List<MaintenanceTask> tasks = new ArrayList<>(raw.size());
      for (Map<String, Object> entry : raw) {
        tasks.add(parseTask(entry));
      }
      return tasks;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("tasks must be valid JSON: " + e.getMessage(), e);
    }
  }

  private static MaintenanceTask parseTask(@Nullable Map<String, Object> entry) {
    if (entry == null) {
      throw new IllegalArgumentException("tasks entry must not be null");
    }
    long tableId = requirePositiveLong(entry.get("table_id"), "table_id");
    MaintenanceType type = MaintenanceType.fromWire(stringOrNull(entry.get("maintenance_type")));
    long policyId = requirePositiveLong(entry.get("policy_id"), "policy_id");
    long actionId = requirePositiveLong(entry.get("action_id"), "action_id");
    Map<String, String> params = asStringMap(entry.get("params"));
    String catalogName = requireNonBlank(params.get("catalog_name"), "params.catalog_name");
    String tableIdentifier =
        requireNonBlank(params.get("table_identifier"), "params.table_identifier");
    Map<String, String> procedureArgs = toProcedureArgMap(params);
    Map<String, Object> rules = asObjectMap(entry.get("rules"));
    return new MaintenanceTask(
        tableId, catalogName, tableIdentifier, type, policyId, actionId, procedureArgs, rules);
  }

  @VisibleForTesting
  static Map<String, String> toProcedureArgMap(@Nullable Map<String, String> params) {
    Map<String, String> args = new HashMap<>();
    if (params == null || params.isEmpty()) {
      return args;
    }
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String key = entry.getKey();
      if (key == null
          || "catalog_name".equals(key)
          || "table_identifier".equals(key)
          || entry.getValue() == null
          || entry.getValue().trim().isEmpty()) {
        continue;
      }
      String argKey = PARAM_KEY_TO_ARG.get(key);
      if (argKey == null) {
        argKey = key.replace('_', '-');
      }
      args.put(argKey, entry.getValue());
    }
    return args;
  }

  private static Map<String, String> asStringMap(@Nullable Object raw) {
    Map<String, String> out = new LinkedHashMap<>();
    if (raw == null) {
      return out;
    }
    if (!(raw instanceof Map)) {
      throw new IllegalArgumentException("params must be a JSON object");
    }
    Map<?, ?> map = (Map<?, ?>) raw;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      out.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
    return out;
  }

  private static Map<String, Object> asObjectMap(@Nullable Object raw) {
    Map<String, Object> out = new LinkedHashMap<>();
    if (raw == null) {
      return out;
    }
    if (!(raw instanceof Map)) {
      throw new IllegalArgumentException("rules must be a JSON object");
    }
    Map<?, ?> map = (Map<?, ?>) raw;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      out.put(String.valueOf(entry.getKey()), entry.getValue());
    }
    return out;
  }

  private static long requirePositiveLong(@Nullable Object value, String field) {
    if (value == null) {
      throw new IllegalArgumentException(field + " is required");
    }
    try {
      long id = Long.parseLong(String.valueOf(value));
      if (id <= 0) {
        throw new IllegalArgumentException(field + " must be positive: " + id);
      }
      return id;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(field + " must be a long", e);
    }
  }

  private static String requireNonBlank(@Nullable Object value, String field) {
    String text = stringOrNull(value);
    if (text == null || text.trim().isEmpty()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return text.trim();
  }

  @Nullable
  private static String stringOrNull(@Nullable Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static void finishMaintenanceAction(
      MaintenanceActionHttpClient client, long actionId, long tableId, long policyId) {
    try {
      client.finish(actionId, new MaintenanceActionFinishRequest(tableId, policyId));
    } catch (Exception e) {
      LOG.warn(
          "Best-effort TMS finish failed for action_id {} table {} policy {}",
          actionId,
          tableId,
          policyId,
          e);
    }
  }

  /**
   * Reports before-metrics. Failure means the TMS action is abandoned/unbound — do not run the
   * procedure.
   */
  private static boolean reportBeforeMetricsOrStop(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      TableSnapshotMetrics sample) {
    if (sample.metricsJson() == null || sample.metricsJson().trim().isEmpty()) {
      return false;
    }
    try {
      client.reportBeforeMetrics(
          actionId,
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId(), sample.metricsJson()));
      return true;
    } catch (Exception e) {
      LOG.warn(
          "TMS before metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId(),
          e.toString());
      return false;
    }
  }

  private static void reportAfterMetrics(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      TableSnapshotMetrics sample) {
    if (sample.metricsJson() == null || sample.metricsJson().trim().isEmpty()) {
      LOG.warn(
          "TMS after metrics skipped for action_id {} table {} policy {}: empty metrics JSON",
          actionId,
          tableId,
          policyId);
      return;
    }
    try {
      client.reportAfterMetrics(
          actionId,
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId(), sample.metricsJson()));
    } catch (Exception e) {
      LOG.warn(
          "TMS after metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId(),
          e.toString());
    }
  }

  private static Map<String, String> parseUpdaterOptions(@Nullable String json) {
    if (json == null || json.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      @SuppressWarnings("unchecked")
      Map<String, String> parsed = JsonUtils.anyFieldMapper().readValue(json, Map.class);
      return parsed == null ? Collections.emptyMap() : parsed;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid updater-options JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Ensures {@code spark.sql.catalog.<name>} points at Iceberg {@code SparkCatalog} whenever nested
   * catalog connection keys are present. Needed because TMS schedule {@code spark_conf} may only
   * carry {@code .type}/{@code .uri}/{@code .warehouse}.
   */
  private static void ensureIcebergCatalogImpl(Map<String, String> sparkConfigs) {
    String prefix = "spark.sql.catalog.";
    Set<String> catalogs = new HashSet<>();
    for (String key : sparkConfigs.keySet()) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      String rest = key.substring(prefix.length());
      int dot = rest.indexOf('.');
      if (dot <= 0) {
        continue;
      }
      String prop = rest.substring(dot + 1);
      if ("type".equals(prop) || "uri".equals(prop) || "warehouse".equals(prop)) {
        catalogs.add(rest.substring(0, dot));
      }
    }
    for (String catalog : catalogs) {
      sparkConfigs.putIfAbsent(prefix + catalog, "org.apache.iceberg.spark.SparkCatalog");
      sparkConfigs.putIfAbsent("spark.sql.defaultCatalog", catalog);
    }
    sparkConfigs.putIfAbsent(
        "spark.sql.extensions", IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS);
  }

  private static List<String> templateArguments() {
    return Arrays.asList(
        "--tasks",
        "{{tasks}}",
        "--updater-options",
        "{{updater_options:-}}",
        "--spark-conf",
        "{{spark_conf:-}}");
  }

  /** One ordered maintenance work item inside this Spark job. */
  static final class MaintenanceTask {
    final long tableId;
    final String catalogName;
    final String tableIdentifier;
    final MaintenanceType type;
    final long policyId;
    final long actionId;
    final Map<String, String> procedureArgs;
    final Map<String, Object> rules;

    private MaintenanceTask(
        long tableId,
        String catalogName,
        String tableIdentifier,
        MaintenanceType type,
        long policyId,
        long actionId,
        Map<String, String> procedureArgs,
        Map<String, Object> rules) {
      this.tableId = tableId;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
      this.type = type;
      this.policyId = policyId;
      this.actionId = actionId;
      this.procedureArgs = procedureArgs;
      this.rules = rules;
    }
  }
}
