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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionTriggerEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.manifest.ManifestRewriteTriggerEvaluator;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Spark maintain-or-skip gate after S0 metrics (§5.2.2).
 *
 * <p>Evaluates enqueue-time policy rules locally with the same QL stack as the optimizer. Does not
 * call Gravitino. Snapshot expiry always maintains (optimizer-only scheduling for that type).
 * Compaction uses {@code CompactionTriggerEvaluator}; manifest rewrite uses {@code
 * ManifestRewriteTriggerEvaluator}.
 */
final class MaintenanceTriggerGate {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceTriggerGate.class);

  private MaintenanceTriggerGate() {}

  /**
   * @param policyType Iceberg maintenance {@link Policy.BuiltInType}
   * @param rules per-task policy {@code rules()} object (may be empty → type defaults)
   * @param metricsJson S0 metrics JSON
   * @return whether the procedure should run
   */
  static boolean shouldMaintain(
      Policy.BuiltInType policyType,
      @Nullable Map<String, Object> rules,
      @Nullable String metricsJson) {
    if (policyType == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      return true;
    }

    Map<String, Object> effectiveRules = effectiveRules(policyType, rules);
    Map<String, Object> metrics = parseNumericMetrics(metricsJson);
    // Compaction / manifest need some numeric metrics; do not require every known field — a custom
    // trigger-expr may reference only a subset. Empty metrics still skip (nothing to evaluate).
    if ((policyType == Policy.BuiltInType.ICEBERG_COMPACTION
            || policyType == Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
        && metrics.isEmpty()) {
      LOG.warn(
          "{} recommend skip: metrics JSON empty or non-numeric",
          policyType == Policy.BuiltInType.ICEBERG_COMPACTION ? "Compaction" : "Manifest rewrite");
      return false;
    }

    boolean maintain =
        policyType == Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS
            ? ManifestRewriteTriggerEvaluator.shouldMaintain(effectiveRules, metrics)
            : CompactionTriggerEvaluator.shouldMaintain(effectiveRules, metrics);
    String triggerExprKey =
        policyType == Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS
            ? IcebergRewriteManifestsContent.TRIGGER_EXPR_KEY
            : IcebergDataCompactionContent.TRIGGER_EXPR_KEY;
    LOG.info(
        "In-Spark recommend type={} expr={} -> {}",
        policyType.policyType(),
        effectiveRules.get(triggerExprKey),
        maintain ? "MAINTAIN" : "SKIP");
    return maintain;
  }

  private static Map<String, Object> effectiveRules(
      Policy.BuiltInType policyType, @Nullable Map<String, Object> rules) {
    if (rules != null && !rules.isEmpty()) {
      return rules;
    }
    switch (policyType) {
      case ICEBERG_COMPACTION:
        return new HashMap<>(PolicyContents.icebergDataCompaction().rules());
      case ICEBERG_REWRITE_MANIFESTS:
        return new HashMap<>(PolicyContents.icebergRewriteManifests().rules());
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return new HashMap<>(PolicyContents.icebergOrphanFileRemoval().rules());
      default:
        Map<String, Object> empty = new HashMap<>();
        empty.put(IcebergOrphanFileRemovalContent.TRIGGER_EXPR_KEY, "false");
        return empty;
    }
  }

  private static Map<String, Object> parseNumericMetrics(@Nullable String metricsJson) {
    Map<String, Object> metrics = new HashMap<>();
    if (metricsJson == null || metricsJson.trim().isEmpty()) {
      return metrics;
    }
    try {
      JsonNode root = JsonUtils.anyFieldMapper().readTree(metricsJson);
      Iterator<Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> field = fields.next();
        JsonNode value = field.getValue();
        if (value == null || value.isNull() || !value.isNumber()) {
          continue;
        }
        metrics.put(
            field.getKey(), value.isIntegralNumber() ? value.longValue() : value.doubleValue());
      }
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to parse metrics JSON: {}", e.toString());
    }
    return metrics;
  }
}