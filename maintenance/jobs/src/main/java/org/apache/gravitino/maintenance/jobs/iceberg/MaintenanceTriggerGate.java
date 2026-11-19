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
import org.apache.gravitino.maintenance.optimizer.recommender.handler.TriggerExprEvaluator;
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
 * <p>Evaluates enqueue-time policy rules locally via {@link TriggerExprEvaluator}. Does not call
 * Gravitino. Snapshot expiry always maintains (optimizer-only scheduling for that type). Compaction
 * / rewrite-manifests require their S0 numeric fields before evaluation.
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
    if (!hasRequiredMetrics(policyType, metrics)) {
      return false;
    }

    boolean maintain = TriggerExprEvaluator.evaluate(effectiveRules, metrics);
    LOG.info(
        "In-Spark recommend type={} expr={} -> {}",
        policyType.policyType(),
        effectiveRules.get(TriggerExprEvaluator.TRIGGER_EXPR_KEY),
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
        Map<String, Object> fallback = new HashMap<>();
        fallback.put(IcebergOrphanFileRemovalContent.TRIGGER_EXPR_KEY, "false");
        return fallback;
    }
  }

  /**
   * Compaction needs mse + delete-file-number; rewrite-manifests needs manifest-number. Other types
   * (orphan) evaluate without S0 metrics.
   */
  private static boolean hasRequiredMetrics(
      Policy.BuiltInType policyType, Map<String, Object> metrics) {
    switch (policyType) {
      case ICEBERG_COMPACTION:
        if (metrics.isEmpty()
            || !metrics.containsKey(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC)
            || !metrics.containsKey(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC)) {
          LOG.warn("Compaction recommend skip: metrics JSON missing compaction fields");
          return false;
        }
        return true;
      case ICEBERG_REWRITE_MANIFESTS:
        if (metrics.isEmpty()
            || !metrics.containsKey(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC)) {
          LOG.warn("Manifest rewrite recommend skip: metrics JSON missing manifest fields");
          return false;
        }
        return true;
      default:
        return true;
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
