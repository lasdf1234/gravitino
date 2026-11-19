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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceTriggerGate}. */
public class TestMaintenanceTriggerGate {

  @Test
  public void testExpireSnapshotsAlwaysMaintain() {
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            new HashMap<>(),
            "{\"custom-snapshot-number\":1}"));
  }

  @Test
  public void testCompactionUsesEnqueueRules() {
    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "custom-data-file-mse >= minDataFileMse");
    rules.put("minDataFileMse", 10);

    String below =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":5,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":0}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(Policy.BuiltInType.ICEBERG_COMPACTION, rules, below));

    String above =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":11,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":0}";
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(Policy.BuiltInType.ICEBERG_COMPACTION, rules, above));
  }

  @Test
  public void testManifestRewriteUsesDefaultThresholds() {
    // Default threshold=100 (Iceberg commit.manifest.min-count-to-merge).
    String belowThreshold =
        "{\"" + IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC + "\":99}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new HashMap<>(), belowThreshold));

    String atThreshold = "{\"" + IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC + "\":100}";
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new HashMap<>(), atThreshold));

    String aboveThreshold =
        "{\"" + IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC + "\":500}";
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new HashMap<>(), aboveThreshold));
  }

  @Test
  public void testManifestRewriteUsesEnqueueRules() {
    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "custom-manifest-number >= manifest_count_threshold");
    rules.put("manifest_count_threshold", 10);

    String below = "{\"" + IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC + "\":5}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, rules, below));

    String above = "{\"" + IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC + "\":10}";
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, rules, above));
  }

  @Test
  public void testManifestRewriteSkipsWhenMetricsMissing() {
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new HashMap<>(), "{}"));
  }

  @Test
  public void testOrphanEvaluatesTriggerExprWithoutBeforeMetrics() {
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, new HashMap<>(), null));

    Map<String, Object> skip = new HashMap<>();
    skip.put("trigger-expr", "false");
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, skip, null));
  }

  @Test
  public void testCompactionSkipsWhenMetricsMissing() {
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION, new HashMap<>(), "{}"));

    // Gate requires both compaction S0 fields even if custom expr only names one.
    String onlyMse = "{\"" + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC + "\":15}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION, new HashMap<>(), onlyMse));
  }

  @Test
  public void testMalformedTriggerExprSkipsNotThrows() {
    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "(((( broken");
    String metrics =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":15,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":2}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION, rules, metrics));
  }
}
