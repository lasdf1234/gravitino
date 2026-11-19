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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergMaintenanceJob} orchestration. */
public class TestIcebergMaintenanceJob {

  @Test
  public void testTemplatePlaceholders() {
    SparkJobTemplate template = new IcebergMaintenanceJob().jobTemplate();
    assertEquals(IcebergMaintenanceJob.NAME, template.name());
    List<String> args = template.arguments();
    assertTrue(args.contains("--tasks"));
    assertTrue(args.contains("{{tasks}}"));
    assertFalse(args.contains("{{tasks:-}}"));
    assertTrue(args.contains("--updater-options"));
    assertTrue(args.contains("--spark-conf"));
    assertFalse(args.contains("--maintenance-types"));
    assertFalse(args.contains("--policy-ids"));
    assertFalse(args.contains("--tables"));
    assertFalse(args.contains("--strategy"));
  }

  @Test
  public void testRequireMaintenancePolicyType() {
    assertEquals(
        Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
        IcebergMaintenanceJob.requireMaintenancePolicyType("system_iceberg_snapshot_expiration"));
    assertEquals(
        Policy.BuiltInType.ICEBERG_COMPACTION,
        IcebergMaintenanceJob.requireMaintenancePolicyType("SYSTEM_ICEBERG_COMPACTION"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergMaintenanceJob.requireMaintenancePolicyType("system_row_filter"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergMaintenanceJob.requireMaintenancePolicyType(null));
  }

  @Test
  public void testRunRejectsInvalidArgs() {
    assertEquals(1, IcebergMaintenanceJob.run(new String[] {"--tasks", "[]"}));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--tasks",
              "[{\"table_id\":1,\"policy_type\":\"system_iceberg_snapshot_expiration\","
                  + "\"policy_id\":10,\"action_id\":100,\"params\":{}}]"
            }));
  }

  @Test
  public void testParseTasksKeepsPerTaskParams() {
    List<IcebergMaintenanceJob.MaintenanceTask> tasks =
        IcebergMaintenanceJob.parseTasks(
            "["
                + "{\"table_id\":1,\"policy_type\":\"system_iceberg_snapshot_expiration\","
                + "\"policy_id\":10,\"action_id\":100,\"params\":{\"catalog_name\":\"c\","
                + "\"table_identifier\":\"db.t1\",\"older_than\":\"2024-06-08 12:00:00Z\","
                + "\"retain_last\":\"10\"},\"rules\":{}},"
                + "{\"table_id\":2,\"policy_type\":\"system_iceberg_snapshot_expiration\","
                + "\"policy_id\":11,\"action_id\":101,\"params\":{\"catalog_name\":\"c\","
                + "\"table_identifier\":\"db.t2\",\"older_than\":\"2024-05-16 12:00:00Z\","
                + "\"retain_last\":\"3\"},\"rules\":{}}"
                + "]");
    assertEquals(2, tasks.size());
    assertEquals("c", tasks.get(0).catalogName);
    assertEquals("db.t1", tasks.get(0).tableIdentifier);
    assertEquals("2024-06-08 12:00:00Z", tasks.get(0).procedureArgs.get("older-than"));
    assertEquals("10", tasks.get(0).procedureArgs.get("retain-last"));
    assertFalse(tasks.get(0).procedureArgs.containsKey("catalog-name"));
    assertFalse(tasks.get(0).procedureArgs.containsKey("table-identifier"));
    assertEquals("2024-05-16 12:00:00Z", tasks.get(1).procedureArgs.get("older-than"));
    assertEquals("3", tasks.get(1).procedureArgs.get("retain-last"));
  }

  @Test
  public void testToProcedureArgMap() {
    Map<String, String> args =
        IcebergMaintenanceJob.toProcedureArgMap(
            Map.of(
                "catalog_name",
                "c",
                "table_identifier",
                "db.t",
                "strategy",
                "binpack",
                "where_clause",
                "dt=1",
                "sort_order",
                "id"));
    assertEquals("binpack", args.get("strategy"));
    assertEquals("dt=1", args.get("where"));
    assertEquals("id", args.get("sort-order"));
    assertFalse(args.containsKey("catalog-name"));
    assertFalse(args.containsKey("table-identifier"));
  }

  @Test
  public void testSnapshotDriftGateOnlyForCompactionAndManifest() {
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_COMPACTION)
            .shouldCheckSnapshotDrift());
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .shouldCheckSnapshotDrift());
    assertFalse(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .shouldCheckSnapshotDrift());
    assertFalse(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .shouldCheckSnapshotDrift());
  }

  @Test
  public void testMaintenanceActionsWireToJobClasses() {
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_COMPACTION)
            instanceof IcebergRewriteDataFilesJob);
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            instanceof IcebergRewriteManifestsJob);
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            instanceof IcebergExpireSnapshotsJob);
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            instanceof IcebergRemoveOrphanFilesJob);

    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_COMPACTION).hasBeforeMetrics());
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .hasBeforeMetrics());
    assertTrue(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .hasBeforeMetrics());
    assertFalse(
        IcebergMaintenanceJob.actionFor(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .hasBeforeMetrics());
  }
}
