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
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
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
  public void testMaintenanceTypeFromWire() {
    assertEquals(MaintenanceType.EXPIRE_SNAPSHOTS, MaintenanceType.fromWire("expire_snapshots"));
    assertEquals(
        MaintenanceType.REWRITE_DATA_FILES, MaintenanceType.fromWire("REWRITE_DATA_FILES"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire("unknown"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire(null));
  }

  @Test
  public void testRunRejectsInvalidArgs() {
    assertEquals(1, IcebergMaintenanceJob.run(new String[] {"--tasks", "[]"}));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--tasks",
              "[{\"table_id\":1,\"catalog_name\":\"c\",\"table_identifier\":\"db.t\","
                  + "\"maintenance_type\":\"expire_snapshots\",\"policy_id\":10}]"
            }));
  }

  @Test
  public void testParseTasksKeepsPerTaskParams() {
    List<IcebergMaintenanceJob.MaintenanceTask> tasks =
        IcebergMaintenanceJob.parseTasks(
            "["
                + "{\"table_id\":1,\"catalog_name\":\"c\",\"table_identifier\":\"db.t1\","
                + "\"maintenance_type\":\"expire_snapshots\",\"policy_id\":10,\"action_id\":100,"
                + "\"params\":{\"older_than\":\"7d\",\"retain_last\":\"10\"},\"rules\":{}},"
                + "{\"table_id\":2,\"catalog_name\":\"c\",\"table_identifier\":\"db.t2\","
                + "\"maintenance_type\":\"expire_snapshots\",\"policy_id\":11,\"action_id\":101,"
                + "\"params\":{\"older_than\":\"30d\",\"retain_last\":\"3\"},\"rules\":{}}"
                + "]");
    assertEquals(2, tasks.size());
    assertEquals("7d", tasks.get(0).procedureArgs.get("older-than"));
    assertEquals("10", tasks.get(0).procedureArgs.get("retain-last"));
    assertEquals("30d", tasks.get(1).procedureArgs.get("older-than"));
    assertEquals("3", tasks.get(1).procedureArgs.get("retain-last"));
  }

  @Test
  public void testToProcedureArgMap() {
    Map<String, String> args =
        IcebergMaintenanceJob.toProcedureArgMap(
            Map.of("strategy", "binpack", "where_clause", "dt=1", "sort_order", "id"));
    assertEquals("binpack", args.get("strategy"));
    assertEquals("dt=1", args.get("where"));
    assertEquals("id", args.get("sort-order"));
  }

  @Test
  public void testSnapshotDriftGateOnlyForCompactionAndManifest() {
    assertTrue(IcebergMaintenanceJob.shouldCheckSnapshotDrift(MaintenanceType.REWRITE_DATA_FILES));
    assertTrue(IcebergMaintenanceJob.shouldCheckSnapshotDrift(MaintenanceType.REWRITE_MANIFESTS));
    assertFalse(IcebergMaintenanceJob.shouldCheckSnapshotDrift(MaintenanceType.EXPIRE_SNAPSHOTS));
    assertFalse(
        IcebergMaintenanceJob.shouldCheckSnapshotDrift(MaintenanceType.REMOVE_ORPHAN_FILES));
  }

  @Test
  public void testMaintenanceActionsWireToJobClasses() {
    assertTrue(MaintenanceType.REWRITE_DATA_FILES.action() instanceof IcebergRewriteDataFilesJob);
    assertTrue(MaintenanceType.REWRITE_MANIFESTS.action() instanceof IcebergRewriteManifestsJob);
    assertTrue(MaintenanceType.EXPIRE_SNAPSHOTS.action() instanceof IcebergExpireSnapshotsJob);
    assertTrue(MaintenanceType.REMOVE_ORPHAN_FILES.action() instanceof IcebergRemoveOrphanFilesJob);

    assertTrue(MaintenanceType.REWRITE_DATA_FILES.action().hasBeforeMetrics());
    assertTrue(MaintenanceType.REWRITE_MANIFESTS.action().hasBeforeMetrics());
    assertTrue(MaintenanceType.EXPIRE_SNAPSHOTS.action().hasBeforeMetrics());
    assertFalse(MaintenanceType.REMOVE_ORPHAN_FILES.action().hasBeforeMetrics());
  }
}
