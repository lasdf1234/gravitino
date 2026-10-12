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
package org.apache.gravitino.maintenance.occupancy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOccupancyGate extends TestJDBCBackend {

  @TestTemplate
  void testInFlightReadsJobRunMetaWithoutBackfillingFinishedAt() throws SQLException {
    TableMaintenanceJobMetaService jobs = TableMaintenanceJobMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(jobs);

    long tableId = 11L;
    long policyId = 22L;
    long jobId = 7001L;

    Assertions.assertFalse(gate.isInFlight(tableId, policyId));

    jobs.insertJob(jobId, tableId, policyId);
    // finished_at is null but job_run_meta is missing ⇒ not treated as in-flight.
    Assertions.assertFalse(gate.isInFlight(tableId, policyId));

    insertJobRun(jobId, JobHandle.Status.STARTED.name(), 0L);
    Assertions.assertTrue(gate.isInFlight(tableId, policyId));
    Assertions.assertTrue(
        gate.shouldSkip(tableId, policyId, 3_600_000L, System.currentTimeMillis()));
    Assertions.assertNull(jobs.getJob(jobId).finishedAt());

    long finishedAt = 1_700_000_000_000L;
    updateJobRun(jobId, JobHandle.Status.SUCCEEDED.name(), finishedAt);
    Assertions.assertFalse(gate.isInFlight(tableId, policyId));
    // Still no backfill on table_maintenance_job.finished_at.
    Assertions.assertNull(jobs.getJob(jobId).finishedAt());
    Assertions.assertTrue(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, finishedAt + 1_000L));
    Assertions.assertFalse(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, finishedAt + 3_600_001L));
  }

  @TestTemplate
  void testMinIntervalUsesMarkedFinishedAt() {
    TableMaintenanceJobMetaService jobs = TableMaintenanceJobMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(jobs);

    long tableId = 33L;
    long policyId = 44L;
    long jobId = 7002L;
    long finishedAt = 1_700_000_100_000L;

    jobs.insertJob(jobId, tableId, policyId);
    jobs.markFinished(jobId, 200L, finishedAt);
    Assertions.assertFalse(gate.isInFlight(tableId, policyId));
    Assertions.assertTrue(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, finishedAt + 1_000L));
  }

  private static void insertJobRun(long jobRunId, String status, long finishedAt)
      throws SQLException {
    String sql =
        "INSERT INTO job_run_meta (job_run_id, job_template_id, metalake_id, job_execution_id,"
            + " job_run_status, job_started_at, job_finished_at, audit_info, current_version,"
            + " last_version, deleted_at) VALUES (?, 1, 1, ?, ?, 1, ?, '{}', 1, 1, 0)";
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setLong(1, jobRunId);
      stmt.setString(2, "exec-" + jobRunId);
      stmt.setString(3, status);
      stmt.setLong(4, finishedAt);
      stmt.executeUpdate();
    }
  }

  private static void updateJobRun(long jobRunId, String status, long finishedAt)
      throws SQLException {
    String sql =
        "UPDATE job_run_meta SET job_run_status = ?, job_finished_at = ?"
            + " WHERE job_run_id = ? AND deleted_at = 0";
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, status);
      stmt.setLong(2, finishedAt);
      stmt.setLong(3, jobRunId);
      stmt.executeUpdate();
    }
  }
}
