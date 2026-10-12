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
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOccupancyReconciler extends TestJDBCBackend {

  @TestTemplate
  void testReconcileClosesMissingAndTerminalJobsWithoutInventingMetrics() throws SQLException {
    TableMaintenanceJobMetaService jobs = TableMaintenanceJobMetaService.getInstance();
    OccupancyReconciler reconciler = new OccupancyReconciler(jobs);

    long missingJobId = 7101L;
    long terminalJobId = 7102L;
    long runningJobId = 7103L;

    jobs.insertJob(missingJobId, 1L, 2L);
    jobs.insertJob(terminalJobId, 3L, 4L);
    jobs.insertJob(runningJobId, 5L, 6L);

    insertJobRun(terminalJobId, JobHandle.Status.SUCCEEDED.name(), 1_700_000_000_100L);
    insertJobRun(runningJobId, JobHandle.Status.STARTED.name(), 0L);

    Assertions.assertEquals(2, reconciler.reconcile());

    TableMaintenanceJobPO missing = jobs.getJob(missingJobId);
    Assertions.assertNotNull(missing.finishedAt());
    Assertions.assertNull(missing.afterSnapshotId());

    TableMaintenanceJobPO terminal = jobs.getJob(terminalJobId);
    Assertions.assertEquals(1_700_000_000_100L, terminal.finishedAt());
    Assertions.assertNull(terminal.afterSnapshotId());

    TableMaintenanceJobPO running = jobs.getJob(runningJobId);
    Assertions.assertNull(running.finishedAt());

    Assertions.assertEquals(0, reconciler.reconcile());
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
}
