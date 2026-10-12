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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.storage.mapper.JobRunStatusMapper;
import org.apache.gravitino.maintenance.storage.po.JobRunStatusPO;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes stale {@code table_maintenance_job} occupancy when the Gravitino job is already terminal
 * or missing (§5.2, §8).
 *
 * <p>Does <strong>not</strong> invent snapshot metrics: {@code after_snapshot_id} stays null.
 */
public final class OccupancyReconciler {

  private static final Logger LOG = LoggerFactory.getLogger(OccupancyReconciler.class);

  private static final Set<JobHandle.Status> TERMINAL =
      EnumSet.of(JobHandle.Status.SUCCEEDED, JobHandle.Status.FAILED, JobHandle.Status.CANCELLED);

  private final TableMaintenanceJobMetaService jobMetaService;

  /**
   * @param jobMetaService maintenance job DAO
   */
  public OccupancyReconciler(TableMaintenanceJobMetaService jobMetaService) {
    this.jobMetaService = jobMetaService;
  }

  /** Uses {@link TableMaintenanceJobMetaService#getInstance()}. */
  public OccupancyReconciler() {
    this(TableMaintenanceJobMetaService.getInstance());
  }

  /**
   * Marks in-flight rows finished when {@code job_run_meta} is terminal or absent.
   *
   * @return number of rows closed
   */
  public int reconcile() {
    List<TableMaintenanceJobPO> inFlight = jobMetaService.listInFlight();
    int closed = 0;
    for (TableMaintenanceJobPO job : inFlight) {
      if (closeIfStale(job)) {
        closed++;
      }
    }
    if (closed > 0) {
      LOG.info("Closed {} stale table maintenance occupancy row(s)", closed);
    }
    return closed;
  }

  private boolean closeIfStale(TableMaintenanceJobPO job) {
    JobRunStatusPO run =
        SessionUtils.getWithoutCommit(
            JobRunStatusMapper.class, mapper -> mapper.selectByJobRunId(job.jobId()));
    if (run == null) {
      // Soft-deleted / GC'd job_run_meta — free occupancy without inventing metrics.
      jobMetaService.markFinished(job.jobId(), null, System.currentTimeMillis());
      return true;
    }
    JobHandle.Status status;
    try {
      status = JobHandle.Status.valueOf(run.status());
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "Unknown job_run_status {} for job_id {}; leaving occupancy open",
          run.status(),
          job.jobId());
      return false;
    }
    if (!TERMINAL.contains(status)) {
      return false;
    }
    long finishedAt =
        run.finishedAt() != null && run.finishedAt() > 0
            ? run.finishedAt()
            : System.currentTimeMillis();
    jobMetaService.markFinished(job.jobId(), null, finishedAt);
    return true;
  }
}
