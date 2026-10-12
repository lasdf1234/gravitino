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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.storage.mapper.JobRunStatusMapper;
import org.apache.gravitino.maintenance.storage.po.JobRunStatusPO;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_job} (§5.2).
 *
 * <p>When {@code finished_at} is null, this gate reads {@code job_run_meta} to decide whether the
 * Gravitino job is still running. It does <strong>not</strong> backfill {@code finished_at}.
 */
public final class OccupancyGate {

  private static final Set<JobHandle.Status> ACTIVE =
      EnumSet.of(JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING);

  private final TableMaintenanceJobMetaService jobMetaService;

  /**
   * @param jobMetaService maintenance job DAO
   */
  public OccupancyGate(TableMaintenanceJobMetaService jobMetaService) {
    this.jobMetaService = jobMetaService;
  }

  /** Uses {@link TableMaintenanceJobMetaService#getInstance()}. */
  public OccupancyGate() {
    this(TableMaintenanceJobMetaService.getInstance());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when a row with null {@code finished_at} still maps to an active {@code
   *     job_run_meta} status
   */
  public boolean isInFlight(long tableId, long policyId) {
    TableMaintenanceJobPO open = jobMetaService.getInFlight(tableId, policyId);
    if (open == null) {
      return false;
    }
    return isJobActive(open.jobId());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis
   * @return true when the last known finish is still inside the cooldown window
   */
  public boolean withinMinInterval(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (minIntervalMs <= 0) {
      return false;
    }
    Long lastFinished = lastFinishedAt(tableId, policyId);
    return lastFinished != null && nowMs - lastFinished < minIntervalMs;
  }

  /**
   * Expand / submit should skip when occupied or still cooling down.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis
   * @return true when enqueue / submit must be skipped
   */
  public boolean shouldSkip(long tableId, long policyId, long minIntervalMs, long nowMs) {
    return isInFlight(tableId, policyId)
        || withinMinInterval(tableId, policyId, minIntervalMs, nowMs);
  }

  /**
   * Prefer {@code table_maintenance_job.finished_at}; if that column is still null but {@code
   * job_run_meta} is already terminal, use {@code job_finished_at} without writing back.
   */
  @Nullable
  private Long lastFinishedAt(long tableId, long policyId) {
    Long marked = jobMetaService.maxFinishedAt(tableId, policyId);
    TableMaintenanceJobPO open = jobMetaService.getInFlight(tableId, policyId);
    if (open == null) {
      return marked;
    }
    JobRunStatusPO run = lookupJobRun(open.jobId());
    if (run == null || isActiveStatus(run.status())) {
      return marked;
    }
    Long fromJob = run.finishedAt() != null && run.finishedAt() > 0 ? run.finishedAt() : null;
    if (marked == null) {
      return fromJob;
    }
    if (fromJob == null) {
      return marked;
    }
    return Math.max(marked, fromJob);
  }

  private static boolean isJobActive(long jobRunId) {
    JobRunStatusPO run = lookupJobRun(jobRunId);
    // Missing job_run_meta ⇒ not treated as in-flight; do not invent finished_at.
    return run != null && isActiveStatus(run.status());
  }

  private static boolean isActiveStatus(String statusName) {
    try {
      return ACTIVE.contains(JobHandle.Status.valueOf(statusName));
    } catch (IllegalArgumentException e) {
      // Unknown status: fail closed and treat as occupied.
      return true;
    }
  }

  @Nullable
  private static JobRunStatusPO lookupJobRun(long jobRunId) {
    return SessionUtils.getWithoutCommit(
        JobRunStatusMapper.class, mapper -> mapper.selectByJobRunId(jobRunId));
  }
}
