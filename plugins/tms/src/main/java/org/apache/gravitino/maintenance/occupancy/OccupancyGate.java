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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.JobMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_job} (§5.2).
 *
 * <p>When {@code finished_at} is null, this gate loads the Gravitino job via {@link
 * JobMetaService#getJobByIdentifier} to decide whether it is still running. It does
 * <strong>not</strong> backfill {@code finished_at}.
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
   * @return true when a row with null {@code finished_at} still maps to an active Gravitino job
   */
  public boolean isInFlight(long tableId, long policyId) {
    TableMaintenanceJobPO open = jobMetaService.getInFlight(tableId, policyId);
    if (open == null) {
      return false;
    }
    JobEntity job = loadJob(open.jobId(), tableId);
    return job != null && ACTIVE.contains(job.status());
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
   * Prefer {@code table_maintenance_job.finished_at}; if that column is still null but the
   * Gravitino job is already terminal, use {@link JobEntity#finishedAt()} without writing back.
   */
  @Nullable
  private Long lastFinishedAt(long tableId, long policyId) {
    Long marked = jobMetaService.maxFinishedAt(tableId, policyId);
    TableMaintenanceJobPO open = jobMetaService.getInFlight(tableId, policyId);
    if (open == null) {
      return marked;
    }
    JobEntity job = loadJob(open.jobId(), tableId);
    if (job == null || ACTIVE.contains(job.status())) {
      return marked;
    }
    Long fromJob = job.finishedAt() != null && job.finishedAt() > 0 ? job.finishedAt() : null;
    if (marked == null) {
      return fromJob;
    }
    if (fromJob == null) {
      return marked;
    }
    return Math.max(marked, fromJob);
  }

  @Nullable
  static JobEntity loadJob(long jobRunId, long tableId) {
    String metalake = metalakeNameForTable(tableId);
    if (metalake == null) {
      return null;
    }
    try {
      return JobMetaService.getInstance()
          .getJobByIdentifier(
              NameIdentifierUtil.ofJob(metalake, JobHandle.JOB_ID_PREFIX + jobRunId));
    } catch (NoSuchEntityException e) {
      return null;
    }
  }

  @Nullable
  private static String metalakeNameForTable(long tableId) {
    List<TablePO> tables =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.listTablePOsByTableIds(Collections.singletonList(tableId)));
    if (tables == null || tables.isEmpty() || tables.get(0).getMetalakeId() == null) {
      return null;
    }
    MetalakePO metalake =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaById(tables.get(0).getMetalakeId()));
    return metalake == null ? null : metalake.getMetalakeName();
  }
}
