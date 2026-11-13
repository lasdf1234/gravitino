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
package org.apache.gravitino.maintenance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Submits TMS Spark jobs and binds the resulting job run id onto soft-slot claims.
 *
 * <p>{@link #submitAndBind} injects {@code action_id} onto each {@code tasks[]} element (same order
 * as {@code claims}), calls {@code runJob}, then binds {@code job_id}. If {@code runJob} succeeds
 * but bind fails, the orphaned Spark job is cancelled and unbound claims are released. Callers that
 * fail before {@code runJob} must release claims themselves.
 */
public class MaintenanceJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceJobManager.class);

  private static final TableMaintenanceActionMetaService ACTION_SERVICE =
      TableMaintenanceActionMetaService.getInstance();

  private static volatile MaintenanceJobManager instance;

  private final JobOperationDispatcher jobOperationDispatcher;

  /**
   * Returns the shared TMS job manager for the server process.
   *
   * @return singleton manager
   */
  public static MaintenanceJobManager getInstance() {
    if (instance == null) {
      synchronized (MaintenanceJobManager.class) {
        if (instance == null) {
          instance = new MaintenanceJobManager(GravitinoEnv.getInstance().jobOperationDispatcher());
        }
      }
    }
    return instance;
  }

  private MaintenanceJobManager(JobOperationDispatcher jobOperationDispatcher) {
    this.jobOperationDispatcher = Preconditions.checkNotNull(jobOperationDispatcher);
  }

  /**
   * Injects per-task {@code action_id} values, runs the TMS maintenance template, and binds {@code
   * job_id} on the claims (one or many tables / chain steps; claim order matches {@code tasks[]}).
   *
   * @param metalake metalake name
   * @param jobConf mutable runtime jobConf (policy options already resolved)
   * @param claims soft-slot claims from acquire (non-empty, parallel to tasks)
   * @return job run id when run+bind succeeded; empty when submit or bind failed (claims released)
   */
  public Optional<Long> submitAndBind(
      String metalake, Map<String, String> jobConf, List<OccupancyClaim> claims) {
    Preconditions.checkNotNull(metalake, "metalake");
    Preconditions.checkNotNull(jobConf, "jobConf");
    Preconditions.checkArgument(claims != null && !claims.isEmpty(), "claims must be non-empty");

    try {
      injectActionIds(jobConf, claims);
    } catch (RuntimeException e) {
      try {
        ACTION_SERVICE.deleteClaims(claims);
      } catch (RuntimeException releaseError) {
        LOG.warn("Failed to release occupancy claims after action_id inject failure", releaseError);
      }
      LOG.warn("Failed TMS jobConf action_id inject: {}", e.toString());
      return Optional.empty();
    }

    Long jobId = null;
    try {
      JobEntity job =
          jobOperationDispatcher.runJob(
              metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
      jobId = job.id();
      ACTION_SERVICE.bindJobIds(claims, jobId);
      return Optional.of(jobId);
    } catch (RuntimeException e) {
      if (jobId != null) {
        try {
          jobOperationDispatcher.cancelJob(metalake, JobHandle.JOB_ID_PREFIX + jobId);
        } catch (RuntimeException cancelError) {
          LOG.warn(
              "Failed to cancel orphaned TMS Spark job {} under metalake {} after bind failure",
              jobId,
              metalake,
              cancelError);
        }
      }
      try {
        ACTION_SERVICE.deleteClaims(claims);
      } catch (RuntimeException releaseError) {
        LOG.warn("Failed to release occupancy claims after submit failure", releaseError);
      }
      LOG.warn(
          "Failed TMS job submitAndBind for action_ids {}: {}",
          claims.stream().map(c -> Long.toString(c.actionId())).collect(Collectors.joining(",")),
          e.toString());
      return Optional.empty();
    }
  }

  private static void injectActionIds(Map<String, String> jobConf, List<OccupancyClaim> claims) {
    String tasksJson = jobConf.get(MaintenanceJobTemplates.TASKS_KEY);
    Preconditions.checkArgument(
        tasksJson != null && !tasksJson.isBlank(),
        "jobConf.%s is required",
        MaintenanceJobTemplates.TASKS_KEY);
    List<Map<String, Object>> tasks;
    try {
      tasks =
          JsonUtils.anyFieldMapper()
              .readValue(tasksJson, new TypeReference<List<Map<String, Object>>>() {});
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "jobConf." + MaintenanceJobTemplates.TASKS_KEY + " must be valid JSON", e);
    }
    Preconditions.checkArgument(
        tasks != null && tasks.size() == claims.size(),
        "tasks size %s must match claims size %s",
        tasks == null ? 0 : tasks.size(),
        claims.size());
    for (int i = 0; i < tasks.size(); i++) {
      Map<String, Object> task = tasks.get(i);
      Preconditions.checkNotNull(task, "tasks[%s] must not be null", i);
      Map<String, Object> copy = new LinkedHashMap<>(task);
      copy.put("action_id", claims.get(i).actionId());
      tasks.set(i, copy);
    }
    try {
      jobConf.put(
          MaintenanceJobTemplates.TASKS_KEY, JsonUtils.anyFieldMapper().writeValueAsString(tasks));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize tasks with action_id", e);
    }
  }
}
