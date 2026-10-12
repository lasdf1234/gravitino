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

import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_job} (§5.2).
 *
 * <p>In-flight means {@code finished_at IS NULL}. {@code minIntervalMs} uses {@code
 * MAX(finished_at)} for the {@code (table_id, policy_id)} pair.
 */
public final class OccupancyGate {

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
   * @return true when an unfinished job row exists
   */
  public boolean isInFlight(long tableId, long policyId) {
    return jobMetaService.hasInFlight(tableId, policyId);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis
   * @return true when the last finish is still inside the cooldown window
   */
  public boolean withinMinInterval(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (minIntervalMs <= 0) {
      return false;
    }
    Long lastFinished = jobMetaService.maxFinishedAt(tableId, policyId);
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
}
