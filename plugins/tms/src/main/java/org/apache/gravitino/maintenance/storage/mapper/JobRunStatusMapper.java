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
package org.apache.gravitino.maintenance.storage.mapper;

import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.storage.po.JobRunStatusPO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * Read-only lookup of {@code job_run_meta} status for occupancy checks.
 *
 * <p>When {@code table_maintenance_job.finished_at} is null, {@code OccupancyGate} consults this
 * mapper instead of backfilling {@code finished_at}.
 */
public interface JobRunStatusMapper {

  /**
   * @param jobRunId {@code job_run_meta.job_run_id}
   * @return status row or null when missing / soft-deleted
   */
  @Nullable
  @Select(
      "SELECT job_run_status AS status, job_finished_at AS finishedAt FROM job_run_meta"
          + " WHERE job_run_id = #{jobRunId} AND deleted_at = 0")
  JobRunStatusPO selectByJobRunId(@Param("jobRunId") long jobRunId);
}
