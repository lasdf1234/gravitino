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
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOccupancyGate extends TestJDBCBackend {

  @TestTemplate
  void testInFlightAndMinIntervalGates() {
    TableMaintenanceJobMetaService jobs = TableMaintenanceJobMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(jobs);

    long tableId = 11L;
    long policyId = 22L;
    long jobId = 7001L;

    Assertions.assertFalse(gate.isInFlight(tableId, policyId));
    Assertions.assertFalse(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, System.currentTimeMillis()));

    jobs.insertJob(jobId, tableId, policyId);
    Assertions.assertTrue(gate.isInFlight(tableId, policyId));
    Assertions.assertTrue(
        gate.shouldSkip(tableId, policyId, 3_600_000L, System.currentTimeMillis()));

    long finishedAt = 1_700_000_000_000L;
    jobs.markFinished(jobId, null, finishedAt);
    Assertions.assertFalse(gate.isInFlight(tableId, policyId));
    Assertions.assertTrue(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, finishedAt + 1_000L));
    Assertions.assertFalse(
        gate.withinMinInterval(tableId, policyId, 3_600_000L, finishedAt + 3_600_001L));
  }
}
