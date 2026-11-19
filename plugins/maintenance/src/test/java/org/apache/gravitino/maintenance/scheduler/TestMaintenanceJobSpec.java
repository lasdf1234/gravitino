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
package org.apache.gravitino.maintenance.scheduler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceJobSpec} helpers. */
class TestMaintenanceJobSpec {

  @Test
  void testContainsSnapshotExpiration() {
    Map<String, String> params =
        new LinkedHashMap<>(Map.of("catalog_name", "c", "table_identifier", "db.t"));
    MaintenanceJobSpec empty = new MaintenanceJobSpec(List.of(), "{}", null);
    Assertions.assertFalse(empty.containsSnapshotExpiration());

    MaintenanceJobSpec compaction =
        new MaintenanceJobSpec(
            List.of(
                new Task(1L, 2L, Policy.BuiltInType.ICEBERG_COMPACTION.policyType(), null, params)),
            "{\"metalake\":\"ml\"}",
            null);
    Assertions.assertFalse(compaction.containsSnapshotExpiration());

    MaintenanceJobSpec expire =
        new MaintenanceJobSpec(
            List.of(
                new Task(
                    1L,
                    3L,
                    Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION.policyType(),
                    null,
                    params)),
            "{\"metalake\":\"ml\"}",
            null);
    Assertions.assertTrue(expire.containsSnapshotExpiration());
  }
}
