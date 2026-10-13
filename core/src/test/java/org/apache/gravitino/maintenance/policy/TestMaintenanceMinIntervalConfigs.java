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
package org.apache.gravitino.maintenance.policy;

import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMaintenanceMinIntervalConfigs {

  @Test
  void testRegisteredDefaultsMatchTaskTypeDefaults() {
    Config config = new Config(false) {};
    for (TableMaintenanceTaskType taskType : TableMaintenanceTaskType.values()) {
      Assertions.assertEquals(
          taskType.defaultMinIntervalMs(),
          config.get(MaintenanceMinIntervalConfigs.forTaskType(taskType)));
      Assertions.assertEquals(
          MinIntervalMsResolver.configKey(taskType),
          MaintenanceMinIntervalConfigs.forTaskType(taskType).getKey());
    }
  }

  @Test
  void testResolvePrefersPolicyThenConfigThenDefault() {
    Config config = new Config(false) {};
    config.set(MaintenanceMinIntervalConfigs.MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS, 900_000L);

    Assertions.assertEquals(
        120_000L,
        MaintenanceMinIntervalConfigs.resolve(
            config, TableMaintenanceTaskType.COMPACTION, 120_000L));
    Assertions.assertEquals(
        900_000L,
        MaintenanceMinIntervalConfigs.resolve(config, TableMaintenanceTaskType.COMPACTION, null));

    Config defaultsOnly = new Config(false) {};
    Assertions.assertEquals(
        TableMaintenanceTaskType.ORPHAN_CLEANUP.defaultMinIntervalMs(),
        MaintenanceMinIntervalConfigs.resolve(
            defaultsOnly, TableMaintenanceTaskType.ORPHAN_CLEANUP, null));
  }

  @Test
  void testRejectsNegativeMinInterval() {
    Config config = new Config(false) {};
    config.set(MaintenanceMinIntervalConfigs.MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS, -1L);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            config.get(MaintenanceMinIntervalConfigs.MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS));
  }
}
