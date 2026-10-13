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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMinIntervalMsResolver {

  @Test
  void testMinIntervalMsResolutionOrder() {
    long fromPolicy =
        MinIntervalMsResolver.resolve(
            TableMaintenanceTaskType.COMPACTION, 120_000L, ImmutableMap.of());
    Assertions.assertEquals(120_000L, fromPolicy);

    long fromConf =
        MinIntervalMsResolver.resolve(
            TableMaintenanceTaskType.COMPACTION,
            null,
            ImmutableMap.of(
                MinIntervalMsResolver.configKey(TableMaintenanceTaskType.COMPACTION), "900000"));
    Assertions.assertEquals(900_000L, fromConf);

    long fromDefault =
        MinIntervalMsResolver.resolve(TableMaintenanceTaskType.COMPACTION, null, ImmutableMap.of());
    Assertions.assertEquals(
        TableMaintenanceTaskType.COMPACTION.defaultMinIntervalMs(), fromDefault);
  }

  @Test
  void testConfigKeyFormat() {
    Assertions.assertEquals(
        "gravitino.maintenance.task.compaction.minIntervalMs",
        MinIntervalMsResolver.configKey(TableMaintenanceTaskType.COMPACTION));
    Assertions.assertEquals(
        "gravitino.maintenance.task.orphan-cleanup.minIntervalMs",
        MinIntervalMsResolver.configKey(TableMaintenanceTaskType.ORPHAN_CLEANUP));
  }
}
