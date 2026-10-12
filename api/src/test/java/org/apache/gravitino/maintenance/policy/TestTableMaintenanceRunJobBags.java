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
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTableMaintenanceRunJobBags {

  @Test
  void overlayAuthWinsOnDuplicateKeys() {
    Map<String, String> policyJob =
        ImmutableMap.of("uri", "http://irc", "spark.executor.memory", "2g");
    Map<String, String> policyUpdate = ImmutableMap.of("metalake", "ml");
    Map<String, String> ircAuth =
        ImmutableMap.of("oauthCredential", "secret", "uri", "http://irc-auth");
    Map<String, String> gravitinoAuth = ImmutableMap.of("authType", "oauth");

    TableMaintenanceRunJobBags bags =
        TableMaintenanceRunJobBags.overlayAtRunJob(policyJob, policyUpdate, gravitinoAuth, ircAuth);

    Assertions.assertEquals("http://irc-auth", bags.jobOptions().get("uri"));
    Assertions.assertEquals("secret", bags.jobOptions().get("oauthCredential"));
    Assertions.assertEquals("2g", bags.jobOptions().get("spark.executor.memory"));
    Assertions.assertEquals("oauth", bags.updateOptions().get("authType"));
    Assertions.assertEquals("ml", bags.updateOptions().get("metalake"));
  }

  @Test
  void emptyAuthLeavesPolicyOnly() {
    TableMaintenanceRunJobBags bags =
        TableMaintenanceRunJobBags.overlayAtRunJob(
            ImmutableMap.of("a", "1"),
            ImmutableMap.of("b", "2"),
            ImmutableMap.of(),
            ImmutableMap.of());
    Assertions.assertEquals("1", bags.jobOptions().get("a"));
    Assertions.assertEquals("2", bags.updateOptions().get("b"));
  }
}
