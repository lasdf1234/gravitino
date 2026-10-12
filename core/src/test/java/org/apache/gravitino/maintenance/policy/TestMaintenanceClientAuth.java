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
import org.apache.gravitino.server.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceClientAuth {

  @Test
  void loadsAuthBagsFromServerConfig() {
    ServerConfig config = new ServerConfig();
    config.loadFromMap(
        ImmutableMap.of(
            MaintenanceAuthConfigKeys.GRAVITINO_AUTH_PREFIX + "authType",
            "oauth2",
            MaintenanceAuthConfigKeys.IRC_AUTH_PREFIX + "authType",
            "oauth2",
            MaintenanceAuthConfigKeys.IRC_AUTH_PREFIX + "oauthCredential",
            "id:secret"),
        key -> key.startsWith("gravitino."));

    MaintenanceClientAuth auth = MaintenanceClientAuth.from(config);
    Assertions.assertEquals("oauth2", auth.gravitinoAuth().get("authType"));
    Assertions.assertEquals("oauth2", auth.ircAuth().get("authType"));
    Assertions.assertEquals("id:secret", auth.ircAuth().get("oauthCredential"));

    TableMaintenanceRunJobBags bags =
        auth.overlayOn(ImmutableMap.of("uri", "http://irc"), ImmutableMap.of("metalake", "ml"));
    Assertions.assertEquals("id:secret", bags.jobOptions().get("oauthCredential"));
    Assertions.assertEquals("oauth2", bags.updateOptions().get("authType"));
    Assertions.assertEquals("ml", bags.updateOptions().get("metalake"));
  }

  @Test
  void rejectsBlankAuthValue() {
    ServerConfig config = new ServerConfig();
    config.loadFromMap(
        ImmutableMap.of(MaintenanceAuthConfigKeys.GRAVITINO_AUTH_PREFIX + "authType", "   "),
        key -> key.startsWith("gravitino."));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MaintenanceClientAuth.from(config));
  }
}
