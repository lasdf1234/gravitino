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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.Config;
import org.apache.gravitino.policy.TableMaintenanceContent;

/**
 * Cluster-static auth bags for TMS {@code runJob}, loaded from {@code gravitino.conf}.
 *
 * <p>Not part of {@link TableMaintenanceContent}; overlaid when building {@link
 * TableMaintenanceRunJobBags}.
 */
public final class MaintenanceClientAuth {

  private final Map<String, String> gravitinoAuth;
  private final Map<String, String> ircAuth;

  private MaintenanceClientAuth(Map<String, String> gravitinoAuth, Map<String, String> ircAuth) {
    this.gravitinoAuth = gravitinoAuth;
    this.ircAuth = ircAuth;
  }

  /**
   * Loads auth bags from server configuration.
   *
   * @param config Gravitino server config
   * @return parsed auth bags (empty maps when unset)
   */
  public static MaintenanceClientAuth from(Config config) {
    Map<String, String> gravitinoAuth =
        config.getConfigsWithPrefix(MaintenanceAuthConfigKeys.GRAVITINO_AUTH_PREFIX);
    Map<String, String> ircAuth =
        config.getConfigsWithPrefix(MaintenanceAuthConfigKeys.IRC_AUTH_PREFIX);
    validateAuthBag(gravitinoAuth, MaintenanceAuthConfigKeys.GRAVITINO_AUTH_PREFIX);
    validateAuthBag(ircAuth, MaintenanceAuthConfigKeys.IRC_AUTH_PREFIX);
    return new MaintenanceClientAuth(
        gravitinoAuth.isEmpty() ? Collections.emptyMap() : gravitinoAuth,
        ircAuth.isEmpty() ? Collections.emptyMap() : ircAuth);
  }

  /**
   * @return Gravitino client auth entries (suffix keys only)
   */
  public Map<String, String> gravitinoAuth() {
    return gravitinoAuth;
  }

  /**
   * @return Iceberg REST client auth entries (suffix keys only)
   */
  public Map<String, String> ircAuth() {
    return ircAuth;
  }

  /**
   * Overlays these cluster auth bags onto policy peer options.
   *
   * @param policyJobOptions effective policy {@code jobOptions}
   * @param policyUpdateOptions effective policy {@code updateOptions}
   * @return merged {@code runJob} bags
   */
  public TableMaintenanceRunJobBags overlayOn(
      Map<String, String> policyJobOptions, Map<String, String> policyUpdateOptions) {
    return TableMaintenanceRunJobBags.overlayAtRunJob(
        policyJobOptions, policyUpdateOptions, gravitinoAuth, ircAuth);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MaintenanceClientAuth)) {
      return false;
    }
    MaintenanceClientAuth that = (MaintenanceClientAuth) o;
    return Objects.equals(gravitinoAuth, that.gravitinoAuth)
        && Objects.equals(ircAuth, that.ircAuth);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gravitinoAuth, ircAuth);
  }

  private static void validateAuthBag(Map<String, String> options, String prefix) {
    options.forEach(
        (key, value) -> {
          if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Configuration %s%s must have a non-empty value", prefix, key));
          }
        });
  }
}
