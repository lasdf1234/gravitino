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

package org.apache.gravitino.auth.local;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.server.authentication.Authenticator;

/** Built-in basic authenticator backed by persisted local users. */
public class BasicAuthenticator implements Authenticator {

  private BuiltInAuthenticationManager authenticationManager;

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticate(HttpServletRequest request, byte[] tokenData) {
    return authenticationManager.authenticate(request, tokenData);
  }

  @Override
  public void initialize(Config config) {
    authenticationManager = BuiltInAuthenticationManager.fromConfig(config);
    authenticationManager.initializeServiceAdmins();
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData == null
        || new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }
}
