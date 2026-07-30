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

package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.responses.SecretProviderListResponse;
import org.apache.gravitino.dto.secret.SecretProviderDTO;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.secret.SecretProviderInfo;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations for Gravitino secret management. */
@Path("/secrets")
@Produces(MediaType.APPLICATION_JSON)
public class SecretsOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SecretsOperations.class);

  @Context private HttpServletRequest httpRequest;

  /**
   * Lists configured secret providers.
   *
   * @return a response containing provider metadata
   */
  @GET
  @Path("/providers")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-secret-providers." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-secret-providers", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listProviders() {
    LOG.info("Received list secret providers request");
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            var registry = GravitinoEnv.getInstance().secretProviderRegistry();
            List<SecretProviderInfo> infos =
                registry == null ? List.of() : registry.listProviderInfos();
            SecretProviderDTO[] providers =
                infos.stream()
                    .map(info -> new SecretProviderDTO(info.name(), info.type()))
                    .toArray(SecretProviderDTO[]::new);
            return Utils.ok(new SecretProviderListResponse(providers));
          });
    } catch (Exception e) {
      LOG.error("Failed to list secret providers", e);
      return Utils.internalError(e.getMessage(), e);
    }
  }
}
