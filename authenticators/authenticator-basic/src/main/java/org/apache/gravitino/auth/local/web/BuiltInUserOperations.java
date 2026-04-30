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

package org.apache.gravitino.auth.local.web;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.auth.local.BuiltInAuthenticationManager;
import org.apache.gravitino.auth.local.PasswordUnchangedException;
import org.apache.gravitino.auth.local.dto.requests.CreateUserRequest;
import org.apache.gravitino.auth.local.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.auth.local.dto.responses.BuiltInUserResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.server.web.Utils;

/** REST operations for built-in authentication users. */
@Path("idp/users")
public class BuiltInUserOperations {

  private final BuiltInAuthenticationManager authenticationManager;

  @Context private HttpServletRequest httpRequest;

  public BuiltInUserOperations() {
    this(BuiltInAuthenticationManager.fromEnvironment());
  }

  BuiltInUserOperations(BuiltInAuthenticationManager authenticationManager) {
    this.authenticationManager = authenticationManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  public Response listUsers() {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new NameListResponse(authenticationManager.listUsers())));
    } catch (Exception e) {
      return handleException(e);
    }
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response getUser(@PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> Utils.ok(new BuiltInUserResponse(authenticationManager.getUser(user))));
    } catch (Exception e) {
      return handleException(e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  public Response createUser(CreateUserRequest request) {
    try {
      request.validate();
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new BuiltInUserResponse(
                      authenticationManager.createUser(request.user(), request.password()))));
    } catch (Exception e) {
      return handleException(e);
    }
  }

  @PUT
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response resetPassword(@PathParam("user") String user, ResetPasswordRequest request) {
    try {
      request.validate();
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new BuiltInUserResponse(
                      authenticationManager.resetPassword(user, request.password()))));
    } catch (Exception e) {
      return handleException(e);
    }
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response deleteUser(@PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new RemoveResponse(authenticationManager.deleteUser(user))));
    } catch (Exception e) {
      return handleException(e);
    }
  }

  private Response handleException(Exception e) {
    if (e instanceof UserAlreadyExistsException) {
      return Utils.alreadyExists(e.getMessage(), e);
    }
    if (e instanceof NoSuchUserException) {
      return Utils.notFound(e.getMessage(), e);
    }
    if (e instanceof IllegalArgumentException) {
      return Utils.illegalArguments(e.getMessage(), e);
    }
    if (e instanceof ForbiddenException) {
      return Utils.forbidden(e.getMessage(), e);
    }
    if (e instanceof UnsupportedOperationException) {
      return Utils.unsupportedOperation(e.getMessage(), e);
    }
    if (e instanceof PasswordUnchangedException) {
      return Response.status(422)
          .entity(ErrorResponse.illegalArguments(e.getClass().getSimpleName(), e.getMessage(), e))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }

    return Utils.internalError(e.getMessage(), e);
  }
}
