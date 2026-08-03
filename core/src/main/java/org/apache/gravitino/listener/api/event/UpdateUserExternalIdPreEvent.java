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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.AuthorizationUtils;

/** Represents an event triggered before updating a user external id by Gravitino-assigned id. */
@DeveloperApi
public class UpdateUserExternalIdPreEvent extends UserPreEvent {
  private final long userId;
  private final String newExternalId;

  /**
   * Creates a new {@link UpdateUserExternalIdPreEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param userId The Gravitino-assigned id of the user.
   * @param newExternalId The new external identifier, or null to clear it.
   */
  public UpdateUserExternalIdPreEvent(
      String initiator, String metalake, long userId, String newExternalId) {
    super(initiator, AuthorizationUtils.ofUserId(metalake, userId));
    this.userId = userId;
    this.newExternalId = newExternalId;
  }

  /**
   * Returns the Gravitino-assigned id of the user being updated.
   *
   * @return The user id.
   */
  public long userId() {
    return userId;
  }

  /**
   * Returns the new external identifier.
   *
   * @return The new external identifier, or null.
   */
  public String newExternalId() {
    return newExternalId;
  }

  @Override
  public OperationType operationType() {
    return OperationType.UPDATE_USER_EXTERNAL_ID;
  }
}
