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

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to change built-in IdP group membership. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class GroupMembershipChangeRequest implements RESTRequest {

  @JsonProperty("additions")
  private final String[] additions;

  @JsonProperty("removals")
  private final String[] removals;

  /** Default constructor for GroupMembershipChangeRequest. (Used for Jackson deserialization.) */
  public GroupMembershipChangeRequest() {
    this(null, null);
  }

  /**
   * Creates a new GroupMembershipChangeRequest.
   *
   * @param additions The user names to add to the built-in IdP group.
   * @param removals The user names to remove from the built-in IdP group.
   */
  public GroupMembershipChangeRequest(String[] additions, String[] removals) {
    this.additions = additions;
    this.removals = removals;
  }

  /**
   * Validates the {@link GroupMembershipChangeRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    boolean hasAdditions = additions != null && additions.length > 0;
    boolean hasRemovals = removals != null && removals.length > 0;
    Preconditions.checkArgument(
        hasAdditions || hasRemovals, "At least one of additions or removals must be non-empty");

    if (additions != null) {
      for (String user : additions) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(user), "additions must not contain null or empty user names");
      }
    }

    if (removals != null) {
      for (String user : removals) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(user), "removals must not contain null or empty user names");
      }
    }
  }
}
