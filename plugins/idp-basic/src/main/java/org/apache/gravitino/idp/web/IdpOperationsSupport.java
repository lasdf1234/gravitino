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
package org.apache.gravitino.idp.web;

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.authorization.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.IdpGroupDTO;
import org.apache.gravitino.idp.dto.IdpUserDTO;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.relational.IdpEntityStore;

/** Shared helpers for built-in IdP REST resources in the {@code idp-basic} plugin. */
public final class IdpOperationsSupport {

  private static volatile IdpUserGroupManager manager;

  private IdpOperationsSupport() {}

  public static IdpUserGroupManager manager() {
    if (manager == null) {
      synchronized (IdpOperationsSupport.class) {
        if (manager == null) {
          IdpEntityStore store = new IdpEntityStore();
          store.initialize(GravitinoEnv.getInstance().config());
          manager = new IdpUserGroupManager(store, GravitinoEnv.getInstance().idGenerator());
        }
      }
    }
    return manager;
  }

  public static IdpUserDTO toUserDTO(IdpUser user) {
    return IdpUserDTO.builder().withName(user.name()).withGroups(user.groupNames()).build();
  }

  public static IdpGroupDTO toGroupDTO(IdpGroup group) {
    return IdpGroupDTO.builder().withName(group.name()).withUsers(group.usernames()).build();
  }
}
