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
package org.apache.gravitino.idp.storage.provider;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The provider class for built-in IdP user metadata. */
public class IdpBasicUserMetaProvider implements IdpUserMetaService<IdpUserPO> {
  private static final IdpBasicUserMetaProvider INSTANCE = new IdpBasicUserMetaProvider();

  /** Returns the singleton provider instance. */
  public static IdpBasicUserMetaProvider getInstance() {
    return INSTANCE;
  }

  /** Creates a new provider instance for ServiceLoader. */
  public IdpBasicUserMetaProvider() {}

  @Override
  public Optional<IdpUserPO> findUser(String userName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpUserMetaMapper.class, mapper -> mapper.selectIdpUser(userName)));
  }

  @Override
  public List<IdpUserPO> findUsers(List<String> userNames) {
    if (userNames.isEmpty()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpUserMetaMapper.class, mapper -> mapper.selectIdpUsersByUsernames(userNames));
  }

  @Override
  public List<String> listGroupNames(String userName) {
    Optional<IdpUserPO> user = findUser(userName);
    if (!user.isPresent()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectGroupNamesByUsername(userName));
  }

  @Override
  public void createUser(IdpUserPO userPO) {
    SessionUtils.doWithCommit(IdpUserMetaMapper.class, mapper -> mapper.insertIdpUser(userPO));
  }

  @Override
  public void updatePassword(IdpUserPO userPO, String passwordHash, Long nextVersion) {
    Integer updated =
        SessionUtils.doWithCommitAndFetchResult(
            IdpUserMetaMapper.class,
            mapper -> mapper.updateIdpUserPassword(userPO.getUsername(), passwordHash));
    Preconditions.checkState(
        updated == 1, "Failed to update password for user %s", userPO.getUsername());
  }

  @Override
  public boolean deleteUser(IdpUserPO userPO, Long deletedAt) {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByUsername(userPO.getUsername())),
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserMetaMapper.class, mapper -> mapper.softDeleteIdpUser(userPO.getUsername())));
    return true;
  }

  @Override
  public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
    return org.apache.gravitino.idp.storage.service.IdpUserMetaService.getInstance()
        .deleteUserMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
