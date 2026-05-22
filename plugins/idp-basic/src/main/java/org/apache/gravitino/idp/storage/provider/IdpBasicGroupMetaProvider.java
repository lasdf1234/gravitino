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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The provider class for built-in IdP group metadata. */
public class IdpBasicGroupMetaProvider
    implements IdpGroupMetaService<IdpGroupPO, IdpUserGroupRelPO> {
  private static final IdpBasicGroupMetaProvider INSTANCE = new IdpBasicGroupMetaProvider();

  /** Returns the singleton provider instance. */
  public static IdpBasicGroupMetaProvider getInstance() {
    return INSTANCE;
  }

  /** Creates a new provider instance for ServiceLoader. */
  public IdpBasicGroupMetaProvider() {}

  @Override
  public Optional<IdpGroupPO> findGroup(String groupName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroup(groupName)));
  }

  @Override
  public List<String> listUserNames(String groupName) {
    Optional<IdpGroupPO> group = findGroup(groupName);
    if (!group.isPresent()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectUsernamesByGroupName(groupName));
  }

  @Override
  public void createGroup(IdpGroupPO groupPO) {
    SessionUtils.doWithCommit(IdpGroupMetaMapper.class, mapper -> mapper.insertIdpGroup(groupPO));
  }

  @Override
  public boolean deleteGroup(IdpGroupPO groupPO, Long deletedAt) {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByGroupName(groupPO.getGroupName())),
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupMetaMapper.class,
                mapper -> mapper.softDeleteIdpGroup(groupPO.getGroupName())));
    return true;
  }

  @Override
  public List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds) {
    if (userIds == null || userIds.isEmpty()) {
      return Collections.emptyList();
    }
    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectRelatedUserIds(groupId, userIds));
  }

  @Override
  public void addUsersToGroup(List<IdpUserGroupRelPO> relations) {
    if (relations.isEmpty()) {
      return;
    }
    SessionUtils.doWithCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.batchInsertRelations(relations));
  }

  @Override
  public void removeUsersFromGroup(Long groupId, List<Long> userIds, Long deletedAt) {
    if (userIds.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpUserGroupRelMapper.class,
        mapper -> mapper.softDeleteRelationsByGroupIdAndUserIds(groupId, userIds, deletedAt));
  }

  @Override
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    return org.apache.gravitino.idp.storage.service.IdpGroupMetaService.getInstance()
        .deleteGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
