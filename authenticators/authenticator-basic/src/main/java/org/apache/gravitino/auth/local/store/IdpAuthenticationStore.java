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

package org.apache.gravitino.auth.local.store;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpGroupUserRelPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Relational persistence layer for built-in authentication metadata. */
public class IdpAuthenticationStore {

  private static final long INITIAL_VERSION = 0L;

  private final IdGenerator idGenerator;

  public IdpAuthenticationStore(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  public Optional<IdpUserPO> findUser(String userName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpAuthenticationMapper.class, mapper -> mapper.selectLocalUser(userName)));
  }

  public List<IdpUserPO> findUsers(List<String> userNames) {
    if (userNames.isEmpty()) {
      return new ArrayList<>();
    }

    return SessionUtils.getWithoutCommit(
        IdpAuthenticationMapper.class, mapper -> mapper.selectLocalUsers(userNames));
  }

  public Optional<IdpGroupPO> findGroup(String groupName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpAuthenticationMapper.class, mapper -> mapper.selectLocalGroup(groupName)));
  }

  public List<String> listUserNames() {
    return SessionUtils.getWithoutCommit(
        IdpAuthenticationMapper.class, IdpAuthenticationMapper::selectLocalUserNames);
  }

  public List<String> listGroupNames() {
    return SessionUtils.getWithoutCommit(
        IdpAuthenticationMapper.class, IdpAuthenticationMapper::selectLocalGroupNames);
  }

  public List<String> listGroupNames(String userName) {
    Optional<IdpUserPO> user = findUser(userName);
    if (!user.isPresent()) {
      return new ArrayList<>();
    }

    return SessionUtils.getWithoutCommit(
        IdpAuthenticationMapper.class,
        mapper -> mapper.selectGroupNamesByUserId(user.get().getUserId()));
  }

  public List<String> listUserNames(String groupName) {
    Optional<IdpGroupPO> group = findGroup(groupName);
    if (!group.isPresent()) {
      return new ArrayList<>();
    }

    return SessionUtils.getWithoutCommit(
        IdpAuthenticationMapper.class,
        mapper -> mapper.selectUserNamesByGroupId(group.get().getGroupId()));
  }

  public IdpUserPO createUser(long userId, String userName, String passwordHash, String actor) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(passwordHash), "Password hash is required");

    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(userId);
    userPO.setUserName(userName);
    userPO.setPasswordHash(passwordHash);
    userPO.setAuditInfo(toAuditJson(newAudit(actor)));
    userPO.setCurrentVersion(INITIAL_VERSION);
    userPO.setLastVersion(INITIAL_VERSION);
    userPO.setDeletedAt(0L);
    SessionUtils.doWithCommit(
        IdpAuthenticationMapper.class, mapper -> mapper.insertLocalUser(userPO));
    return userPO;
  }

  public IdpGroupPO createGroup(long groupId, String groupName, String actor) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name is required");

    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(groupId);
    groupPO.setGroupName(groupName);
    groupPO.setAuditInfo(toAuditJson(newAudit(actor)));
    groupPO.setCurrentVersion(INITIAL_VERSION);
    groupPO.setLastVersion(INITIAL_VERSION);
    groupPO.setDeletedAt(0L);
    SessionUtils.doWithCommit(
        IdpAuthenticationMapper.class, mapper -> mapper.insertLocalGroup(groupPO));
    return groupPO;
  }

  public IdpUserPO updatePassword(IdpUserPO userPO, String passwordHash, String actor) {
    AuditInfo updatedAudit = updateAudit(fromAuditJson(userPO.getAuditInfo()), actor);
    long nextVersion = userPO.getCurrentVersion() + 1;
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            IdpAuthenticationMapper.class,
            mapper ->
                mapper.updateLocalUserPassword(
                    userPO.getUserId(),
                    passwordHash,
                    toAuditJson(updatedAudit),
                    userPO.getCurrentVersion(),
                    nextVersion,
                    nextVersion));
    Preconditions.checkState(
        updated == 1, "Failed to update password for user %s", userPO.getUserName());
    userPO.setPasswordHash(passwordHash);
    userPO.setAuditInfo(toAuditJson(updatedAudit));
    userPO.setCurrentVersion(nextVersion);
    userPO.setLastVersion(nextVersion);
    return userPO;
  }

  public boolean deleteUser(IdpUserPO userPO, String actor) {
    long deletedAt = Instant.now().toEpochMilli();
    String auditInfo = toAuditJson(updateAudit(fromAuditJson(userPO.getAuditInfo()), actor));
    long nextVersion = userPO.getCurrentVersion() + 1;
    final int[] updated = new int[1];
    SessionUtils.doMultipleWithCommit(
        () ->
            updated[0] =
                SessionUtils.doWithCommitAndFetchResult(
                    IdpAuthenticationMapper.class,
                    mapper ->
                        mapper.softDeleteLocalUser(
                            userPO.getUserId(),
                            deletedAt,
                            auditInfo,
                            userPO.getCurrentVersion(),
                            nextVersion,
                            nextVersion)),
        () ->
            SessionUtils.doWithCommit(
                IdpAuthenticationMapper.class,
                mapper ->
                    mapper.softDeleteGroupUsersByUserId(userPO.getUserId(), deletedAt, auditInfo)));
    return updated[0] == 1;
  }

  public boolean deleteGroup(IdpGroupPO groupPO, String actor) {
    long deletedAt = Instant.now().toEpochMilli();
    String auditInfo = toAuditJson(updateAudit(fromAuditJson(groupPO.getAuditInfo()), actor));
    long nextVersion = groupPO.getCurrentVersion() + 1;
    final int[] updated = new int[1];
    SessionUtils.doMultipleWithCommit(
        () ->
            updated[0] =
                SessionUtils.doWithCommitAndFetchResult(
                    IdpAuthenticationMapper.class,
                    mapper ->
                        mapper.softDeleteLocalGroup(
                            groupPO.getGroupId(),
                            deletedAt,
                            auditInfo,
                            groupPO.getCurrentVersion(),
                            nextVersion,
                            nextVersion)),
        () ->
            SessionUtils.doWithCommit(
                IdpAuthenticationMapper.class,
                mapper ->
                    mapper.softDeleteGroupUsersByGroupId(
                        groupPO.getGroupId(), deletedAt, auditInfo)));
    return updated[0] == 1;
  }

  public void addUsersToGroup(IdpGroupPO groupPO, List<IdpUserPO> users, String actor) {
    Set<Long> requestedUserIds =
        users.stream()
            .map(IdpUserPO::getUserId)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (requestedUserIds.isEmpty()) {
      return;
    }

    List<Long> existingUserIds =
        SessionUtils.getWithoutCommit(
            IdpAuthenticationMapper.class,
            mapper ->
                mapper.selectRelatedUserIds(
                    groupPO.getGroupId(), new ArrayList<>(requestedUserIds)));
    Set<Long> existingSet = new LinkedHashSet<>(existingUserIds);
    List<IdpGroupUserRelPO> relations = new ArrayList<>();
    String auditInfo = toAuditJson(newAudit(actor));
    for (IdpUserPO user : users) {
      if (existingSet.contains(user.getUserId())) {
        continue;
      }

      IdpGroupUserRelPO relation = new IdpGroupUserRelPO();
      relation.setId(idGenerator.nextId());
      relation.setGroupId(groupPO.getGroupId());
      relation.setUserId(user.getUserId());
      relation.setAuditInfo(auditInfo);
      relation.setCurrentVersion(INITIAL_VERSION);
      relation.setLastVersion(INITIAL_VERSION);
      relation.setDeletedAt(0L);
      relations.add(relation);
    }

    if (relations.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpAuthenticationMapper.class, mapper -> mapper.batchInsertLocalGroupUsers(relations));
  }

  public void removeUsersFromGroup(IdpGroupPO groupPO, List<IdpUserPO> users, String actor) {
    List<Long> userIds = users.stream().map(IdpUserPO::getUserId).collect(Collectors.toList());
    if (userIds.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpAuthenticationMapper.class,
        mapper ->
            mapper.softDeleteLocalGroupUsers(
                groupPO.getGroupId(),
                userIds,
                Instant.now().toEpochMilli(),
                toAuditJson(newAudit(actor))));
  }

  public void truncateAll() {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithCommit(
                IdpAuthenticationMapper.class, IdpAuthenticationMapper::truncateLocalGroupUserRel),
        () ->
            SessionUtils.doWithCommit(
                IdpAuthenticationMapper.class, IdpAuthenticationMapper::truncateLocalGroupMeta),
        () ->
            SessionUtils.doWithCommit(
                IdpAuthenticationMapper.class, IdpAuthenticationMapper::truncateLocalUserMeta));
  }

  private AuditInfo newAudit(String actor) {
    Instant now = Instant.now();
    return AuditInfo.builder()
        .withCreator(actor)
        .withCreateTime(now)
        .withLastModifier(actor)
        .withLastModifiedTime(now)
        .build();
  }

  private AuditInfo updateAudit(AuditInfo original, String actor) {
    Instant now = Instant.now();
    return AuditInfo.builder()
        .withCreator(original.creator())
        .withCreateTime(original.createTime())
        .withLastModifier(actor)
        .withLastModifiedTime(now)
        .build();
  }

  private String toAuditJson(AuditInfo auditInfo) {
    try {
      return JsonUtils.objectMapper().writeValueAsString(auditInfo);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize built-in authentication audit info", e);
    }
  }

  private AuditInfo fromAuditJson(String auditInfo) {
    try {
      return JsonUtils.objectMapper().readValue(auditInfo, AuditInfo.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize built-in authentication audit info", e);
    }
  }
}
