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

import java.util.List;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpGroupUserRelPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** MyBatis mapper for built-in authentication metadata. */
public interface IdpAuthenticationMapper {

  @Select(
      "SELECT user_id AS userId, user_name AS userName, password_hash AS passwordHash, "
          + "audit_info AS auditInfo, current_version AS currentVersion, "
          + "last_version AS lastVersion, deleted_at AS deletedAt "
          + "FROM idp_user_meta WHERE user_name = #{userName} AND deleted_at = 0")
  IdpUserPO selectLocalUser(@Param("userName") String userName);

  @Select(
      "<script>"
          + "SELECT user_id AS userId, user_name AS userName, password_hash AS passwordHash, "
          + "audit_info AS auditInfo, current_version AS currentVersion, "
          + "last_version AS lastVersion, deleted_at AS deletedAt "
          + "FROM idp_user_meta WHERE deleted_at = 0 AND user_name IN "
          + "<foreach item='item' collection='userNames' open='(' separator=',' close=')'>"
          + "#{item}"
          + "</foreach>"
          + "</script>")
  List<IdpUserPO> selectLocalUsers(@Param("userNames") List<String> userNames);

  @Insert(
      "INSERT INTO idp_user_meta (user_id, user_name, password_hash, audit_info, "
          + "current_version, last_version, deleted_at) VALUES (#{userId}, #{userName}, "
          + "#{passwordHash}, #{auditInfo}, #{currentVersion}, #{lastVersion}, #{deletedAt})")
  int insertLocalUser(IdpUserPO userPO);

  @Select("SELECT user_name FROM idp_user_meta WHERE deleted_at = 0 ORDER BY user_name")
  List<String> selectLocalUserNames();

  @Update(
      "UPDATE idp_user_meta SET password_hash = #{passwordHash}, audit_info = #{auditInfo}, "
          + "current_version = #{newCurrentVersion}, last_version = #{newLastVersion} "
          + "WHERE user_id = #{userId} AND current_version = #{currentVersion} AND deleted_at = 0")
  int updateLocalUserPassword(
      @Param("userId") long userId,
      @Param("passwordHash") String passwordHash,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") long currentVersion,
      @Param("newCurrentVersion") long newCurrentVersion,
      @Param("newLastVersion") long newLastVersion);

  @Update(
      "UPDATE idp_user_meta SET deleted_at = #{deletedAt}, audit_info = #{auditInfo}, "
          + "current_version = #{newCurrentVersion}, last_version = #{newLastVersion} "
          + "WHERE user_id = #{userId} AND current_version = #{currentVersion} AND deleted_at = 0")
  int softDeleteLocalUser(
      @Param("userId") long userId,
      @Param("deletedAt") long deletedAt,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") long currentVersion,
      @Param("newCurrentVersion") long newCurrentVersion,
      @Param("newLastVersion") long newLastVersion);

  @Select(
      "SELECT group_id AS groupId, group_name AS groupName, audit_info AS auditInfo, "
          + "current_version AS currentVersion, last_version AS lastVersion, "
          + "deleted_at AS deletedAt FROM idp_group_meta "
          + "WHERE group_name = #{groupName} AND deleted_at = 0")
  IdpGroupPO selectLocalGroup(@Param("groupName") String groupName);

  @Insert(
      "INSERT INTO idp_group_meta (group_id, group_name, audit_info, current_version, "
          + "last_version, deleted_at) VALUES (#{groupId}, #{groupName}, #{auditInfo}, "
          + "#{currentVersion}, #{lastVersion}, #{deletedAt})")
  int insertLocalGroup(IdpGroupPO groupPO);

  @Select("SELECT group_name FROM idp_group_meta WHERE deleted_at = 0 ORDER BY group_name")
  List<String> selectLocalGroupNames();

  @Update(
      "UPDATE idp_group_meta SET deleted_at = #{deletedAt}, audit_info = #{auditInfo}, "
          + "current_version = #{newCurrentVersion}, last_version = #{newLastVersion} "
          + "WHERE group_id = #{groupId} AND current_version = #{currentVersion} AND deleted_at = 0")
  int softDeleteLocalGroup(
      @Param("groupId") long groupId,
      @Param("deletedAt") long deletedAt,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") long currentVersion,
      @Param("newCurrentVersion") long newCurrentVersion,
      @Param("newLastVersion") long newLastVersion);

  @Select(
      "SELECT g.group_name FROM idp_group_user_rel r "
          + "JOIN idp_group_meta g ON g.group_id = r.group_id "
          + "WHERE r.user_id = #{userId} AND r.deleted_at = 0 AND g.deleted_at = 0 "
          + "ORDER BY g.group_name")
  List<String> selectGroupNamesByUserId(@Param("userId") long userId);

  @Select(
      "SELECT u.user_name FROM idp_group_user_rel r "
          + "JOIN idp_user_meta u ON u.user_id = r.user_id "
          + "WHERE r.group_id = #{groupId} AND r.deleted_at = 0 AND u.deleted_at = 0 "
          + "ORDER BY u.user_name")
  List<String> selectUserNamesByGroupId(@Param("groupId") long groupId);

  @Select(
      "<script>"
          + "SELECT user_id FROM idp_group_user_rel WHERE group_id = #{groupId} "
          + "AND deleted_at = 0 AND user_id IN "
          + "<foreach item='item' collection='userIds' open='(' separator=',' close=')'>"
          + "#{item}"
          + "</foreach>"
          + "</script>")
  List<Long> selectRelatedUserIds(
      @Param("groupId") long groupId, @Param("userIds") List<Long> userIds);

  @Insert(
      "<script>"
          + "INSERT INTO idp_group_user_rel (id, group_id, user_id, audit_info, current_version, "
          + "last_version, deleted_at) VALUES "
          + "<foreach item='item' collection='relations' separator=','>"
          + "(#{item.id}, #{item.groupId}, #{item.userId}, #{item.auditInfo}, "
          + "#{item.currentVersion}, #{item.lastVersion}, #{item.deletedAt})"
          + "</foreach>"
          + "</script>")
  int batchInsertLocalGroupUsers(@Param("relations") List<IdpGroupUserRelPO> relations);

  @Update(
      "<script>"
          + "UPDATE idp_group_user_rel SET deleted_at = #{deletedAt}, audit_info = #{auditInfo}, "
          + "current_version = current_version + 1, last_version = last_version + 1 "
          + "WHERE group_id = #{groupId} AND deleted_at = 0 AND user_id IN "
          + "<foreach item='item' collection='userIds' open='(' separator=',' close=')'>"
          + "#{item}"
          + "</foreach>"
          + "</script>")
  int softDeleteLocalGroupUsers(
      @Param("groupId") long groupId,
      @Param("userIds") List<Long> userIds,
      @Param("deletedAt") long deletedAt,
      @Param("auditInfo") String auditInfo);

  @Update(
      "UPDATE idp_group_user_rel SET deleted_at = #{deletedAt}, audit_info = #{auditInfo}, "
          + "current_version = current_version + 1, last_version = last_version + 1 "
          + "WHERE user_id = #{userId} AND deleted_at = 0")
  int softDeleteGroupUsersByUserId(
      @Param("userId") long userId,
      @Param("deletedAt") long deletedAt,
      @Param("auditInfo") String auditInfo);

  @Update(
      "UPDATE idp_group_user_rel SET deleted_at = #{deletedAt}, audit_info = #{auditInfo}, "
          + "current_version = current_version + 1, last_version = last_version + 1 "
          + "WHERE group_id = #{groupId} AND deleted_at = 0")
  int softDeleteGroupUsersByGroupId(
      @Param("groupId") long groupId,
      @Param("deletedAt") long deletedAt,
      @Param("auditInfo") String auditInfo);

  @Delete("DELETE FROM idp_group_user_rel")
  int truncateLocalGroupUserRel();

  @Delete("DELETE FROM idp_group_meta")
  int truncateLocalGroupMeta();

  @Delete("DELETE FROM idp_user_meta")
  int truncateLocalUserMeta();
}
