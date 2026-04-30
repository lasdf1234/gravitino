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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.local.dto.BuiltInGroupDTO;
import org.apache.gravitino.auth.local.store.IdpAuthenticationStore;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBuiltInAuthenticationManager {

  private Config config;
  private IdpAuthenticationStore store;
  private PasswordHasher passwordHasher;
  private BuiltInAuthenticationManager manager;

  @BeforeEach
  public void setUp() {
    config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Collections.singletonList("basic"));
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    store = mock(IdpAuthenticationStore.class);
    passwordHasher = mock(PasswordHasher.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    when(idGenerator.nextId()).thenReturn(1L);
    manager = new BuiltInAuthenticationManager(config, idGenerator, store, passwordHasher);
  }

  @Test
  public void testAuthenticateStoredUser() throws Exception {
    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(1L);
    userPO.setUserName("alice");
    userPO.setPasswordHash("hashed-password");
    userPO.setAuditInfo(toAuditJson());
    when(store.findUser("alice")).thenReturn(Optional.of(userPO));
    when(store.listGroupNames("alice")).thenReturn(Arrays.asList("dev", "ops"));
    when(passwordHasher.verify("secret", "hashed-password")).thenReturn(true);

    Principal principal =
        manager.authenticate(mock(HttpServletRequest.class), basicToken("alice", "secret"));

    assertEquals("alice", principal.getName());
    assertEquals(2, ((UserPrincipal) principal).getGroups().size());
    assertEquals("dev", ((UserPrincipal) principal).getGroups().get(0).getGroupname());
  }

  @Test
  public void testBootstrapCredentialsOnlyWorkForPasswordReset() {
    when(store.findUser("admin")).thenReturn(Optional.empty());

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/idp/users/admin");

    assertThrows(
        UnauthorizedException.class,
        () -> manager.authenticate(request, basicToken("admin", "admin")));
  }

  @Test
  public void testBootstrapPasswordResetCreatesBootstrapUser() throws Exception {
    IdpUserPO createdUser = new IdpUserPO();
    createdUser.setUserId(1L);
    createdUser.setUserName("admin");
    createdUser.setPasswordHash("new-hash");
    createdUser.setAuditInfo(toAuditJson());
    when(store.findUser("admin")).thenReturn(Optional.empty(), Optional.of(createdUser));
    when(store.listGroupNames("admin")).thenReturn(Collections.emptyList());
    when(passwordHasher.hash("new-password")).thenReturn("new-hash");

    PrincipalUtils.doAs(
        new BuiltInAuthenticationPrincipal("admin", Collections.emptyList(), "Basic test", true),
        () -> {
          manager.resetPassword("admin", "new-password");
          return null;
        });

    verify(store).createUser(anyLong(), eq("admin"), eq("new-hash"), eq("admin"));
  }

  @Test
  public void testInitializeServiceAdminsCreatesMissingAdminsFromEnvironment() {
    config.set(Configs.SERVICE_ADMINS, Arrays.asList("admin1", "admin2"));
    when(store.findUser("admin1")).thenReturn(Optional.empty());
    when(store.findUser("admin2")).thenReturn(Optional.empty());
    when(passwordHasher.hash("passwordForAdmin1")).thenReturn("hash-1");
    when(passwordHasher.hash("passwordForAdmin2")).thenReturn("hash-2");

    manager.initializeServiceAdmins("[\"admin1:passwordForAdmin1\", \"admin2:passwordForAdmin2\"]");

    verify(store).createUser(anyLong(), eq("admin1"), eq("hash-1"), eq("admin1"));
    verify(store).createUser(anyLong(), eq("admin2"), eq("hash-2"), eq("admin2"));
  }

  @Test
  public void testInitializeServiceAdminsSkipsExistingAdmins() throws Exception {
    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(1L);
    userPO.setUserName("admin");
    userPO.setPasswordHash("hash");
    userPO.setAuditInfo(toAuditJson());
    when(store.findUser("admin")).thenReturn(Optional.of(userPO));

    manager.initializeServiceAdmins("[\"admin:passwordForAdmin\"]");

    verify(store, never()).createUser(anyLong(), eq("admin"), anyString(), eq("admin"));
    verify(passwordHasher, never()).hash("passwordForAdmin");
  }

  @Test
  public void testInitializeServiceAdminsFailsWhenMissingAdminHasNoCredential() {
    when(store.findUser("admin")).thenReturn(Optional.empty());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> manager.initializeServiceAdmins(null));

    assertEquals(
        "Service admin admin is missing a local password. Set GRAVITINO_INITIAL_ADMIN_PASSWORD"
            + " to a JSON array like [\"admin:<password>\"] or create the user before startup",
        exception.getMessage());
  }

  @Test
  public void testInitializeServiceAdminsFailsForInvalidEnvironmentJson() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> manager.initializeServiceAdmins("admin:password"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD must be a JSON array of \"username:password\" strings",
        exception.getMessage());
    assertInstanceOf(JsonProcessingException.class, exception.getCause());
  }

  @Test
  public void testDeleteGroupRequiresForceWhenGroupHasUsers() throws Exception {
    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(1L);
    groupPO.setGroupName("engineering");
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.listUserNames("engineering")).thenReturn(Collections.singletonList("alice"));

    UnsupportedOperationException exception =
        PrincipalUtils.doAs(
            new BuiltInAuthenticationPrincipal(
                "admin", Collections.emptyList(), "Basic test", false),
            () ->
                assertThrows(
                    UnsupportedOperationException.class,
                    () -> manager.deleteGroup("engineering", false)));

    assertEquals(
        "Removing local group engineering is dangerous while it still has users, retry with"
            + " force=true if this is intended",
        exception.getMessage());
  }

  @Test
  public void testDeleteGroupAllowsForceWhenGroupHasUsers() throws Exception {
    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(1L);
    groupPO.setGroupName("engineering");
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.listUserNames("engineering")).thenReturn(Collections.singletonList("alice"));
    when(store.deleteGroup(groupPO, "admin")).thenReturn(true);

    boolean removed =
        PrincipalUtils.doAs(
            new BuiltInAuthenticationPrincipal(
                "admin", Collections.emptyList(), "Basic test", false),
            () -> manager.deleteGroup("engineering", true));

    assertEquals(true, removed);
    verify(store).deleteGroup(groupPO, "admin");
  }

  @Test
  public void testRemoveUsersFromGroupRequiresForceToRemoveAllMembers() throws Exception {
    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(1L);
    groupPO.setGroupName("engineering");
    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(2L);
    userPO.setUserName("alice");
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.findUsers(Collections.singletonList("alice")))
        .thenReturn(Collections.singletonList(userPO));
    when(store.listUserNames("engineering")).thenReturn(Collections.singletonList("alice"));

    UnsupportedOperationException exception =
        PrincipalUtils.doAs(
            new BuiltInAuthenticationPrincipal(
                "admin", Collections.emptyList(), "Basic test", false),
            () ->
                assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                        manager.removeUsersFromGroup(
                            "engineering", Collections.singletonList("alice"), false)));

    assertEquals(
        "Removing all users from local group engineering is dangerous, retry with force=true if"
            + " this is intended",
        exception.getMessage());
  }

  @Test
  public void testRemoveUsersFromGroupAllowsForceToRemoveAllMembers() throws Exception {
    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(1L);
    groupPO.setGroupName("engineering");
    groupPO.setAuditInfo(toAuditJson());
    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(2L);
    userPO.setUserName("alice");
    userPO.setAuditInfo(toAuditJson());
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.findUsers(Collections.singletonList("alice")))
        .thenReturn(Collections.singletonList(userPO));
    when(store.listUserNames("engineering"))
        .thenReturn(Collections.singletonList("alice"), Collections.emptyList());

    BuiltInGroupDTO groupDTO =
        PrincipalUtils.doAs(
            new BuiltInAuthenticationPrincipal(
                "admin", Collections.emptyList(), "Basic test", false),
            () ->
                manager.removeUsersFromGroup(
                    "engineering", Collections.singletonList("alice"), true));

    assertEquals("engineering", groupDTO.name());
    assertEquals(Collections.emptyList(), groupDTO.users());
    verify(store).removeUsersFromGroup(groupPO, Collections.singletonList(userPO), "admin");
  }

  private byte[] basicToken(String userName, String password) {
    return (AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((userName + ":" + password).getBytes(StandardCharsets.UTF_8)))
        .getBytes(StandardCharsets.UTF_8);
  }

  private String toAuditJson() throws Exception {
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator("admin")
            .withCreateTime(Instant.now())
            .withLastModifier("admin")
            .withLastModifiedTime(Instant.now())
            .build();
    return JsonUtils.objectMapper().writeValueAsString(auditInfo);
  }
}
