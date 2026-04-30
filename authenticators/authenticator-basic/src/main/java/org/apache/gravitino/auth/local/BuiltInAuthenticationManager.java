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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.local.dto.BuiltInGroupDTO;
import org.apache.gravitino.auth.local.dto.BuiltInUserDTO;
import org.apache.gravitino.auth.local.store.IdpAuthenticationStore;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

/** Main service for built-in basic authentication. */
public class BuiltInAuthenticationManager {

  private static final String BASIC_CHALLENGE = "Basic realm=\"Gravitino\"";
  static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";
  private static final String USER_PASSWORD_RESET_PATH_PREFIX = "/api/idp/users/";

  private final Config config;
  private final IdGenerator idGenerator;
  private final IdpAuthenticationStore store;
  private final PasswordHasher passwordHasher;

  public static BuiltInAuthenticationManager fromEnvironment() {
    GravitinoEnv env = GravitinoEnv.getInstance();
    return fromConfig(env.config());
  }

  public static BuiltInAuthenticationManager fromConfig(Config config) {
    return new BuiltInAuthenticationManager(
        config,
        GravitinoEnv.getInstance().idGenerator(),
        new IdpAuthenticationStore(GravitinoEnv.getInstance().idGenerator()),
        PasswordHasherFactory.create());
  }

  BuiltInAuthenticationManager(
      Config config,
      IdGenerator idGenerator,
      IdpAuthenticationStore store,
      PasswordHasher passwordHasher) {
    this.config = config;
    this.idGenerator = idGenerator;
    this.store = store;
    this.passwordHasher = passwordHasher;
  }

  void initializeServiceAdmins() {
    initializeServiceAdmins(System.getenv(INITIAL_ADMIN_PASSWORD_ENV));
  }

  void initializeServiceAdmins(String initialAdminPasswordsRaw) {
    ensureBasicEnabled();
    List<String> configuredServiceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (configuredServiceAdmins == null || configuredServiceAdmins.isEmpty()) {
      return;
    }

    Set<String> serviceAdmins = new LinkedHashSet<>(configuredServiceAdmins);
    Map<String, String> initialAdminPasswords =
        parseInitialAdminPasswords(initialAdminPasswordsRaw);
    for (String userName : initialAdminPasswords.keySet()) {
      Preconditions.checkArgument(
          serviceAdmins.contains(userName),
          "%s contains a credential for non-service-admin user %s",
          INITIAL_ADMIN_PASSWORD_ENV,
          userName);
    }

    for (String serviceAdmin : serviceAdmins) {
      validateUserName(serviceAdmin);
      if (store.findUser(serviceAdmin).isPresent()) {
        continue;
      }

      String password = initialAdminPasswords.get(serviceAdmin);
      Preconditions.checkState(
          StringUtils.isNotBlank(password),
          "Service admin %s is missing a local password. Set %s to a JSON array like "
              + "[\"%s:<password>\"] or create the user before startup",
          serviceAdmin,
          INITIAL_ADMIN_PASSWORD_ENV,
          serviceAdmin);
      store.createUser(
          idGenerator.nextId(), serviceAdmin, passwordHasher.hash(password), serviceAdmin);
    }
  }

  /** Authenticate one HTTP basic request. */
  public Principal authenticate(HttpServletRequest request, byte[] tokenData) {
    BasicCredential credential = parseCredential(tokenData);
    Optional<IdpUserPO> user = store.findUser(credential.userName);
    if (user.isPresent()) {
      if (!passwordHasher.verify(credential.password, user.get().getPasswordHash())) {
        throw new UnauthorizedException("Invalid username or password", BASIC_CHALLENGE);
      }

      return new BuiltInAuthenticationPrincipal(
          user.get().getUserName(),
          toUserGroups(store.listGroupNames(user.get().getUserName())),
          credential.authorizationHeader,
          false);
    }

    if (isBootstrapCredential(credential.userName, credential.password)) {
      if (isBootstrapPasswordResetRequest(request, credential.userName)) {
        return new BuiltInAuthenticationPrincipal(
            credential.userName, new ArrayList<>(), credential.authorizationHeader, true);
      }

      throw new UnauthorizedException(
          "Bootstrap credentials can only be used to reset the bootstrap password",
          BASIC_CHALLENGE);
    }

    throw new UnauthorizedException("Invalid username or password", BASIC_CHALLENGE);
  }

  public BuiltInUserDTO createUser(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    validatePassword(password);
    if (store.findUser(userName).isPresent()) {
      throw new UserAlreadyExistsException("Local user %s already exists", userName);
    }

    store.createUser(idGenerator.nextId(), userName, passwordHasher.hash(password), currentUser());
    return getUser(userName);
  }

  public BuiltInUserDTO getUser(String userName) {
    ensureBasicEnabled();
    validateUserName(userName);
    IdpUserPO userPO =
        store
            .findUser(userName)
            .orElseThrow(() -> new NoSuchUserException("Local user %s does not exist", userName));
    return toUserDTO(userPO);
  }

  public String[] listUsers() {
    ensureBasicEnabled();
    ensureServiceAdmin();
    return store.listUserNames().toArray(new String[0]);
  }

  public boolean deleteUser(String userName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    Optional<IdpUserPO> user = store.findUser(userName);
    return user.isPresent() && store.deleteUser(user.get(), currentUser());
  }

  public BuiltInUserDTO resetPassword(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    validatePassword(password);
    Optional<IdpUserPO> user = store.findUser(userName);
    String passwordHash = passwordHasher.hash(password);
    if (user.isPresent()) {
      if (passwordHasher.verify(password, user.get().getPasswordHash())) {
        throw new PasswordUnchangedException(
            "The new password must be different from the old password");
      }

      store.updatePassword(user.get(), passwordHash, currentUser());
      return getUser(userName);
    }

    if (isBootstrapCreationAllowed(userName)) {
      store.createUser(idGenerator.nextId(), userName, passwordHash, currentUser());
      return getUser(userName);
    }

    throw new NoSuchUserException("Local user %s does not exist", userName);
  }

  public BuiltInGroupDTO createGroup(String groupName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    if (store.findGroup(groupName).isPresent()) {
      throw new GroupAlreadyExistsException("Local group %s already exists", groupName);
    }

    store.createGroup(idGenerator.nextId(), groupName, currentUser());
    return getGroup(groupName);
  }

  public BuiltInGroupDTO getGroup(String groupName) {
    ensureBasicEnabled();
    validateGroupName(groupName);
    IdpGroupPO groupPO =
        store
            .findGroup(groupName)
            .orElseThrow(
                () -> new NoSuchGroupException("Local group %s does not exist", groupName));
    return toGroupDTO(groupPO);
  }

  public String[] listGroups() {
    ensureBasicEnabled();
    ensureServiceAdmin();
    return store.listGroupNames().toArray(new String[0]);
  }

  public boolean deleteGroup(String groupName) {
    return deleteGroup(groupName, false);
  }

  public boolean deleteGroup(String groupName, boolean force) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    Optional<IdpGroupPO> group = store.findGroup(groupName);
    if (!group.isPresent()) {
      return false;
    }

    validateGroupDeletion(groupName, force);
    return store.deleteGroup(group.get(), currentUser());
  }

  public BuiltInGroupDTO addUsersToGroup(String groupName, List<String> userNames) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    store.addUsersToGroup(groupPO, users, currentUser());
    return getGroup(groupName);
  }

  public BuiltInGroupDTO removeUsersFromGroup(String groupName, List<String> userNames) {
    return removeUsersFromGroup(groupName, userNames, false);
  }

  public BuiltInGroupDTO removeUsersFromGroup(
      String groupName, List<String> userNames, boolean force) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    validateGroupUserRemoval(groupName, userNames, force);
    store.removeUsersFromGroup(groupPO, users, currentUser());
    return getGroup(groupName);
  }

  private Map<String, String> parseInitialAdminPasswords(String initialAdminPasswordsRaw) {
    if (StringUtils.isBlank(initialAdminPasswordsRaw)) {
      return ImmutableMap.of();
    }

    List<String> entries;
    try {
      entries =
          JsonUtils.objectMapper()
              .readValue(initialAdminPasswordsRaw, new TypeReference<List<String>>() {});
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "%s must be a JSON array of \"username:password\" strings",
              INITIAL_ADMIN_PASSWORD_ENV),
          e);
    }

    Preconditions.checkArgument(entries != null, "%s must not be null", INITIAL_ADMIN_PASSWORD_ENV);
    Map<String, String> initialAdminPasswords = new LinkedHashMap<>();
    for (String entry : entries) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(entry),
          "%s must not contain blank entries",
          INITIAL_ADMIN_PASSWORD_ENV);
      int separatorIndex = entry.indexOf(':');
      Preconditions.checkArgument(
          separatorIndex > 0 && separatorIndex < entry.length() - 1,
          "%s entries must use the format username:password",
          INITIAL_ADMIN_PASSWORD_ENV);
      String userName = entry.substring(0, separatorIndex);
      String password = entry.substring(separatorIndex + 1);
      validateUserName(userName);
      validatePassword(password);
      Preconditions.checkArgument(
          !initialAdminPasswords.containsKey(userName),
          "%s contains duplicate credentials for service admin %s",
          INITIAL_ADMIN_PASSWORD_ENV,
          userName);
      initialAdminPasswords.put(userName, password);
    }

    return ImmutableMap.copyOf(initialAdminPasswords);
  }

  private BuiltInUserDTO toUserDTO(IdpUserPO userPO) {
    return new BuiltInUserDTO(userPO.getUserName(), store.listGroupNames(userPO.getUserName()));
  }

  private BuiltInGroupDTO toGroupDTO(IdpGroupPO groupPO) {
    return new BuiltInGroupDTO(groupPO.getGroupName(), store.listUserNames(groupPO.getGroupName()));
  }

  private List<IdpUserPO> requireUsers(List<String> userNames) {
    Preconditions.checkArgument(userNames != null, "Users are required");
    Set<String> normalizedUsers =
        userNames.stream()
            .peek(this::validateUserName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<IdpUserPO> users = store.findUsers(new ArrayList<>(normalizedUsers));
    if (users.size() != normalizedUsers.size()) {
      Set<String> found = users.stream().map(IdpUserPO::getUserName).collect(Collectors.toSet());
      String missingUser =
          normalizedUsers.stream().filter(name -> !found.contains(name)).findFirst().orElse("");
      throw new NoSuchUserException("Local user %s does not exist", missingUser);
    }

    return users;
  }

  private IdpGroupPO requireGroup(String groupName) {
    validateGroupName(groupName);
    return store
        .findGroup(groupName)
        .orElseThrow(() -> new NoSuchGroupException("Local group %s does not exist", groupName));
  }

  private List<UserGroup> toUserGroups(List<String> groupNames) {
    return groupNames.stream()
        .map(name -> new UserGroup(Optional.empty(), name))
        .collect(Collectors.toList());
  }

  private BasicCredential parseCredential(byte[] tokenData) {
    if (tokenData == null) {
      throw new UnauthorizedException("Missing basic authorization header", BASIC_CHALLENGE);
    }

    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      throw new UnauthorizedException("Invalid basic authorization header", BASIC_CHALLENGE);
    }

    String base64Credential =
        authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length()).trim();
    if (StringUtils.isBlank(base64Credential)) {
      throw new UnauthorizedException("Invalid basic authorization header", BASIC_CHALLENGE);
    }

    String decodedCredential;
    try {
      decodedCredential =
          new String(Base64.getDecoder().decode(base64Credential), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      throw new UnauthorizedException("Invalid basic authorization header", BASIC_CHALLENGE);
    }

    int separatorIndex = decodedCredential.indexOf(':');
    if (separatorIndex <= 0 || separatorIndex == decodedCredential.length() - 1) {
      throw new UnauthorizedException("Invalid basic authorization header", BASIC_CHALLENGE);
    }

    return new BasicCredential(
        decodedCredential.substring(0, separatorIndex),
        decodedCredential.substring(separatorIndex + 1),
        authData);
  }

  private boolean isBootstrapCredential(String userName, String password) {
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || serviceAdmins.isEmpty()) {
      return false;
    }

    String bootstrapUser = serviceAdmins.get(0);
    return bootstrapUser.equals(userName) && bootstrapUser.equals(password);
  }

  private boolean isBootstrapPasswordResetRequest(HttpServletRequest request, String userName) {
    return request != null
        && "PUT".equalsIgnoreCase(request.getMethod())
        && (USER_PASSWORD_RESET_PATH_PREFIX + userName).equals(request.getRequestURI());
  }

  private boolean isBootstrapCreationAllowed(String userName) {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    return principal instanceof BuiltInAuthenticationPrincipal
        && ((BuiltInAuthenticationPrincipal) principal).bootstrapAuthenticated()
        && serviceAdmins != null
        && !serviceAdmins.isEmpty()
        && serviceAdmins.get(0).equals(userName);
  }

  private void ensureBasicEnabled() {
    Preconditions.checkState(
        config.get(Configs.AUTHENTICATORS).contains("basic"),
        "Built-in basic authentication is disabled");
  }

  private void ensureServiceAdmin() {
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || !serviceAdmins.contains(currentUser())) {
      throw new ForbiddenException("Only Gravitino service admins can manage built-in identities");
    }
  }

  private void validateUserName(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(!userName.contains(":"), "User name cannot contain ':'");
  }

  private void validateGroupName(String groupName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name is required");
  }

  private void validatePassword(String password) {
    Preconditions.checkArgument(StringUtils.isNotBlank(password), "Password is required");
    Preconditions.checkArgument(
        password.length() >= 12, "Password length must be at least 12 characters");
    Preconditions.checkArgument(
        password.length() <= 64, "Password length must be at most 64 characters");
  }

  private void validateGroupUserRemoval(String groupName, List<String> userNames, boolean force) {
    List<String> existingUsers = store.listUserNames(groupName);
    Set<String> currentUsers = new LinkedHashSet<>(existingUsers);
    currentUsers.removeAll(new LinkedHashSet<>(userNames));
    if (!force && !existingUsers.isEmpty() && currentUsers.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing all users from local group %s is dangerous, retry with force=true if"
                  + " this is intended",
              groupName));
    }
  }

  private void validateGroupDeletion(String groupName, boolean force) {
    if (!force && !store.listUserNames(groupName).isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing local group %s is dangerous while it still has users, retry with"
                  + " force=true if this is intended",
              groupName));
    }
  }

  private String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }

  private static class BasicCredential {
    private final String userName;
    private final String password;
    private final String authorizationHeader;

    private BasicCredential(String userName, String password, String authorizationHeader) {
      this.userName = userName;
      this.password = password;
      this.authorizationHeader = authorizationHeader;
    }
  }
}
