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

package org.apache.gravitino.secret;

import static org.apache.gravitino.secret.SecretConstants.MASK;
import static org.apache.gravitino.secret.SecretConstants.SECRET_KEYS_PROPERTY;
import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSecretPropertyHelper {

  private SecretProviderRegistry registry;

  @BeforeEach
  public void setUp() {
    registry = new SecretProviderRegistry();
    registry.init(
        ImmutableMap.of(
            "gravitino.secret.providers",
            "memory",
            "gravitino.secret.provider.memory.className",
            InMemorySecretsProvider.class.getName()));
  }

  @AfterEach
  public void tearDown() {
    SecretCreateContext.clear();
  }

  @Test
  public void testApplyOnCreateWithBinding() {
    Map<String, String> properties = ImmutableMap.of("jdbc-password", "plain-text");
    Map<String, String> bindings = ImmutableMap.of("jdbc-password", "memory");

    Map<String, String> result =
        SecretPropertyHelper.applyOnCreate("catalog", 1L, properties, bindings, null, registry);

    Assertions.assertEquals("jdbc-password", result.get(SECRET_KEYS_PROPERTY));
    String urn = result.get("jdbc-password");
    Assertions.assertTrue(urn.startsWith(URN_PREFIX));
    Assertions.assertEquals("plain-text", registry.get("memory").readSecret(urn));
  }

  @Test
  public void testResolveOmitAndCleanup() {
    Map<String, String> properties = ImmutableMap.of("jdbc-password", "plain-text");
    Map<String, String> stored =
        SecretPropertyHelper.applyOnCreate(
            "fileset", 9L, properties, ImmutableMap.of("jdbc-password", "memory"), null, registry);

    Map<String, String> resolved = SecretPropertyHelper.resolveSecrets(stored, registry);
    Assertions.assertEquals("plain-text", resolved.get("jdbc-password"));

    Map<String, String> omitted = SecretPropertyHelper.omitSecrets(stored);
    Assertions.assertFalse(omitted.containsKey("jdbc-password"));
    Assertions.assertFalse(omitted.containsKey(SECRET_KEYS_PROPERTY));

    String urn = stored.get("jdbc-password");
    SecretPropertyHelper.cleanupOnDrop("fileset", 9L, stored, registry);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> registry.get("memory").readSecret(urn));
  }

  @Test
  public void testApplyOnCreateUsesConfiguredProviderName() {
    registry = new SecretProviderRegistry();
    registry.init(
        ImmutableMap.of(
            "gravitino.secret.providers",
            "local",
            "gravitino.secret.provider.local.className",
            InMemorySecretsProvider.class.getName()));

    Map<String, String> result =
        SecretPropertyHelper.applyOnCreate(
            "catalog",
            1L,
            ImmutableMap.of("authentication.password", "plain-text"),
            ImmutableMap.of("authentication.password", "local"),
            null,
            registry);

    Assertions.assertEquals(
        "urn:gravitino-secret:local:catalog:1:authentication.password",
        result.get("authentication.password"));
    Assertions.assertEquals(
        "plain-text", registry.get("local").readSecret(result.get("authentication.password")));
  }

  @Test
  public void testRejectClientSentSecretKeys() {
    Map<String, String> properties = ImmutableMap.of(SECRET_KEYS_PROPERTY, "jdbc-password");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecretPropertyHelper.applyOnCreate(
                    "catalog", 1L, properties, Map.of(), Map.of(), registry));
    Assertions.assertTrue(exception.getMessage().contains("gravitino.secret.keys"));
  }

  @Test
  public void testRejectOverlapAndMaskedBinding() {
    Map<String, String> properties = ImmutableMap.of("password", MASK);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applyOnCreate(
                "catalog", 1L, properties, ImmutableMap.of("password", "memory"), null, registry));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applyOnCreate(
                "catalog",
                1L,
                ImmutableMap.of("password", "x"),
                ImmutableMap.of("password", "memory"),
                ImmutableMap.of("password", new SecretReferenceLocator("memory", null)),
                registry));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applyOnCreate(
                "catalog",
                1L,
                ImmutableMap.of("password", "x"),
                Map.of(),
                ImmutableMap.of("password", new SecretReferenceLocator("memory", null)),
                registry));
  }

  @Test
  public void testRejectUnknownProviderAndExternalRefs() {
    Map<String, String> properties = ImmutableMap.of("password", "plain");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applyOnCreate(
                "catalog", 1L, properties, ImmutableMap.of("password", "vault"), null, registry));

    IllegalArgumentException externalRefException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecretPropertyHelper.applyOnCreate(
                    "catalog",
                    1L,
                    Map.of(),
                    Map.of(),
                    ImmutableMap.of(
                        "password",
                        new SecretReferenceLocator(
                            "memory", Map.of("mount", "mount", "path", "path"))),
                    registry));
    Assertions.assertTrue(externalRefException.getMessage().contains("external secret references"));

    IllegalArgumentException rawUrnException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecretPropertyHelper.applyOnCreate(
                    "catalog",
                    1L,
                    Map.of(),
                    Map.of(),
                    ImmutableMap.of(
                        "password",
                        new SecretReferenceLocator(
                            "memory", Map.of("path", URN_PREFIX + "memory:catalog:1:password"))),
                    registry));
    Assertions.assertTrue(rawUrnException.getMessage().contains("locator objects"));
  }

  @Test
  public void testSecretCreateContext() {
    SecretCreateParams params =
        new SecretCreateParams(
            "catalog",
            ImmutableMap.of("password", "memory"),
            ImmutableMap.of("token", new SecretReferenceLocator("memory", Map.of("path", "path"))));
    SecretCreateContext.set(params);
    Assertions.assertEquals(params, SecretCreateContext.get());
    SecretCreateContext.clear();
    Assertions.assertNull(SecretCreateContext.get());
  }

  @Test
  public void testSecretKeysParsing() {
    Map<String, String> properties =
        ImmutableMap.of(SECRET_KEYS_PROPERTY, " password , token ", "password", "urn");
    Assertions.assertEquals(
        java.util.Set.of("password", "token"), SecretPropertyHelper.secretKeys(properties));
  }

  @Test
  public void testApplySetSecretBindingReplace() {
    Map<String, String> props = new java.util.HashMap<>();
    SecretPropertyHelper.applySetSecretBinding(
        props, "catalog", 1L, "password", "memory", "first", registry);
    String firstUrn = props.get("password");

    SecretPropertyHelper.applySetSecretBinding(
        props, "catalog", 1L, "password", "memory", "second", registry);
    String secondUrn = props.get("password");
    Assertions.assertEquals(firstUrn, secondUrn);
    Assertions.assertEquals("second", registry.get("memory").readSecret(secondUrn));
  }

  @Test
  public void testApplyRemovePropertyCleansUpSecret() {
    Map<String, String> props = new java.util.HashMap<>();
    SecretPropertyHelper.applySetSecretBinding(
        props, "catalog", 2L, "token", "memory", "secret-value", registry);
    String urn = props.get("token");

    SecretPropertyHelper.applyRemoveProperty(props, "catalog", 2L, "token", registry);
    Assertions.assertFalse(props.containsKey("token"));
    Assertions.assertFalse(props.containsKey(SECRET_KEYS_PROPERTY));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> registry.get("memory").readSecret(urn));
  }

  @Test
  public void testRejectPlainSetOnSecretKey() {
    Map<String, String> props = new java.util.HashMap<>();
    SecretPropertyHelper.applySetSecretBinding(
        props, "catalog", 3L, "password", "memory", "value", registry);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SecretPropertyHelper.validatePlainSetProperty(props, "password", "new"));
    Assertions.assertTrue(exception.getMessage().contains("setSecretBinding"));
  }

  @Test
  public void testRejectMaskedBindingOnAlter() {
    Map<String, String> props = new java.util.HashMap<>();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applySetSecretBinding(
                props, "catalog", 4L, "password", "memory", MASK, registry));
  }

  @Test
  public void testRejectSecretKeysManagementOnAlter() {
    Map<String, String> props = new java.util.HashMap<>();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.validatePlainSetProperty(props, SECRET_KEYS_PROPERTY, "password"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyHelper.applyRemoveProperty(
                props, "catalog", 5L, SECRET_KEYS_PROPERTY, registry));
  }
}
