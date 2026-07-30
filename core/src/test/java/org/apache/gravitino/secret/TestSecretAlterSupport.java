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

import static org.apache.gravitino.secret.SecretConstants.SECRET_KEYS_PROPERTY;
import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSecretAlterSupport {

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

  @Test
  public void testPrepareSchemaChangesRewritesBinding() {
    Map<String, String> currentProps = ImmutableMap.of();
    SchemaChange[] prepared =
        SecretAlterSupport.prepareSchemaChanges(
            currentProps,
            10L,
            new SchemaChange[] {
              SchemaChange.setSecretBinding("jdbc-password", "memory", "plain-text")
            },
            registry);

    Assertions.assertEquals(2, prepared.length);
    Assertions.assertTrue(prepared[0] instanceof SchemaChange.SetProperty);
    SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) prepared[0];
    Assertions.assertEquals("jdbc-password", setProperty.getProperty());
    Assertions.assertTrue(setProperty.getValue().startsWith(URN_PREFIX));

    Assertions.assertTrue(prepared[1] instanceof SchemaChange.SetProperty);
    SchemaChange.SetProperty secretKeys = (SchemaChange.SetProperty) prepared[1];
    Assertions.assertEquals(SECRET_KEYS_PROPERTY, secretKeys.getProperty());
    Assertions.assertEquals("jdbc-password", secretKeys.getValue());
  }

  @Test
  public void testPrepareFilesetChangesRemoveSecret() {
    Map<String, String> currentProps =
        SecretPropertyHelper.applyOnCreate(
            "fileset",
            11L,
            ImmutableMap.of("token", "old"),
            ImmutableMap.of("token", "memory"),
            null,
            registry);

    FilesetChange[] prepared =
        SecretAlterSupport.prepareFilesetChanges(
            currentProps,
            11L,
            new FilesetChange[] {FilesetChange.removeProperty("token")},
            registry);

    Assertions.assertEquals(2, prepared.length);
    Assertions.assertTrue(prepared[0] instanceof FilesetChange.RemoveProperty);
    Assertions.assertTrue(prepared[1] instanceof FilesetChange.RemoveProperty);
    Assertions.assertEquals(
        SECRET_KEYS_PROPERTY, ((FilesetChange.RemoveProperty) prepared[1]).getProperty());
  }

  @Test
  public void testPrepareSchemaChangesRequiresRegistry() {
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                SecretAlterSupport.prepareSchemaChanges(
                    ImmutableMap.of(),
                    1L,
                    new SchemaChange[] {
                      SchemaChange.setSecretBinding("password", "memory", "value")
                    },
                    null));
    Assertions.assertTrue(exception.getMessage().contains("secret provider registry"));
  }

  @Test
  public void testContainsSecretChanges() {
    Assertions.assertTrue(
        SecretAlterSupport.containsSecretChanges(
            SchemaChange.setSecretBinding("password", "memory", "value")));
    Assertions.assertFalse(
        SecretAlterSupport.containsSecretChanges(SchemaChange.setProperty("key", "value")));
  }
}
