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

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretProviderRegistry {

  @Test
  public void testEmptyProvidersIsNoOp() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    registry.init(ImmutableMap.of());
    Assertions.assertTrue(registry.listNames().isEmpty());
    Assertions.assertTrue(registry.listProviderInfos().isEmpty());
    Assertions.assertNull(registry.get("memory"));
  }

  @Test
  public void testLoadMemoryProvider() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    registry.init(
        ImmutableMap.of(
            "gravitino.secret.providers",
            "memory",
            "gravitino.secret.provider.memory.className",
            InMemorySecretsProvider.class.getName()));

    Assertions.assertEquals(java.util.List.of("memory"), registry.listNames());
    Assertions.assertEquals(1, registry.listProviderInfos().size());
    Assertions.assertEquals(
        new SecretProviderInfo("memory", "memory"), registry.listProviderInfos().get(0));
    Assertions.assertTrue(registry.get("memory") instanceof InMemorySecretsProvider);
  }

  @Test
  public void testMissingClassNameFails() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> registry.init(ImmutableMap.of("gravitino.secret.providers", "memory")));
  }
}
