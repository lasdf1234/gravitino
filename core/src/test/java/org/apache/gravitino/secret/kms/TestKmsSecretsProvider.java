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

package org.apache.gravitino.secret.kms;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SecretWriteContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsSecretsProvider {

  @Test
  public void testInitializeOwnsDedicatedRegistryAndRoundTrip() {
    KmsSecretsProvider provider = new KmsSecretsProvider();
    provider.initialize(
        "kms-local",
        ImmutableMap.of(
            "api", TestSecretKmsClientFactory.API,
            "keyId", "alias/gravitino-secrets",
            "endpoint.region", "us-west-2"));

    Assertions.assertEquals(KmsSecretsProvider.TYPE, provider.type());
    Assertions.assertNotNull(provider.kmsClientRegistry());
    Assertions.assertEquals(
        new KmsReference(TestSecretKmsClientFactory.API, "kms-local", "alias/gravitino-secrets"),
        provider.keyReference());

    SecretWriteContext context =
        new SecretWriteContext("kms-local", "catalog", 42L, "jdbc-password");
    String urn = provider.writeSecret("s3cr3t", context);
    Assertions.assertEquals("urn:gravitino-secret:kms-local:catalog:42:jdbc-password", urn);
    Assertions.assertEquals("s3cr3t", provider.readSecret(urn));

    provider.deleteSecret(urn);
    Assertions.assertThrows(IllegalArgumentException.class, () -> provider.readSecret(urn));
    provider.close();
  }

  @Test
  public void testRegistryLoadsKmsProviderViaClassName() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    registry.init(
        ImmutableMap.of(
            "gravitino.secret.providers",
            "kms-local",
            "gravitino.secret.provider.kms-local.className",
            KmsSecretsProvider.class.getName(),
            "gravitino.secret.provider.kms-local.api",
            TestSecretKmsClientFactory.API,
            "gravitino.secret.provider.kms-local.keyId",
            "alias/gravitino-secrets"));

    Assertions.assertTrue(registry.get("kms-local") instanceof KmsSecretsProvider);
    registry.close();
  }

  @Test
  public void testTwoProvidersGetIndependentRegistries() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    registry.init(
        ImmutableMap.of(
            "gravitino.secret.providers",
            "kms-a,kms-b",
            "gravitino.secret.provider.kms-a.className",
            KmsSecretsProvider.class.getName(),
            "gravitino.secret.provider.kms-a.api",
            TestSecretKmsClientFactory.API,
            "gravitino.secret.provider.kms-a.keyId",
            "alias/a",
            "gravitino.secret.provider.kms-b.className",
            KmsSecretsProvider.class.getName(),
            "gravitino.secret.provider.kms-b.api",
            TestSecretKmsClientFactory.API,
            "gravitino.secret.provider.kms-b.keyId",
            "alias/b"));

    KmsSecretsProvider providerA = (KmsSecretsProvider) registry.get("kms-a");
    KmsSecretsProvider providerB = (KmsSecretsProvider) registry.get("kms-b");
    Assertions.assertNotSame(providerA.kmsClientRegistry(), providerB.kmsClientRegistry());
    registry.close();
  }
}
