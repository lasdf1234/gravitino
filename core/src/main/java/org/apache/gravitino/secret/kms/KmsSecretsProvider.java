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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientRegistry;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.secret.GravitinoSecretProvider;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.SecretWriteContext;

/**
 * Secret provider that owns a dedicated {@link KmsClientRegistry} built from this provider's own
 * configuration.
 *
 * <p>Provider configuration is mapped onto a synthetic {@code gravitino.kms.*} single-source
 * configuration; the provider name becomes the KMS source name. Required keys:
 *
 * <ul>
 *   <li>{@code api} – KMS API identifier (for example {@code aws-kms})
 *   <li>{@code keyId} – KMS key id used when resolving the key reference
 *   <li>other keys – forwarded as {@code gravitino.kms.source.<name>.*} (for example {@code
 *       endpoint.region})
 * </ul>
 *
 * <p>{@link KmsClient} currently exposes key metadata only (no wrap/unwrap). This provider verifies
 * the configured key via {@link KmsClient#getKeyProperties(KmsReference)} and stores secret values
 * in process memory. Replace the local store with KMS wrap/unwrap once those operations are
 * available on {@link KmsClient}.
 */
public class KmsSecretsProvider implements GravitinoSecretProvider {

  public static final String TYPE = "kms";

  private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();

  private String providerName;
  private KmsClientRegistry kmsClientRegistry;
  private KmsReference keyReference;

  @Override
  public void initialize(String name, Map<String, String> config) {
    this.providerName = name;
    String keyId = SecretKmsConfigMapper.requireKeyId(config);
    Map<String, String> kmsConfig = SecretKmsConfigMapper.toKmsConfig(name, config);
    this.kmsClientRegistry = new KmsClientRegistry(new MapBackedConfig(kmsConfig));
    String api = config.get(SecretKmsConfigMapper.API_KEY).trim();
    this.keyReference = new KmsReference(api, name, keyId);
    requireUsableKey(keyReference);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String writeSecret(String plaintext, SecretWriteContext context) {
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (context == null) {
      throw new IllegalArgumentException("context must not be null");
    }
    ensureInitialized();
    requireUsableKey(keyReference);

    String urn =
        SecretUrn.buildWriteThrough(
            context.providerName() == null || context.providerName().isBlank()
                ? providerName
                : context.providerName(),
            context.entityType(),
            context.entityId(),
            context.propertyKey());
    // TODO: replace with KmsClient wrap/encrypt when cryptographic APIs are available.
    secrets.put(
        urn, Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8)));
    return urn;
  }

  @Override
  public String readSecret(String urn) {
    ensureInitialized();
    requireUsableKey(keyReference);
    String encoded = secrets.get(urn);
    if (encoded == null) {
      throw new IllegalArgumentException("Secret not found for URN: " + urn);
    }
    return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
  }

  @Override
  public void deleteSecret(String urn) {
    ensureInitialized();
    secrets.remove(urn);
  }

  @Override
  public void close() {
    if (kmsClientRegistry != null) {
      kmsClientRegistry.close();
      kmsClientRegistry = null;
    }
    secrets.clear();
  }

  KmsClientRegistry kmsClientRegistry() {
    return kmsClientRegistry;
  }

  KmsReference keyReference() {
    return keyReference;
  }

  private void ensureInitialized() {
    if (kmsClientRegistry == null || keyReference == null) {
      throw new IllegalStateException("KmsSecretsProvider has not been initialized");
    }
  }

  private void requireUsableKey(KmsReference reference) {
    KmsClient client = kmsClientRegistry.getClient(reference);
    Optional<KmsKeyProperties> properties = client.getKeyProperties(reference);
    if (properties.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "KMS key '%s' was not found for source '%s'", reference.keyId(), reference.source()));
    }
    KmsKeyProperties keyProperties = properties.get();
    if (!keyProperties.enabled()) {
      throw new IllegalArgumentException(
          String.format("KMS key '%s' is disabled", reference.keyId()));
    }
    if (!keyProperties.supportsWrapping() || !keyProperties.supportsUnwrapping()) {
      throw new IllegalArgumentException(
          String.format(
              "KMS key '%s' must support wrapping and unwrapping for secret storage",
              reference.keyId()));
    }
  }
}
