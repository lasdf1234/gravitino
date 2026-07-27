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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/** Core helpers for applying and resolving entity secret properties. */
public final class SecretPropertyHelper {

  private static final Splitter SECRET_KEY_SPLITTER =
      Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Joiner SECRET_KEY_JOINER = Joiner.on(',');

  private SecretPropertyHelper() {}

  /**
   * Applies create-time secrets and returns a properties map containing secret URNs and {@link
   * SecretConstants#SECRET_KEYS_PROPERTY}.
   *
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param properties the incoming properties map
   * @param bindings property keys mapped to provider names for write-through secrets
   * @param refs property keys mapped to external secret locators
   * @param registry the secret provider registry
   * @return the updated properties map
   */
  public static Map<String, String> applyOnCreate(
      String entityType,
      long entityId,
      @Nullable Map<String, String> properties,
      @Nullable Map<String, String> bindings,
      @Nullable Map<String, SecretReferenceLocator> refs,
      SecretProviderRegistry registry) {
    Map<String, String> bindingsMap = bindings == null ? Map.of() : bindings;
    Map<String, SecretReferenceLocator> refsMap = refs == null ? Map.of() : refs;
    Map<String, String> result = properties == null ? new HashMap<>() : new HashMap<>(properties);

    if (result.containsKey(SECRET_KEYS_PROPERTY)) {
      throw new IllegalArgumentException("Client must not send gravitino.secret.keys");
    }

    for (String key : bindingsMap.keySet()) {
      if (refsMap.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Property key %s cannot be in both secretBindings and secretReferences", key));
      }
    }

    for (String key : refsMap.keySet()) {
      if (result.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Property key %s cannot be in both properties and secretReferences", key));
      }
    }

    Set<String> secretKeySet = new LinkedHashSet<>();

    for (Map.Entry<String, String> binding : bindingsMap.entrySet()) {
      String propertyKey = binding.getKey();
      String providerName = binding.getValue();
      if (!result.containsKey(propertyKey)) {
        throw new IllegalArgumentException(
            String.format("Secret binding key %s is missing from properties", propertyKey));
      }
      String plaintext = result.get(propertyKey);
      if (MASK.equals(plaintext)) {
        throw new IllegalArgumentException(
            String.format("Secret binding key %s has masked placeholder value", propertyKey));
      }
      GravitinoSecretProvider provider = requireProvider(registry, providerName);
      SecretWriteContext context =
          new SecretWriteContext(providerName, entityType, entityId, propertyKey);
      String urn = provider.writeSecret(plaintext, context);
      result.put(propertyKey, urn);
      secretKeySet.add(propertyKey);
    }

    for (Map.Entry<String, SecretReferenceLocator> ref : refsMap.entrySet()) {
      String propertyKey = ref.getKey();
      SecretReferenceLocator locator = ref.getValue();
      rejectRawUrnReference(locator);
      GravitinoSecretProvider provider = requireProvider(registry, locator.provider());
      String urn;
      try {
        urn = provider.buildExternalReferenceUrn(propertyKey, locator);
      } catch (UnsupportedOperationException e) {
        throw new IllegalArgumentException(
            String.format(
                "Secret provider %s does not support external secret references",
                locator.provider()),
            e);
      }
      result.put(propertyKey, urn);
      secretKeySet.add(propertyKey);
    }

    if (!secretKeySet.isEmpty()) {
      result.put(SECRET_KEYS_PROPERTY, SECRET_KEY_JOINER.join(secretKeySet));
    }
    return result;
  }

  /**
   * Resolves secret property values for internal use.
   *
   * @param properties the stored properties map
   * @param registry the secret provider registry
   * @return a copy of the properties map with secret values resolved
   */
  public static Map<String, String> resolveSecrets(
      Map<String, String> properties, SecretProviderRegistry registry) {
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Map.of() : Map.copyOf(properties);
    }

    Map<String, String> resolved = new HashMap<>(properties);
    for (String key : secretKeys(properties)) {
      String value = properties.get(key);
      if (value != null && value.startsWith(URN_PREFIX)) {
        SecretUrn.ParsedUrn parsed = SecretUrn.parse(value);
        GravitinoSecretProvider provider = registry.get(parsed.providerName());
        if (provider == null) {
          throw new IllegalArgumentException(
              String.format("Unknown secret provider: %s", parsed.providerName()));
        }
        resolved.put(key, provider.readSecret(value));
      }
    }
    return resolved;
  }

  /**
   * Applies a write-through secret binding during entity alter.
   *
   * @param props the mutable properties map
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param property the secret property key
   * @param provider the provider name
   * @param plaintext the plaintext secret value
   * @param registry the secret provider registry
   */
  public static void applySetSecretBinding(
      Map<String, String> props,
      String entityType,
      long entityId,
      String property,
      String provider,
      String plaintext,
      SecretProviderRegistry registry) {
    if (MASK.equals(plaintext)) {
      throw new IllegalArgumentException(
          String.format("Secret binding key %s has masked placeholder value", property));
    }

    String existingUrn = props.get(property);
    if (existingUrn != null && existingUrn.startsWith(URN_PREFIX)) {
      if (SecretUrn.isWriteThroughForEntity(existingUrn, entityType, entityId)) {
        SecretUrn.ParsedUrn parsed = SecretUrn.parse(existingUrn);
        GravitinoSecretProvider existingProvider = registry.get(parsed.providerName());
        if (existingProvider != null) {
          existingProvider.deleteSecret(existingUrn);
        }
      }
    }

    GravitinoSecretProvider secretProvider = requireProvider(registry, provider);
    SecretWriteContext context = new SecretWriteContext(provider, entityType, entityId, property);
    String urn = secretProvider.writeSecret(plaintext, context);
    props.put(property, urn);
    updateSecretKeys(props, property, true);
  }

  /**
   * Applies an external secret reference during entity alter.
   *
   * @param props the mutable properties map
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param property the secret property key
   * @param locator the external secret reference locator
   * @param registry the secret provider registry
   */
  public static void applySetSecretReference(
      Map<String, String> props,
      String entityType,
      long entityId,
      String property,
      SecretReferenceLocator locator,
      SecretProviderRegistry registry) {
    rejectRawUrnReference(locator);
    GravitinoSecretProvider secretProvider = requireProvider(registry, locator.provider());
    try {
      secretProvider.buildExternalReferenceUrn(property, locator);
    } catch (UnsupportedOperationException e) {
      throw new IllegalArgumentException(
          String.format(
              "Secret provider %s does not support external secret references", locator.provider()),
          e);
    }
  }

  /**
   * Removes a property during entity alter, cleaning up owned write-through secrets when needed.
   *
   * @param props the mutable properties map
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param property the property key to remove
   * @param registry the secret provider registry
   */
  public static void applyRemoveProperty(
      Map<String, String> props,
      String entityType,
      long entityId,
      String property,
      SecretProviderRegistry registry) {
    if (SECRET_KEYS_PROPERTY.equals(property)) {
      throw new IllegalArgumentException("Client must not manage gravitino.secret.keys");
    }

    if (secretKeys(props).contains(property)) {
      String urn = props.get(property);
      if (urn != null
          && urn.startsWith(URN_PREFIX)
          && SecretUrn.isWriteThroughForEntity(urn, entityType, entityId)) {
        SecretUrn.ParsedUrn parsed = SecretUrn.parse(urn);
        GravitinoSecretProvider secretProvider = registry.get(parsed.providerName());
        if (secretProvider != null) {
          secretProvider.deleteSecret(urn);
        }
      }
      updateSecretKeys(props, property, false);
    }
    props.remove(property);
  }

  /**
   * Validates a plain property set during entity alter.
   *
   * @param props the current properties map
   * @param property the property key
   * @param value the property value
   */
  public static void validatePlainSetProperty(
      Map<String, String> props, String property, String value) {
    if (SECRET_KEYS_PROPERTY.equals(property)) {
      throw new IllegalArgumentException("Client must not manage gravitino.secret.keys");
    }
    if (secretKeys(props).contains(property)) {
      throw new IllegalArgumentException(
          String.format("Property %s is a secret key; use setSecretBinding instead", property));
    }
  }

  /**
   * Returns a copy of the properties map with secret keys removed.
   *
   * @param properties the stored properties map
   * @return the properties map without secret values
   */
  public static Map<String, String> omitSecrets(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Map.of() : Map.copyOf(properties);
    }

    Map<String, String> result = new HashMap<>(properties);
    for (String key : secretKeys(properties)) {
      result.remove(key);
    }
    result.remove(SECRET_KEYS_PROPERTY);
    return result;
  }

  /**
   * Deletes write-through secrets owned by the entity.
   *
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param properties the stored properties map
   * @param registry the secret provider registry
   */
  public static void cleanupOnDrop(
      String entityType,
      long entityId,
      @Nullable Map<String, String> properties,
      SecretProviderRegistry registry) {
    if (properties == null || properties.isEmpty()) {
      return;
    }

    for (String key : secretKeys(properties)) {
      String urn = properties.get(key);
      if (urn == null || !urn.startsWith(URN_PREFIX)) {
        continue;
      }
      if (!SecretUrn.isWriteThroughForEntity(urn, entityType, entityId)) {
        continue;
      }
      SecretUrn.ParsedUrn parsed = SecretUrn.parse(urn);
      GravitinoSecretProvider provider = registry.get(parsed.providerName());
      if (provider != null) {
        provider.deleteSecret(urn);
      }
    }
  }

  /**
   * Parses {@link SecretConstants#SECRET_KEYS_PROPERTY} into a set of property keys.
   *
   * @param properties the stored properties map
   * @return the secret property keys
   */
  public static Set<String> secretKeys(@Nullable Map<String, String> properties) {
    if (properties == null) {
      return Set.of();
    }
    String keysValue = properties.get(SECRET_KEYS_PROPERTY);
    if (keysValue == null || keysValue.isBlank()) {
      return Set.of();
    }
    return ImmutableSet.copyOf(SECRET_KEY_SPLITTER.splitToList(keysValue));
  }

  private static GravitinoSecretProvider requireProvider(
      SecretProviderRegistry registry, String providerName) {
    GravitinoSecretProvider provider = registry.get(providerName);
    if (provider == null) {
      throw new IllegalArgumentException(
          String.format("Unknown secret provider: %s", providerName));
    }
    return provider;
  }

  private static void rejectRawUrnReference(SecretReferenceLocator locator) {
    for (String value : locator.attributes().values()) {
      if (value != null && value.startsWith(URN_PREFIX)) {
        throw new IllegalArgumentException(
            "secretReferences must use locator objects, not raw secret URNs");
      }
    }
  }

  private static void updateSecretKeys(Map<String, String> props, String propertyKey, boolean add) {
    LinkedHashSet<String> keys = new LinkedHashSet<>(secretKeys(props));
    if (add) {
      keys.add(propertyKey);
    } else {
      keys.remove(propertyKey);
    }
    if (keys.isEmpty()) {
      props.remove(SECRET_KEYS_PROPERTY);
    } else {
      props.put(SECRET_KEYS_PROPERTY, SECRET_KEY_JOINER.join(keys));
    }
  }
}
