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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.GravitinoEnv;

/** Bridges {@link SecretCreateContext} to entity managers. */
public final class SecretCreateSupport {

  private SecretCreateSupport() {}

  /**
   * Applies create-time secrets when {@link SecretCreateContext} is set for the entity type.
   *
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param properties the incoming properties map
   * @return the updated properties map
   */
  public static Map<String, String> applyOnCreateIfContextSet(
      String entityType, long entityId, @Nullable Map<String, String> properties) {
    SecretCreateParams params = SecretCreateContext.get();
    if (params == null || !entityType.equals(params.entityType())) {
      return properties;
    }
    try {
      SecretProviderRegistry registry = registryOrNull();
      if (registry == null) {
        throw new IllegalStateException(
            "Secret bindings/references were provided but secret provider registry is not"
                + " initialized");
      }
      return SecretPropertyHelper.applyOnCreate(
          entityType,
          entityId,
          properties,
          params.secretBindings(),
          params.secretReferences(),
          registry);
    } finally {
      SecretCreateContext.clear();
    }
  }

  /**
   * Deletes write-through secrets owned by the entity when a registry is configured.
   *
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param properties the stored properties map
   */
  public static void cleanupOnDropIfRegistryPresent(
      String entityType, long entityId, @Nullable Map<String, String> properties) {
    SecretProviderRegistry registry = registryOrNull();
    if (registry != null) {
      SecretPropertyHelper.cleanupOnDrop(entityType, entityId, properties, registry);
    }
  }

  /**
   * Resolves secret property values for internal use when a registry is configured.
   *
   * @param properties the stored properties map
   * @return the properties map with secrets resolved when possible
   */
  public static Map<String, String> resolveSecretsIfRegistryPresent(
      Map<String, String> properties) {
    SecretProviderRegistry registry = registryOrNull();
    if (registry == null) {
      return properties;
    }
    return SecretPropertyHelper.resolveSecrets(properties, registry);
  }

  @Nullable
  private static SecretProviderRegistry registryOrNull() {
    return GravitinoEnv.getInstance().secretProviderRegistry();
  }
}
