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

package org.apache.gravitino.server.web;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.secret.SecretReferenceLocatorDTO;
import org.apache.gravitino.secret.SecretCreateContext;
import org.apache.gravitino.secret.SecretCreateParams;
import org.apache.gravitino.secret.SecretReferenceLocator;

/** Helpers for wiring REST create requests to {@link SecretCreateContext}. */
public final class SecretCreateWebSupport {

  private SecretCreateWebSupport() {}

  /**
   * Sets secret create parameters for the current request thread.
   *
   * @param entityType the entity type
   * @param secretBindings property keys mapped to provider names for write-through secrets
   * @param secretReferences property keys mapped to external secret locator DTOs
   */
  public static void setCreateContext(
      String entityType,
      @Nullable Map<String, String> secretBindings,
      @Nullable Map<String, SecretReferenceLocatorDTO> secretReferences) {
    Map<String, SecretReferenceLocator> refs = toReferenceLocators(secretReferences);
    boolean hasBindings = secretBindings != null && !secretBindings.isEmpty();
    if (!hasBindings && refs.isEmpty()) {
      return;
    }
    SecretCreateContext.set(
        new SecretCreateParams(entityType, secretBindings, refs.isEmpty() ? null : refs));
  }

  /** Clears secret create parameters for the current request thread. */
  public static void clearCreateContext() {
    SecretCreateContext.clear();
  }

  private static Map<String, SecretReferenceLocator> toReferenceLocators(
      @Nullable Map<String, SecretReferenceLocatorDTO> secretReferences) {
    if (secretReferences == null || secretReferences.isEmpty()) {
      return Map.of();
    }
    Map<String, SecretReferenceLocator> refs = new HashMap<>();
    for (Map.Entry<String, SecretReferenceLocatorDTO> entry : secretReferences.entrySet()) {
      SecretReferenceLocatorDTO dto = entry.getValue();
      refs.put(entry.getKey(), new SecretReferenceLocator(dto.provider(), dto.attributes()));
    }
    return refs;
  }
}
