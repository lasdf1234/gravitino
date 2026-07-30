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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/** Create-time secret parameters supplied by the API layer. */
public final class SecretCreateParams {

  private final String entityType;
  private final Map<String, String> secretBindings;
  private final Map<String, SecretReferenceLocator> secretReferences;

  /**
   * Creates secret create parameters.
   *
   * @param entityType the entity type
   * @param secretBindings property keys mapped to provider names for write-through secrets
   * @param secretReferences property keys mapped to external secret locators
   */
  public SecretCreateParams(
      String entityType,
      @Nullable Map<String, String> secretBindings,
      @Nullable Map<String, SecretReferenceLocator> secretReferences) {
    this.entityType = entityType;
    this.secretBindings =
        secretBindings == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(secretBindings);
    this.secretReferences =
        secretReferences == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(secretReferences);
  }

  /**
   * Returns the entity type.
   *
   * @return the entity type
   */
  public String entityType() {
    return entityType;
  }

  /**
   * Returns write-through secret bindings.
   *
   * @return property key to provider name mappings
   */
  public Map<String, String> secretBindings() {
    return secretBindings;
  }

  /**
   * Returns external secret references.
   *
   * @return property key to locator mappings
   */
  public Map<String, SecretReferenceLocator> secretReferences() {
    return secretReferences;
  }
}
