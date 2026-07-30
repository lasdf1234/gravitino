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

package org.apache.gravitino.dto.requests;

import java.util.Map;
import javax.annotation.Nullable;

/** Validation helpers for entity create requests with secret fields. */
final class SecretCreateRequestValidation {

  /** Reserved property key listing comma-separated secret property names. */
  static final String SECRET_KEYS_PROPERTY = "gravitino.secret.keys";

  private SecretCreateRequestValidation() {}

  static void validateSecretCreateFields(
      @Nullable Map<String, String> properties,
      @Nullable Map<String, String> secretBindings,
      @Nullable Map<String, ?> secretReferences) {
    if (properties != null && properties.containsKey(SECRET_KEYS_PROPERTY)) {
      throw new IllegalArgumentException("Client must not send gravitino.secret.keys");
    }
    if (secretBindings != null && secretReferences != null) {
      for (String key : secretBindings.keySet()) {
        if (secretReferences.containsKey(key)) {
          throw new IllegalArgumentException(
              String.format(
                  "Property key %s cannot be in both secretBindings and secretReferences", key));
        }
      }
    }
  }
}
