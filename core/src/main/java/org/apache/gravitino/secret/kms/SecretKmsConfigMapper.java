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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Maps secret-provider configuration onto a synthetic {@code gravitino.kms.*} configuration so each
 * KMS-backed secret provider can own a dedicated {@link
 * org.apache.gravitino.encryption.kms.KmsClientRegistry} without requiring a separate global KMS
 * source entry.
 */
final class SecretKmsConfigMapper {

  static final String CLASS_NAME_KEY = "className";
  static final String KEY_ID_KEY = "keyId";
  static final String API_KEY = "api";

  private static final String KMS_PREFIX = "gravitino.kms.";
  private static final Set<String> RESERVED_KEYS = Set.of(CLASS_NAME_KEY, KEY_ID_KEY);

  private SecretKmsConfigMapper() {}

  /**
   * Builds a single-source KMS configuration from provider settings.
   *
   * <p>Every provider property except {@code className} and {@code keyId} is copied under {@code
   * gravitino.kms.source.<providerName>.*}. The provider name becomes the sole KMS source name.
   *
   * @param providerName configured secret provider name (also used as KMS source name)
   * @param providerConfig provider configuration map
   * @return synthetic full KMS configuration entries
   */
  static Map<String, String> toKmsConfig(String providerName, Map<String, String> providerConfig) {
    if (providerName == null || providerName.isBlank()) {
      throw new IllegalArgumentException("Secret provider name must not be blank");
    }
    if (providerConfig == null || providerConfig.isEmpty()) {
      throw new IllegalArgumentException(
          "KMS-backed secret provider requires configuration including api and keyId");
    }
    if (!providerConfig.containsKey(API_KEY)
        || providerConfig.get(API_KEY) == null
        || providerConfig.get(API_KEY).isBlank()) {
      throw new IllegalArgumentException(
          "KMS-backed secret provider requires non-blank 'api' configuration");
    }

    Map<String, String> kmsConfig = new LinkedHashMap<>();
    kmsConfig.put(KMS_PREFIX + "sources", providerName);
    for (Map.Entry<String, String> entry : providerConfig.entrySet()) {
      String key = entry.getKey();
      if (RESERVED_KEYS.contains(key)) {
        continue;
      }
      kmsConfig.put(KMS_PREFIX + "source." + providerName + "." + key, entry.getValue());
    }
    return kmsConfig;
  }

  static String requireKeyId(Map<String, String> providerConfig) {
    String keyId = providerConfig == null ? null : providerConfig.get(KEY_ID_KEY);
    if (keyId == null || keyId.isBlank()) {
      throw new IllegalArgumentException(
          "KMS-backed secret provider requires non-blank 'keyId' configuration");
    }
    return keyId;
  }
}
