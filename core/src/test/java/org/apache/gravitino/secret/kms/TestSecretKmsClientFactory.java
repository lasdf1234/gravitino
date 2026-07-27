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

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;

/**
 * Test-only {@link KmsClientFactory} registered via {@code META-INF/services} for secret/kms unit
 * tests.
 */
public final class TestSecretKmsClientFactory implements KmsClientFactory {

  public static final String API = "test-kms";

  @Override
  public String api() {
    return API;
  }

  @Override
  public KmsClient create(String source, Map<String, String> properties) {
    return new KmsClient() {
      @Override
      public Optional<KmsKeyProperties> getKeyProperties(KmsReference reference) {
        if (reference == null) {
          throw new IllegalArgumentException("KMS reference cannot be null");
        }
        if (!API.equals(reference.api())) {
          throw new IllegalArgumentException("Unexpected KMS API: " + reference.api());
        }
        return Optional.of(
            new KmsKeyProperties() {
              @Override
              public KmsReference reference() {
                return reference;
              }

              @Override
              public boolean enabled() {
                return true;
              }

              @Override
              public boolean supportsWrapping() {
                return true;
              }

              @Override
              public boolean supportsUnwrapping() {
                return true;
              }
            });
      }
    };
  }
}
