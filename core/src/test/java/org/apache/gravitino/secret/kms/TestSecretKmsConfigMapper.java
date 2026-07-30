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
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretKmsConfigMapper {

  @Test
  public void testMapsProviderConfigToSingleKmsSource() {
    Map<String, String> mapped =
        SecretKmsConfigMapper.toKmsConfig(
            "aws-secret",
            ImmutableMap.of(
                "className",
                KmsSecretsProvider.class.getName(),
                "api",
                "aws-kms",
                "keyId",
                "alias/gravitino",
                "endpoint.region",
                "us-west-2"));

    Assertions.assertEquals(
        ImmutableMap.of(
            "gravitino.kms.sources",
            "aws-secret",
            "gravitino.kms.source.aws-secret.api",
            "aws-kms",
            "gravitino.kms.source.aws-secret.endpoint.region",
            "us-west-2"),
        mapped);
  }

  @Test
  public void testRequiresApiAndKeyId() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretKmsConfigMapper.toKmsConfig("aws-secret", ImmutableMap.of("keyId", "k")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretKmsConfigMapper.requireKeyId(ImmutableMap.of("api", "aws-kms")));
  }
}
