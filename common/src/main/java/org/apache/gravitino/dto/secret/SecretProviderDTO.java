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

package org.apache.gravitino.dto.secret;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/** Data transfer object for a configured secret provider. */
public class SecretProviderDTO {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("type")
  private final String type;

  /**
   * Creates a secret provider DTO.
   *
   * @param name the configured provider name
   * @param type the provider type identifier
   */
  public SecretProviderDTO(@JsonProperty("name") String name, @JsonProperty("type") String type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Returns the configured provider name.
   *
   * @return the provider name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the provider type identifier.
   *
   * @return the provider type
   */
  public String type() {
    return type;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SecretProviderDTO)) {
      return false;
    }
    SecretProviderDTO that = (SecretProviderDTO) other;
    return Objects.equal(name, that.name) && Objects.equal(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, type);
  }
}
