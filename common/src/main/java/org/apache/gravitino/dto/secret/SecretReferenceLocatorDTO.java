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
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/** Data transfer object for an external secret reference locator. */
public class SecretReferenceLocatorDTO {

  @JsonProperty("provider")
  private final String provider;

  @JsonProperty("attributes")
  private final Map<String, String> attributes;

  /**
   * Creates a secret reference locator DTO.
   *
   * @param provider the configured secret provider name
   * @param attributes optional provider-specific locator attributes; never null after construction
   */
  public SecretReferenceLocatorDTO(
      @JsonProperty("provider") String provider,
      @JsonProperty("attributes") @Nullable Map<String, String> attributes) {
    this.provider = provider;
    if (attributes == null || attributes.isEmpty()) {
      this.attributes = Collections.emptyMap();
    } else {
      this.attributes = ImmutableMap.copyOf(attributes);
    }
  }

  /**
   * Returns the configured secret provider name.
   *
   * @return the provider name
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns the provider-specific locator attributes.
   *
   * @return an unmodifiable map of attributes, never null
   */
  public Map<String, String> attributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SecretReferenceLocatorDTO)) {
      return false;
    }
    SecretReferenceLocatorDTO that = (SecretReferenceLocatorDTO) other;
    return Objects.equal(provider, that.provider) && Objects.equal(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(provider, attributes);
  }
}
