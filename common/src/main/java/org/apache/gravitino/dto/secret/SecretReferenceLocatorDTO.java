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
import javax.annotation.Nullable;

/** Data transfer object for an external secret reference locator. */
public class SecretReferenceLocatorDTO {

  @JsonProperty("provider")
  private final String provider;

  @Nullable
  @JsonProperty("mount")
  private final String mount;

  @Nullable
  @JsonProperty("path")
  private final String path;

  /**
   * Creates a secret reference locator DTO.
   *
   * @param provider the configured secret provider name
   * @param mount optional mount or namespace within the external store
   * @param path optional path to the secret within the external store
   */
  public SecretReferenceLocatorDTO(
      @JsonProperty("provider") String provider,
      @JsonProperty("mount") @Nullable String mount,
      @JsonProperty("path") @Nullable String path) {
    this.provider = provider;
    this.mount = mount;
    this.path = path;
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
   * Returns the optional mount or namespace.
   *
   * @return the mount, or {@code null}
   */
  @Nullable
  public String mount() {
    return mount;
  }

  /**
   * Returns the optional secret path.
   *
   * @return the path, or {@code null}
   */
  @Nullable
  public String path() {
    return path;
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
    return Objects.equal(provider, that.provider)
        && Objects.equal(mount, that.mount)
        && Objects.equal(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(provider, mount, path);
  }
}
