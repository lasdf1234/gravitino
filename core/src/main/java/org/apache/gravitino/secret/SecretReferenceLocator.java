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
import java.util.Objects;
import javax.annotation.Nullable;

/** Locator for an externally managed secret referenced at entity create time. */
public final class SecretReferenceLocator {

  private final String provider;
  private final Map<String, String> attributes;

  /**
   * Creates a secret reference locator.
   *
   * @param provider the configured secret provider name
   * @param attributes optional provider-specific locator attributes; never null after construction
   */
  public SecretReferenceLocator(String provider, @Nullable Map<String, String> attributes) {
    this.provider = provider;
    if (attributes == null || attributes.isEmpty()) {
      this.attributes = Map.of();
    } else {
      this.attributes = Map.copyOf(attributes);
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
    if (!(other instanceof SecretReferenceLocator)) {
      return false;
    }
    SecretReferenceLocator that = (SecretReferenceLocator) other;
    return Objects.equals(provider, that.provider) && Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, attributes);
  }
}
