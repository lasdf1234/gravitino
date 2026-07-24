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

import javax.annotation.Nullable;

/**
 * Holds per-request secret create parameters in a {@link ThreadLocal} so that entity managers can
 * access them without threading servlet-layer types through the core stack.
 */
public final class SecretCreateContext {

  private static final ThreadLocal<SecretCreateParams> PARAMS = new ThreadLocal<>();

  private SecretCreateContext() {}

  /**
   * Sets secret create parameters for the current request thread.
   *
   * @param params the secret create parameters
   */
  public static void set(SecretCreateParams params) {
    PARAMS.set(params);
  }

  /**
   * Returns secret create parameters for the current request thread.
   *
   * @return the secret create parameters, or {@code null} if unset
   */
  @Nullable
  public static SecretCreateParams get() {
    return PARAMS.get();
  }

  /** Clears secret create parameters for the current request thread. */
  public static void clear() {
    PARAMS.remove();
  }
}
