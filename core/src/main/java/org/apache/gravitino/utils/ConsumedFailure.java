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
package org.apache.gravitino.utils;

import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Hands a failure that a caller has already consumed to the server health recorder. The caller
 * keeps its own result: recording must not throw, and a successful database commit stays
 * successful.
 */
public final class ConsumedFailure {
  private static volatile Consumer<Throwable> recorder = failure -> {};

  private ConsumedFailure() {}

  /**
   * Installs the recorder used by {@link #record(Throwable)}. The server points this at its health
   * state. A null recorder drops the failure.
   *
   * @param recorder the recorder, or null to drop consumed failures
   */
  public static void setRecorder(@Nullable Consumer<Throwable> recorder) {
    ConsumedFailure.recorder = recorder == null ? failure -> {} : recorder;
  }

  /**
   * Passes {@code failure} to the installed recorder. A recorder that itself throws is ignored.
   *
   * @param failure the consumed failure, or null
   */
  public static void record(@Nullable Throwable failure) {
    try {
      recorder.accept(failure);
    } catch (RuntimeException | Error ignored) {
      // The caller already decided to consume this failure. Recording it must not replace it.
    }
  }
}
