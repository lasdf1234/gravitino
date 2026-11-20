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

package org.apache.gravitino.storage.relational;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

/**
 * Queues entity-cache invalidations that must wait for the current thread's outermost SQL session
 * to commit.
 *
 * <p>{@link org.apache.gravitino.storage.relational.utils.SessionUtils} flushes on a successful
 * outer commit and discards on outer rollback.
 */
public final class DeferredEntityCacheOps {

  private static final ThreadLocal<List<Runnable>> PENDING =
      ThreadLocal.withInitial(ArrayList::new);

  private DeferredEntityCacheOps() {}

  /**
   * Runs {@code action} now, or queues it until the outer SQL session commits when {@code defer} is
   * true.
   *
   * @param defer whether to defer until outer commit
   * @param action the cache side effect
   */
  public static void runOrDefer(boolean defer, Runnable action) {
    if (!defer) {
      action.run();
      return;
    }
    PENDING.get().add(action);
  }

  /** Runs and clears all deferred actions for this thread (outer commit). */
  public static void flush() {
    List<Runnable> ops = PENDING.get();
    PENDING.remove();
    if (ops == null || ops.isEmpty()) {
      return;
    }
    for (Runnable op : ops) {
      op.run();
    }
  }

  /** Drops deferred actions without running them (outer rollback). */
  public static void discard() {
    PENDING.remove();
  }

  @VisibleForTesting
  static int pendingCountForTesting() {
    List<Runnable> ops = PENDING.get();
    return ops == null ? 0 : ops.size();
  }

  @VisibleForTesting
  static void clearForTesting() {
    PENDING.remove();
  }
}
