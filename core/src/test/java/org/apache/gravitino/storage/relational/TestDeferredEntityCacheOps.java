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

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDeferredEntityCacheOps {

  @AfterEach
  void tearDown() {
    DeferredEntityCacheOps.clearForTesting();
  }

  @Test
  void testRunImmediatelyWhenNotDeferred() {
    AtomicInteger count = new AtomicInteger();
    DeferredEntityCacheOps.runOrDefer(false, count::incrementAndGet);
    Assertions.assertEquals(1, count.get());
    Assertions.assertEquals(0, DeferredEntityCacheOps.pendingCountForTesting());
  }

  @Test
  void testFlushRunsDeferredActions() {
    AtomicInteger count = new AtomicInteger();
    DeferredEntityCacheOps.runOrDefer(true, count::incrementAndGet);
    DeferredEntityCacheOps.runOrDefer(true, count::incrementAndGet);
    Assertions.assertEquals(0, count.get());
    Assertions.assertEquals(2, DeferredEntityCacheOps.pendingCountForTesting());
    DeferredEntityCacheOps.flush();
    Assertions.assertEquals(2, count.get());
    Assertions.assertEquals(0, DeferredEntityCacheOps.pendingCountForTesting());
  }

  @Test
  void testDiscardDropsDeferredActions() {
    AtomicInteger count = new AtomicInteger();
    DeferredEntityCacheOps.runOrDefer(true, count::incrementAndGet);
    DeferredEntityCacheOps.discard();
    Assertions.assertEquals(0, count.get());
    Assertions.assertEquals(0, DeferredEntityCacheOps.pendingCountForTesting());
  }
}
