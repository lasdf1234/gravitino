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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

/**
 * Covers the transaction-close cache clear that a pre-commit invalidation does not replace. One
 * case pauses a second connection after it has read the old row. The other overlaps two commits on
 * the global cache lock and checks both the caller's result and the committed row.
 */
public class TestEntityCacheCloseVisibility extends TestJDBCBackend {

  @TestTemplate
  void testCommittedDeleteStaysInvisibleToReaderPausedAfterFetch() throws Exception {
    NameIdentifier ident = NameIdentifier.of("close_visible_lake");
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), ident.name(), AUDIT_INFO);
    backend.insert(metalake, false);

    CaffeineEntityCache cache = new CaffeineEntityCache(new Config(false) {});
    RelationalBackend spied = Mockito.spy(backend);
    RelationalEntityStore store = newStore(spied, cache);
    CountDownLatch fetched = new CountDownLatch(1);
    CountDownLatch releaseReader = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              Object entity = invocation.callRealMethod();
              fetched.countDown();
              Assertions.assertTrue(releaseReader.await(15, TimeUnit.SECONDS));
              return entity;
            })
        .when(spied)
        .get(ident, Entity.EntityType.METALAKE);

    CountDownLatch deleted = new CountDownLatch(1);
    CountDownLatch commitDelete = new CountDownLatch(1);
    AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              SessionUtils.beginTransaction();
              boolean committed = false;
              try {
                Assertions.assertTrue(store.delete(ident, Entity.EntityType.METALAKE, true));
                deleted.countDown();
                Assertions.assertTrue(commitDelete.await(15, TimeUnit.SECONDS));
                SessionUtils.commitTransaction();
                committed = true;
              } catch (Throwable thrown) {
                writerFailure.set(thrown);
              } finally {
                deleted.countDown();
                if (!committed) {
                  SessionUtils.rollbackTransaction();
                }
              }
            });
    AtomicReference<Throwable> readerFailure = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              try {
                store.get(ident, Entity.EntityType.METALAKE, BaseMetalake.class);
              } catch (Throwable thrown) {
                readerFailure.set(thrown);
              }
            });
    writer.start();
    try {
      Assertions.assertTrue(deleted.await(15, TimeUnit.SECONDS));
      Assertions.assertNull(writerFailure.get());
      reader.start();
      Assertions.assertTrue(fetched.await(15, TimeUnit.SECONDS));
      commitDelete.countDown();
      writer.join(15_000);
      Assertions.assertFalse(writer.isAlive());
      Assertions.assertNull(writerFailure.get());
      Assertions.assertFalse(backend.exists(ident, Entity.EntityType.METALAKE));
    } finally {
      commitDelete.countDown();
      releaseReader.countDown();
      writer.join(15_000);
      if (reader.isAlive() || fetched.getCount() == 0) {
        reader.join(15_000);
      }
    }
    Assertions.assertNull(readerFailure.get());
    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.METALAKE));
    Assertions.assertFalse(store.exists(ident, Entity.EntityType.METALAKE));
  }

  @TestTemplate
  void testOverlappingCommitReturnsAndPersistsTheRow() throws Exception {
    NameIdentifier ident = NameIdentifier.of("overlap_commit_lake");
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), ident.name(), AUDIT_INFO);
    CountDownLatch enteredClear = new CountDownLatch(1);
    CaffeineEntityCache cache =
        new CaffeineEntityCache(new Config(false) {}) {
          @Override
          public void clear() {
            enteredClear.countDown();
            super.clear();
          }
        };
    RelationalEntityStore store = newStore(backend, cache);
    CountDownLatch inserted = new CountDownLatch(1);
    CountDownLatch startCommit = new CountDownLatch(1);
    CountDownLatch holding = new CountDownLatch(1);
    CountDownLatch releaseLock = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread locker =
        new Thread(
            () -> {
              try {
                cache.withCacheLock(
                    EntityCacheKey.of(NameIdentifier.of("lock-target"), Entity.EntityType.CATALOG),
                    () -> {
                      holding.countDown();
                      if (!releaseLock.await(15, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to release the lock");
                      }
                    });
              } catch (Exception exception) {
                failure.compareAndSet(null, exception);
              }
            });
    Thread writer =
        new Thread(
            () -> {
              SessionUtils.beginTransaction();
              boolean committed = false;
              try {
                // Insert before the other thread takes a segment lock. put() invalidates under
                // that lock, so holding it first can stop the writer from ever reaching clear().
                store.put(metalake, false);
                inserted.countDown();
                Assertions.assertTrue(startCommit.await(15, TimeUnit.SECONDS));
                SessionUtils.commitTransaction();
                committed = true;
              } catch (Throwable thrown) {
                failure.compareAndSet(null, thrown);
              } finally {
                inserted.countDown();
                if (!committed) {
                  SessionUtils.rollbackTransaction();
                }
              }
            });
    writer.start();
    try {
      Assertions.assertTrue(inserted.await(15, TimeUnit.SECONDS));
      Assertions.assertNull(failure.get());
      locker.start();
      Assertions.assertTrue(holding.await(15, TimeUnit.SECONDS));
      startCommit.countDown();
      Assertions.assertTrue(
          enteredClear.await(15, TimeUnit.SECONDS), () -> String.valueOf(failure.get()));
    } finally {
      startCommit.countDown();
      releaseLock.countDown();
      locker.join(15_000);
      writer.join(15_000);
    }
    Assertions.assertNull(failure.get());
    Assertions.assertFalse(writer.isAlive());
    Assertions.assertFalse(locker.isAlive());
    Assertions.assertTrue(backend.exists(ident, Entity.EntityType.METALAKE));
  }

  private static RelationalEntityStore newStore(
      RelationalBackend storeBackend, CaffeineEntityCache cache) throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", storeBackend, true);
    FieldUtils.writeField(store, "cache", cache, true);
    return store;
  }
}
