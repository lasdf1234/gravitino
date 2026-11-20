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

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The process cache follows committed rows. These tests open a real SQL session around a mocked
 * backend so the session refcount, not a stubbed {@code isInTransaction}, decides cache behavior.
 */
public class TestEntityCacheCommitVisibility {

  private static final String STORE_PATH =
      "/tmp/gravitino_test_cache_commit_" + UUID.randomUUID().toString().replace("-", "");

  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake", "catalog", "schema", "topic");

  private RelationalEntityStore store;
  private RelationalBackend backend;
  private CaffeineEntityCache cache;

  @BeforeEach
  void setUp() throws IllegalAccessException {
    Config jdbc = Mockito.mock(Config.class);
    Mockito.when(jdbc.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s/testdb;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(20);
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS)).thenReturn(5);
    Mockito.when(jdbc.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    SqlSessionFactoryHelper.getInstance().init(jdbc);

    store = new RelationalEntityStore();
    backend = Mockito.mock(RelationalBackend.class);
    cache = new CaffeineEntityCache(new Config(false) {});
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", cache, true);
  }

  @AfterEach
  void tearDown() {
    if (SqlSessions.hasActiveSession()) {
      SqlSessions.rollbackAndCloseSqlSession();
    }
    SqlSessionFactoryHelper.getInstance().close();
  }

  @AfterAll
  static void deleteStore() throws IOException {
    File directory = new File(STORE_PATH);
    if (directory.exists()) {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  void committedInsertIsCached() throws Exception {
    TopicEntity created = topic();
    store.put(created, false);
    Assertions.assertSame(created, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.never()).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void uncommittedInsertIsNotPublishedAndDisappearsOnRollback() throws Exception {
    TopicEntity created = topic();
    Mockito.when(backend.get(IDENT, Entity.EntityType.TOPIC))
        .thenThrow(new NoSuchEntityException("missing %s", IDENT));
    SessionUtils.beginTransaction();
    try {
      store.put(created, false);
      Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
      AtomicReference<Throwable> peerFailure = new AtomicReference<>();
      Thread peer =
          new Thread(
              () -> {
                try {
                  store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class);
                  peerFailure.set(new AssertionError("peer observed an uncommitted topic"));
                } catch (NoSuchEntityException expected) {
                  // The other connection still sees the committed state: no row.
                } catch (Throwable failure) {
                  peerFailure.set(failure);
                }
              });
      peer.start();
      peer.join(10_000);
      Assertions.assertFalse(peer.isAlive());
      Assertions.assertNull(peerFailure.get());
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
  }

  @Test
  void sameThreadReadsItsUncommittedUpdateWhilePeersKeepTheCommittedCache() throws Exception {
    TopicEntity committed = topic();
    TopicEntity edited = topic();
    cache.put(committed);
    Mockito.doReturn(edited).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    Mockito.when(backend.get(IDENT, Entity.EntityType.TOPIC)).thenReturn(edited);

    CountDownLatch inTransaction = new CountDownLatch(1);
    CountDownLatch peerObserved = new CountDownLatch(1);
    AtomicReference<TopicEntity> peerSaw = new AtomicReference<>();
    AtomicReference<Throwable> peerFailure = new AtomicReference<>();
    Thread peer =
        new Thread(
            () -> {
              try {
                if (!inTransaction.await(10, TimeUnit.SECONDS)) {
                  peerFailure.set(new AssertionError("timed out waiting for the open transaction"));
                  return;
                }
                peerSaw.set(store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
              } catch (Throwable failure) {
                peerFailure.set(failure);
              } finally {
                peerObserved.countDown();
              }
            });
    peer.start();

    SessionUtils.beginTransaction();
    try {
      store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> edited);
      Assertions.assertSame(edited, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertSame(committed, cache.getIfPresent(IDENT, Entity.EntityType.TOPIC).get());
      inTransaction.countDown();
      Assertions.assertTrue(peerObserved.await(10, TimeUnit.SECONDS));
      Assertions.assertNull(peerFailure.get());
      Assertions.assertSame(committed, peerSaw.get());
      SessionUtils.commitTransaction();
    } catch (Throwable failure) {
      if (SqlSessions.hasActiveSession()) {
        SessionUtils.rollbackTransaction();
      }
      throw failure;
    }
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
    Assertions.assertSame(edited, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.times(2)).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void rollbackDropsDeferredInvalidation() throws Exception {
    TopicEntity committed = topic();
    cache.put(committed);
    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    SessionUtils.commitTransaction();
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
    SessionUtils.rollbackTransaction();
    Assertions.assertSame(committed, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.never()).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void outerCommitInvalidatesAfterNestedSessionCloses() throws Exception {
    cache.put(topic());
    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    SessionUtils.commitTransaction();
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
    SessionUtils.commitTransaction();
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void plainCloseDropsDeferredInvalidation() throws Exception {
    TopicEntity committed = topic();
    cache.put(committed);
    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    SqlSessions.closeSqlSession();
    Assertions.assertSame(committed, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.never()).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void existsInsideATransactionDoesNotTrustTheCache() throws Exception {
    cache.put(topic());
    Mockito.when(backend.exists(IDENT, Entity.EntityType.TOPIC)).thenReturn(false);
    SessionUtils.beginTransaction();
    try {
      Assertions.assertFalse(store.exists(IDENT, Entity.EntityType.TOPIC));
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertTrue(store.exists(IDENT, Entity.EntityType.TOPIC));
    Mockito.verify(backend, Mockito.times(1)).exists(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void batchGetInsideATransactionDoesNotPublish() throws Exception {
    TopicEntity created = topic();
    Mockito.doReturn(List.of(created)).when(backend).batchGet(any(), eq(Entity.EntityType.TOPIC));
    SessionUtils.beginTransaction();
    try {
      Assertions.assertSame(
          created,
          store.batchGet(List.of(IDENT), Entity.EntityType.TOPIC, TopicEntity.class).get(0));
      Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void overwriteInsideATransactionDoesNotReplaceTheCommittedEntry() throws Exception {
    TopicEntity committed = topic();
    cache.put(committed);
    SessionUtils.beginTransaction();
    try {
      store.put(topic(), true);
      Assertions.assertSame(
          committed, cache.getIfPresent(IDENT, Entity.EntityType.TOPIC).orElseThrow());
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertSame(committed, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.never()).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void relationWriteInsideATransactionDefersInvalidation() throws Exception {
    cache.put(topic());
    SessionUtils.beginTransaction();
    store.insertRelation(
        SupportsRelationOperations.Type.OWNER_REL,
        IDENT,
        Entity.EntityType.TOPIC,
        NameIdentifier.of("metalake"),
        Entity.EntityType.METALAKE,
        false);
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
    SessionUtils.commitTransaction();
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void failedUpdateThenCommitDropsTheOldEntry() throws Exception {
    cache.put(topic());
    Mockito.doThrow(new RuntimeException("update failed"))
        .when(backend)
        .update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic()));
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
    SessionUtils.commitTransaction();
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void failedUpdateThenRollbackKeepsTheOldEntry() throws Exception {
    TopicEntity committed = topic();
    cache.put(committed);
    Mockito.doThrow(new RuntimeException("update failed"))
        .when(backend)
        .update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic()));
    SessionUtils.rollbackTransaction();
    Assertions.assertSame(committed, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(backend, Mockito.never()).get(IDENT, Entity.EntityType.TOPIC);
  }

  @Test
  void keyIsGoneBeforeJdbcCommit() throws Exception {
    TopicEntity committed = topic();
    cache.put(committed);
    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    Mockito.when(backend.get(IDENT, Entity.EntityType.TOPIC)).thenReturn(committed);
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    @SuppressWarnings("unchecked")
    ThreadLocal<SqlSession> sessions =
        (ThreadLocal<SqlSession>) FieldUtils.readStaticField(SqlSessions.class, "sessions", true);
    SqlSession spy = Mockito.spy(sessions.get());
    Mockito.doAnswer(
            invocation -> {
              Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
              // This thread still has the session, so the read must not refill the cache.
              store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class);
              Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
              return invocation.callRealMethod();
            })
        .when(spy)
        .commit();
    sessions.set(spy);
    SessionUtils.commitTransaction();
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void preCommitReadIsNotPublishedAfterCommit() throws Exception {
    TopicEntity committed = topic();
    CountDownLatch readEntered = new CountDownLatch(1);
    CountDownLatch finishRead = new CountDownLatch(1);
    AtomicReference<Throwable> peerFailure = new AtomicReference<>();
    Mockito.when(backend.get(IDENT, Entity.EntityType.TOPIC))
        .thenAnswer(
            invocation -> {
              readEntered.countDown();
              if (!finishRead.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for the commit");
              }
              return committed;
            });
    Thread peer =
        new Thread(
            () -> {
              try {
                store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class);
              } catch (Throwable failure) {
                peerFailure.set(failure);
              }
            });
    peer.start();
    Assertions.assertTrue(readEntered.await(10, TimeUnit.SECONDS));

    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    SessionUtils.commitTransaction();
    finishRead.countDown();
    peer.join(10_000);
    Assertions.assertFalse(peer.isAlive());
    Assertions.assertNull(peerFailure.get());
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void failedJdbcCommitLeavesTheCacheEmpty() throws Exception {
    cache.put(topic());
    Mockito.doReturn(topic()).when(backend).update(eq(IDENT), eq(Entity.EntityType.TOPIC), any());
    SessionUtils.beginTransaction();
    store.update(IDENT, TopicEntity.class, Entity.EntityType.TOPIC, entity -> topic());
    @SuppressWarnings("unchecked")
    ThreadLocal<SqlSession> sessions =
        (ThreadLocal<SqlSession>) FieldUtils.readStaticField(SqlSessions.class, "sessions", true);
    SqlSession spy = Mockito.spy(sessions.get());
    Mockito.doThrow(new IllegalStateException("commit failed")).when(spy).commit();
    sessions.set(spy);
    Assertions.assertThrows(IllegalStateException.class, SessionUtils::commitTransaction);
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
    Assertions.assertFalse(SqlSessions.hasActiveSession());

    TopicEntity reloaded = topic();
    Mockito.when(backend.get(IDENT, Entity.EntityType.TOPIC)).thenReturn(reloaded);
    Assertions.assertSame(reloaded, store.get(IDENT, Entity.EntityType.TOPIC, TopicEntity.class));
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  @Test
  void deleteInsideATransactionKeepsTheCommittedEntryUntilCommit() throws Exception {
    cache.put(topic());
    Mockito.when(backend.delete(IDENT, Entity.EntityType.TOPIC, true)).thenReturn(true);
    SessionUtils.beginTransaction();
    Assertions.assertTrue(store.delete(IDENT, Entity.EntityType.TOPIC, true));
    Assertions.assertTrue(cache.contains(IDENT, Entity.EntityType.TOPIC));
    SessionUtils.commitTransaction();
    Assertions.assertFalse(cache.contains(IDENT, Entity.EntityType.TOPIC));
  }

  private static TopicEntity topic() {
    TopicEntity topic = Mockito.mock(TopicEntity.class);
    Mockito.when(topic.nameIdentifier()).thenReturn(IDENT);
    Mockito.when(topic.type()).thenReturn(Entity.EntityType.TOPIC);
    return topic;
  }
}
