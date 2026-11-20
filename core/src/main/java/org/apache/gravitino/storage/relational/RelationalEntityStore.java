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

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.BaseEntityCache;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.EntityChangeLogMetricsSource;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface. The default JDBC backend is {@link JDBCBackend}.
 *
 * <p>A write that fails because a statement waited on a row lock longer than the backend's lock
 * wait timeout is reported as an {@link OptimisticLockException}, so callers see a conflict rather
 * than an internal error. The store write is rolled back; retrying the whole operation also
 * requires accounting for any preceding external side effects. Writers queue on the same rows
 * inside the database, for example a table drop behind a statistic update of that table, and the
 * backend aborts the waiting statement once the timeout is reached. See {@link
 * ExceptionUtils#isLockWaitTimeout}.
 */
public class RelationalEntityStore
    implements EntityStore,
        SupportsRelationOperations,
        SupportsDomainOperations,
        SupportsEntityChangeLog {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private EntityChangeLogPoller entityChangeLogPoller;
  private EntityChangeLogCleaner entityChangeLogCleaner;
  private EntityCache cache;
  // Created with the store rather than in initialize(), so that a listener built before or without
  // initialization still has somewhere to record. initialize() only registers it for export.
  private final EntityChangeLogMetricsSource changeLogMetrics = new EntityChangeLogMetricsSource();

  // Advanced before every invalidation observed by this store, whether local or replayed from the
  // change log. A shared cache without a local change-log listener needs its own distributed
  // version check: this counter cannot detect changes made on another node.
  private final AtomicLong cacheInvalidationEpoch = new AtomicLong();

  // Held while dropping keys and while the outermost SQL commit runs, and also while a read checks
  // or fills the cache. It is never held across a database read. Changed keys are removed before
  // the JDBC commit returns, and the epoch is advanced again before the lock is released, so a
  // reader cannot observe the old row or put that pre-commit row back.
  private final Object committedCacheLock = new Object();
  private final ThreadLocal<Set<EntityCacheKey>> pendingInvalidations =
      ThreadLocal.withInitial(LinkedHashSet::new);
  private final ThreadLocal<Boolean> commitHookInstalled =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  // Non-null only for a LOCAL_PER_NODE cache, which needs cross-node invalidation. SHARED and NONE
  // caches have no per-node copy to invalidate, so no listener is registered.
  @Nullable private EntityCacheChangeLogListener entityCacheChangeLogListener;

  @VisibleForTesting
  public EntityCache getCache() {
    return cache;
  }

  /**
   * Wraps a relational backend for offline migration without caches or background workers.
   *
   * <p>The caller initializes the SQL session factory and owns transaction boundaries. Closing the
   * returned store also closes the backend. All servers sharing the database must be stopped.
   *
   * @param backend The backend using the caller's SQL session factory.
   * @param config The configuration for the uncached store.
   * @return A store that participates in the current thread's SQL transaction.
   */
  public static RelationalEntityStore forOfflineMigration(
      RelationalBackend backend, Config config) {
    RelationalEntityStore store = new RelationalEntityStore();
    store.backend = backend;
    store.cache = new NoOpsCache(config);
    EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    return store;
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    if (config.get(Configs.CACHE_ENABLED)) {
      this.cache = CacheFactory.getEntityCache(config);
      EntityIdService.initialize(
          new CachedEntityIdResolver(cache, new RelationalEntityStoreIdResolver()));
    } else {
      this.cache = new NoOpsCache(config);
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    }

    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();

    // Polling and cleanup use separate single-threaded schedulers. Polling only dispatches changes
    // to local listeners, while cleanup independently removes records beyond the retention period.
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.register(changeLogMetrics);
    }
    this.entityChangeLogPoller =
        new EntityChangeLogPoller(
            config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS),
            config.get(Configs.ENTITY_CHANGE_LOG_POLL_BATCH_SIZE),
            config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS),
            config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES),
            changeLogMetrics);
    this.entityChangeLogCleaner =
        new EntityChangeLogCleaner(
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_RETENTION_SECS)),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)));

    registerCacheChangeLogListener();

    this.entityChangeLogPoller.start();
    this.entityChangeLogCleaner.start();
  }

  /**
   * The coherence gate: a {@link Coherence#LOCAL_PER_NODE} cache keeps its own copy per node, so
   * changes made on other nodes must be replayed here through the change log. {@link
   * Coherence#SHARED} and {@link Coherence#NONE} caches have nothing per-node to invalidate, so no
   * listener is registered.
   */
  @VisibleForTesting
  void registerCacheChangeLogListener() {
    if (cache.coherence() != Coherence.LOCAL_PER_NODE) {
      return;
    }

    this.entityCacheChangeLogListener = newCacheChangeLogListener();
    this.entityChangeLogPoller.registerListener(entityCacheChangeLogListener);
  }

  /**
   * Creates the change-log listener that keeps this store's cache coherent with changes made on
   * other nodes.
   *
   * @return a listener whose invalidations go through this store, see {@link #batchGet}
   */
  @VisibleForTesting
  EntityCacheChangeLogListener newCacheChangeLogListener() {
    return new EntityCacheChangeLogListener(
        new EntityCacheChangeLogListener.Target() {
          @Override
          public void invalidate(NameIdentifier ident, Entity.EntityType type) {
            invalidateCache(ident, type);
          }

          @Override
          public void clear() {
            clearCache();
          }
        },
        changeLogMetrics);
  }

  private RelationalBackend createRelationalEntityBackend(Config config) {
    String backendName = config.get(ENTITY_RELATIONAL_STORE);
    String className =
        RELATIONAL_BACKENDS.getOrDefault(backendName, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);

    try {
      RelationalBackend relationalBackend =
          (RelationalBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      relationalBackend.initialize(config);

      return relationalBackend;
    } catch (Exception e) {
      LOGGER.error(
          "Failed to create and initialize RelationalBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize RelationalBackend by name: " + backendName, e);
    }
  }

  @Override
  public List<SchemaEntity> listDescendantSchemas(NameIdentifier identifier) throws IOException {
    return backend.listDescendantSchemas(identifier);
  }

  @Override
  public List<NameIdentifier> listSemanticModelMetrics(Namespace namespace) throws IOException {
    return backend.listSemanticModelMetrics(namespace);
  }

  @Override
  public List<SemanticModelMetric> listSemanticModelMetricInfos(Namespace namespace)
      throws IOException {
    return backend.listSemanticModelMetricInfos(namespace);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    return backend.list(namespace, entityType, false);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType, boolean allFields)
      throws IOException {
    return backend.list(namespace, entityType, allFields);
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    if (!useCommittedCache()) {
      return backend.exists(ident, entityType);
    }
    synchronized (committedCacheLock) {
      if (cache.contains(ident, entityType)) {
        return true;
      }
    }
    return backend.exists(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    long epochBeforeWrite;
    synchronized (committedCacheLock) {
      epochBeforeWrite = cacheInvalidationEpoch.get();
    }
    try {
      backend.insert(e, overwritten);
    } catch (RuntimeException re) {
      // The failed insert may already be part of the caller's open transaction. Remember the key
      // so a later commit drops it; a rollback discards the reminder and leaves the cache as it
      // was.
      if (SessionUtils.isInTransaction()) {
        invalidateCache(e.nameIdentifier(), e.type());
      }
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, e.type(), e.nameIdentifier());
    }
    if (overwritten || !useCommittedCache()) {
      // Overwrite: the stored row may not match this copy. Open session: the insert is not
      // committed, so publishing it would leave a ghost after rollback. Both reload after commit.
      invalidateCache(e.nameIdentifier(), e.type());
    } else {
      cacheIfEpochUnchanged(e, epochBeforeWrite);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    E updatedEntity;
    boolean finished = false;
    try {
      updatedEntity = backend.update(ident, entityType, updater);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
    } finally {
      // A failure outside a transaction was already rolled back. A failure inside one may still
      // be committed by the caller, so that key is dropped at the outermost commit.
      invalidateChanged(finished, ident, entityType);
    }
    return updatedEntity;
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    if (!useCommittedCache()) {
      return backend.get(ident, entityType);
    }
    long epochBeforeRead;
    Optional<E> cached;
    synchronized (committedCacheLock) {
      epochBeforeRead = cacheInvalidationEpoch.get();
      cached = cache.getIfPresent(ident, entityType);
    }
    if (cached.isPresent()) {
      return cached.get();
    }
    E loaded = backend.get(ident, entityType);
    cacheIfEpochUnchanged(loaded, epochBeforeRead);
    return loaded;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, Entity.EntityType entityType, Class<E> clazz) {
    if (!useCommittedCache()) {
      return backend.batchGet(idents, entityType);
    }
    long epochBeforeRead;
    List<E> hits = new ArrayList<>();
    List<NameIdentifier> misses = new ArrayList<>();
    synchronized (committedCacheLock) {
      epochBeforeRead = cacheInvalidationEpoch.get();
      for (NameIdentifier ident : idents) {
        Optional<E> cached = cache.getIfPresent(ident, entityType);
        if (cached.isPresent()) {
          hits.add(cached.get());
        } else {
          misses.add(ident);
        }
      }
    }
    if (!misses.isEmpty()) {
      List<E> loaded = backend.batchGet(misses, entityType);
      for (E entity : loaded) {
        cacheIfEpochUnchanged(entity, epochBeforeRead);
        hits.add(entity);
      }
    }
    return hits;
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    try {
      boolean deleted = backend.delete(ident, entityType, cascade);
      return deleted;
    } catch (NoSuchEntityException e) {
      return false;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
    } finally {
      invalidateCache(ident, entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident,
      Entity.EntityType entityType,
      Class<E> clazz,
      Consumer<E> postDeleteAction)
      throws IOException {
    try {
      return backend.deleteAndGet(ident, entityType, clazz, postDeleteAction);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
    } finally {
      invalidateCache(ident, entityType);
    }
  }

  @Override
  public Optional<SchemaEntity> deleteSchemaAndGet(
      NameIdentifier ident, boolean cascadeFunctions, Consumer<SchemaEntity> postDeleteAction)
      throws IOException {
    try {
      return backend.deleteSchemaAndGet(ident, cascadeFunctions, postDeleteAction);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.SCHEMA, ident);
    } finally {
      invalidateCache(ident, Entity.EntityType.SCHEMA);
    }
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public void registerEntityChangeLogListener(EntityChangeLogListener listener) {
    entityChangeLogPoller.registerListener(listener);
  }

  @Override
  public void unregisterEntityChangeLogListener(EntityChangeLogListener listener) {
    entityChangeLogPoller.unregisterListener(listener);
  }

  @Override
  public void close() throws IOException {
    // Keep shutting the remaining components down even if one of them fails, and tolerate a
    // half-finished initialize() that left some of them null.
    IOException failure = null;
    failure = closeComponent(failure, "entity cache", cache == null ? null : cache::clear);
    failure = closeComponent(failure, "entity change log poller", entityChangeLogPoller);
    failure = closeComponent(failure, "entity change log cleaner", entityChangeLogCleaner);
    failure = closeComponent(failure, "relational garbage collector", garbageCollector);
    failure = closeComponent(failure, "relational backend", backend);
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.unregister(changeLogMetrics);
    }

    if (failure != null) {
      throw failure;
    }
  }

  private static IOException closeComponent(
      @Nullable IOException failure, String name, @Nullable AutoCloseable component) {
    if (component == null) {
      return failure;
    }

    try {
      component.close();
      return failure;
    } catch (Exception e) {
      LOGGER.warn("Failed to close {}", name, e);
      if (failure != null) {
        failure.addSuppressed(e);
        return failure;
      }
      return e instanceof IOException
          ? (IOException) e
          : new IOException("Failed to close " + name, e);
    }
  }

  @Override
  public SupportsRelationOperations relationOperations() {
    return this;
  }

  @Override
  public SupportsDomainOperations domainOperations() {
    return this;
  }

  @Override
  public void insertDomainWithBackingTag(
      DomainEntity domain, TagValue backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    try {
      backend.insertDomainWithBackingTag(domain, backingTag, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, Entity.EntityType.DOMAIN, domain.nameIdentifier());
    }
  }

  @Override
  public void insertDomainWithNewBackingTag(
      DomainEntity domain, TagEntity backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    boolean finished = false;
    try {
      backend.insertDomainWithNewBackingTag(domain, backingTag, check);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, Entity.EntityType.DOMAIN, domain.nameIdentifier());
    } finally {
      // Tags are cached; the new tag must not be shadowed by an entry cached for its name.
      invalidateChanged(finished, backingTag.nameIdentifier(), Entity.EntityType.TAG);
    }
  }

  @Override
  public boolean deleteDomain(NameIdentifier ident, Consumer<DomainEntity> check)
      throws IOException {
    try {
      return backend.deleteDomain(ident, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    } finally {
      invalidateCache(ident, Entity.EntityType.DOMAIN);
    }
  }

  @Override
  public DomainEntity updateDomainBackingTagAndGet(
      NameIdentifier ident,
      @Nullable TagValue backingTag,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.updateDomainBackingTagAndGet(ident, backingTag, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity setDomainParentAndGet(
      NameIdentifier ident,
      NameIdentifier parentIdent,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.setDomainParentAndGet(ident, parentIdent, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity removeDomainParentAndGet(
      NameIdentifier ident, Consumer<LockedDomainState> check) throws IOException {
    try {
      return backend.removeDomainParentAndGet(ident, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity updateDomainStatusAndGet(
      NameIdentifier ident,
      Domain.Status status,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.updateDomainStatusAndGet(ident, status, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public Map<Long, List<GenericEntity>> listDomainAssignments(
      String metalake,
      List<DomainEntity> domains,
      Set<MetadataObject.Type> types,
      Map<Long, List<GenericEntity>> selectedAssignments)
      throws IOException {
    return backend.listDomainAssignments(metalake, domains, types, selectedAssignments);
  }

  @Override
  public Map<Long, List<TagEntity>> listDirectTagsByObjectIds(
      String metalake, MetadataObject.Type type, List<Long> objectIds) throws IOException {
    return backend.listDirectTagsByObjectIds(metalake, type, objectIds);
  }

  @Override
  public List<DomainEntity> listDomainsByBackingTagIds(String metalake, List<Long> backingTagIds)
      throws IOException {
    return backend.listDomainsByBackingTagIds(metalake, backingTagIds);
  }

  @Override
  public List<DomainEntity> listSubDomains(NameIdentifier parentIdent) throws IOException {
    return backend.listSubDomains(parentIdent);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException {
    return backend.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);
  }

  @Override
  public List<RelationalEntity<?>> batchListEntitiesByRelation(
      Type relType, List<NameIdentifier> nameIdentifiers, Entity.EntityType identType)
      throws IOException {
    if (nameIdentifiers == null || nameIdentifiers.isEmpty()) {
      return new ArrayList<>();
    }
    return backend.batchListEntitiesByRelation(relType, nameIdentifiers, identType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E getEntityByRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException {
    return backend.getEntityByRelation(relType, srcIdentifier, srcType, destEntityIdent);
  }

  @Override
  public void insertRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    boolean finished = false;
    try {
      backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          String.format(
              "The %s relation of %s %s",
              relType.name().toLowerCase(Locale.ROOT),
              srcType.name().toLowerCase(Locale.ROOT),
              srcIdentifier));
    } finally {
      // Relation query results themselves are not cached, but both endpoints may be cached
      // entities (OWNER_REL, TAG_METADATA_OBJECT_REL and METADATA_OBJECT_ROLE_REL are keyed by
      // catalog/schema/table/... on the source side), so drop their entries conservatively: a
      // relation write can change data materialized into the endpoint entity. Note this is not
      // free — EntityCache#invalidate cascades over the identifier hierarchy, so invalidating a
      // catalog also drops every cached schema and table beneath it.
      invalidateChanged(finished, srcIdentifier, srcType);
      invalidateChanged(finished, dstIdentifier, dstType);
    }
  }

  @Override
  public void batchInsertRelations(
      Type relType,
      List<NameIdentifier> srcIdentifiers,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    if (srcIdentifiers == null || srcIdentifiers.isEmpty()) {
      return;
    }
    boolean finished = false;
    try {
      backend.batchInsertRelations(
          relType, srcIdentifiers, srcType, dstIdentifier, dstType, override);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          String.format(
              "The %s relations of %d %s entities starting with %s",
              relType.name().toLowerCase(Locale.ROOT),
              srcIdentifiers.size(),
              srcType.name().toLowerCase(Locale.ROOT),
              srcIdentifiers.get(0)));
    } finally {
      // Invalidate both endpoints for the same reason as insertRelation, including the hierarchy
      // cascade noted there.
      for (NameIdentifier ident : srcIdentifiers) {
        invalidateChanged(finished, ident, srcType);
      }
      invalidateChanged(finished, dstIdentifier, dstType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    RelationUpdate update =
        RelationUpdate.of(
            relType,
            srcEntityIdent,
            srcEntityType,
            toRelationEdgeTargets(relType, destEntitiesToAdd),
            toRelationEdgeTargets(relType, destEntitiesToRemove));
    if (relType != SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL) {
      return updateEntityRelations(update);
    }

    List<E> result;
    boolean finished = false;
    try {
      result =
          backend.updateEntityRelations(
              relType, srcEntityIdent, srcEntityType, destEntitiesToAdd, destEntitiesToRemove);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, srcEntityType, srcEntityIdent);
    } finally {
      if (finished || SessionUtils.isInTransaction()) {
        Entity.EntityType targetEntityType = relationUpdateTargetType(relType);
        invalidateCache(srcEntityIdent, srcEntityType);
        invalidateRelationTargetCache(targetEntityType, update.targetsToAdd());
        invalidateRelationTargetCache(targetEntityType, update.targetsToRemove());
      }
    }

    return result;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(RelationQuery query)
      throws IOException {
    if (query.anchorId().isPresent() || query.relationValue().isPresent()) {
      return backend.listEntitiesByRelation(query);
    }

    return listEntitiesByRelation(
        query.relationType(),
        query.anchorIdentifier(),
        query.anchorEntityType(),
        query.allFields());
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(RelationUpdate update)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    validateRelationTargetTypes(update);

    RelationEdgeTarget[] targetsToAdd = update.targetsToAdd();
    RelationEdgeTarget[] targetsToRemove = update.targetsToRemove();
    List<E> result;
    boolean finished = false;
    try {
      result = backend.updateEntityRelations(update);
      finished = true;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, update.sourceEntityType(), update.sourceIdentifier());
    } finally {
      if (finished || SessionUtils.isInTransaction()) {
        Entity.EntityType targetEntityType = relationUpdateTargetType(update.relationType());
        invalidateCache(update.sourceIdentifier(), update.sourceEntityType());
        invalidateRelationTargetCache(targetEntityType, targetsToAdd);
        invalidateRelationTargetCache(targetEntityType, targetsToRemove);
      }
    }

    return result;
  }

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    try {
      return backend.batchDelete(entitiesToDelete, cascade);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          describeBatch(
              entitiesToDelete.size(),
              entitiesToDelete.isEmpty() ? null : entitiesToDelete.get(0).getRight(),
              entitiesToDelete.isEmpty() ? null : entitiesToDelete.get(0).getLeft()));
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    try {
      backend.batchPut(entities, overwritten);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          describeBatch(
              entities.size(),
              entities.isEmpty() ? null : entities.get(0).type(),
              entities.isEmpty() ? null : entities.get(0).nameIdentifier()));
    }
  }

  /** Describes a batch write by its size and its first entity, for a conflict message. */
  private static String describeBatch(
      int size, @Nullable Entity.EntityType firstType, @Nullable NameIdentifier first) {
    if (firstType == null) {
      return String.format("The batch of %d entities", size);
    }
    return String.format(
        "The batch of %d entities starting with %s %s",
        size, firstType.name().toLowerCase(Locale.ROOT), first);
  }

  private void invalidateRelationTargetCache(
      Entity.EntityType targetEntityType, RelationEdgeTarget[] relationTargets) {
    for (RelationEdgeTarget relationTarget : relationTargets) {
      invalidateCache(relationTarget.nameIdentifier(), targetEntityType);
    }
  }

  private static void validateRelationTargetTypes(RelationUpdate update) {
    Entity.EntityType targetEntityType = relationUpdateTargetType(update.relationType());
    validateRelationTargetTypes(update.relationType(), targetEntityType, update.targetsToAdd());
    validateRelationTargetTypes(update.relationType(), targetEntityType, update.targetsToRemove());
  }

  private static void validateRelationTargetTypes(
      Type relType, Entity.EntityType targetEntityType, RelationEdgeTarget[] relationTargets) {
    for (RelationEdgeTarget relationTarget : relationTargets) {
      Preconditions.checkArgument(
          relationTarget.entityType() == targetEntityType,
          "Relation target type %s does not match expected destination type %s for relation type %s",
          relationTarget.entityType(),
          targetEntityType,
          relType);
    }
  }

  private static RelationEdgeTarget[] toRelationEdgeTargets(
      Type relType, NameIdentifier[] nameIdentifiers) {
    if (nameIdentifiers == null) {
      return new RelationEdgeTarget[0];
    }

    Entity.EntityType targetEntityType = relationUpdateTargetType(relType);
    return Arrays.stream(nameIdentifiers)
        .map(nameIdentifier -> RelationEdgeTarget.of(nameIdentifier, targetEntityType, null))
        .toArray(RelationEdgeTarget[]::new);
  }

  private static Entity.EntityType relationUpdateTargetType(Type relType) {
    switch (relType) {
      case TAG_METADATA_OBJECT_REL:
        return Entity.EntityType.TAG;
      case POLICY_TAG_REL:
        return Entity.EntityType.POLICY;
      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the relation type %s", relType));
    }
  }

  private <E extends Entity & HasIdentifier> void cacheIfEpochUnchanged(E entity, long epoch) {
    if (cache instanceof BaseEntityCache && !BaseEntityCache.isCacheable(entity.type())) {
      // Uncached entities still invoke the cache's related-invalidation hook. There is no stale
      // value to publish.
      cache.put(entity);
      return;
    }
    synchronized (committedCacheLock) {
      // Invalidation takes this same lock, so the epoch cannot move between the check and put.
      if (cacheInvalidationEpoch.get() == epoch) {
        cache.put(entity);
      }
    }
  }

  /**
   * Whether this thread may read or publish the process-wide entity cache.
   *
   * <p>The cache mirrors committed rows. When this thread already has a SQL session, the backend
   * has not committed, so reads use that connection and are not published, and writes only remember
   * which keys to drop. The outermost commit removes those keys, then commits, while holding {@link
   * #committedCacheLock}. A rollback or a plain close forgets the keys and leaves the cache as it
   * was. With no open session, including the change-log poller, changes are already durable and the
   * cache is updated immediately.
   *
   * @return {@code false} when this thread must ignore the process cache
   */
  private boolean useCommittedCache() {
    return !SessionUtils.isInTransaction();
  }

  /**
   * Drops {@code ident} when the write completed, and also when it failed inside an open
   * transaction. A failure with no open session was already rolled back by the backend, so the
   * committed cache is still correct.
   */
  private void invalidateChanged(boolean finished, NameIdentifier ident, Entity.EntityType type) {
    if (finished || SessionUtils.isInTransaction()) {
      invalidateCache(ident, type);
    }
  }

  private void invalidateCache(NameIdentifier ident, Entity.EntityType type) {
    if (SessionUtils.isInTransaction()) {
      pendingInvalidations.get().add(EntityCacheKey.of(ident, type));
      if (!commitHookInstalled.get()) {
        SqlSessions.wrapOutermostCommit(
            this::commitPendingInvalidations, this::discardPendingInvalidations);
        commitHookInstalled.set(Boolean.TRUE);
      }
      return;
    }
    synchronized (committedCacheLock) {
      cacheInvalidationEpoch.incrementAndGet();
      cache.invalidate(ident, type);
    }
  }

  private void commitPendingInvalidations(Runnable jdbcCommit) {
    synchronized (committedCacheLock) {
      cacheInvalidationEpoch.incrementAndGet();
      try {
        dropPendingKeys();
        jdbcCommit.run();
        // A write on this thread during commit() joins the same transaction. Drop those keys too,
        // still before the lock is released. The commit has already succeeded, so a failure here
        // empties the cache instead of failing the caller.
        try {
          dropPendingKeys();
        } catch (RuntimeException failure) {
          cache.clear();
          LOGGER.error("Failed to drop cache keys written during SQL commit", failure);
        }
      } catch (RuntimeException failure) {
        // A partial removal or a failed commit must not leave a mix of old and new rows.
        cache.clear();
        throw failure;
      } finally {
        // Move the epoch again before releasing the lock, so a read that loaded the pre-commit
        // row cannot publish it.
        cacheInvalidationEpoch.incrementAndGet();
        discardPendingInvalidations();
      }
    }
  }

  private void dropPendingKeys() {
    Set<EntityCacheKey> pending = pendingInvalidations.get();
    if (pending.isEmpty()) {
      return;
    }
    List<EntityCacheKey> keys = new ArrayList<>(pending);
    pending.clear();
    for (EntityCacheKey key : keys) {
      cache.invalidate(key.identifier(), key.entityType());
    }
  }

  private void discardPendingInvalidations() {
    pendingInvalidations.remove();
    commitHookInstalled.remove();
  }

  @VisibleForTesting
  void clearCache() {
    synchronized (committedCacheLock) {
      cacheInvalidationEpoch.incrementAndGet();
      cache.clear();
    }
  }
}
