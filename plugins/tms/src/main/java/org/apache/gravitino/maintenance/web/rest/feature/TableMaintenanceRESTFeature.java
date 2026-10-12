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
package org.apache.gravitino.maintenance.web.rest.feature;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.occupancy.OccupancyReconciler;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jersey feature that starts table maintenance db-scheduler pools and occupancy reconcile when the
 * extension package is enabled (§7.1).
 *
 * <p>TMS does not register public REST resources in this change; the feature owns server lifecycle
 * for in-process scheduling and stale occupancy cleanup.
 */
@Provider
public class TableMaintenanceRESTFeature implements Feature {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceRESTFeature.class);

  /** Package name for {@link Configs#REST_API_EXTENSION_PACKAGES}. */
  public static final String EXTENSION_PACKAGE = TableMaintenanceRESTFeature.class.getPackageName();

  private static final AtomicReference<TableMaintenanceSchedulerPools> SCHEDULER_POOLS =
      new AtomicReference<>();
  private static final AtomicReference<ScheduledExecutorService> RECONCILE_EXECUTOR =
      new AtomicReference<>();

  @Override
  public boolean configure(FeatureContext context) {
    Config config = GravitinoEnv.getInstance().config();
    if (!Configs.RELATIONAL_ENTITY_STORE.equals(config.get(Configs.ENTITY_STORE))) {
      throw new IllegalStateException(
          "Table maintenance requires gravitino.entity.store = relational");
    }

    TableMaintenanceConfig maintenanceConfig = TableMaintenanceConfig.from(config);
    DataSource dataSource = resolveEntityStoreDataSource();
    TableMaintenanceSchedulerPools pools = new TableMaintenanceSchedulerPools();
    pools.start(dataSource, maintenanceConfig);
    SCHEDULER_POOLS.set(pools);

    OccupancyReconciler reconciler = new OccupancyReconciler();
    reconciler.reconcile();
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "tms-occupancy-reconcile");
              t.setDaemon(true);
              return t;
            });
    executor.scheduleWithFixedDelay(
        () -> {
          try {
            reconciler.reconcile();
          } catch (Exception e) {
            LOG.warn("Occupancy reconcile failed", e);
          }
        },
        maintenanceConfig.pollingIntervalMs(),
        maintenanceConfig.pollingIntervalMs(),
        TimeUnit.MILLISECONDS);
    RECONCILE_EXECUTOR.set(executor);

    LOG.info("Table maintenance scheduler pools and occupancy reconcile are running");
    return true;
  }

  /**
   * Stops scheduler pools and the occupancy reconcile executor. Intended for tests; production
   * shutdown also relies on db-scheduler shutdown hooks.
   */
  public static void stopSchedulers() {
    ScheduledExecutorService executor = RECONCILE_EXECUTOR.getAndSet(null);
    if (executor != null) {
      executor.shutdownNow();
    }
    TableMaintenanceSchedulerPools pools = SCHEDULER_POOLS.getAndSet(null);
    if (pools != null) {
      pools.stop();
    }
  }

  private static DataSource resolveEntityStoreDataSource() {
    Object dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    if (!(dataSource instanceof DataSource)) {
      throw new IllegalStateException("Entity store data source is not available");
    }
    return (DataSource) dataSource;
  }
}
