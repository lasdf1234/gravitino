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
package org.apache.gravitino.maintenance.policy;

/**
 * {@code gravitino.conf} prefixes for cluster-static TMS client auth bags.
 *
 * <p>These values are overlaid at {@code runJob} onto policy {@code updateOptions} and {@code
 * jobOptions}; they are not stored in policy content. See the Table Maintenance Service design
 * document.
 */
public final class MaintenanceAuthConfigKeys {

  /** Prefix for Gravitino control-plane client auth (merged into {@code updateOptions}). */
  public static final String GRAVITINO_AUTH_PREFIX = "gravitino.maintenance.gravitinoAuth.";

  /** Prefix for Iceberg REST client auth for Spark jobs (merged into {@code jobOptions}). */
  public static final String IRC_AUTH_PREFIX = "gravitino.maintenance.ircAuth.";

  private MaintenanceAuthConfigKeys() {}
}
