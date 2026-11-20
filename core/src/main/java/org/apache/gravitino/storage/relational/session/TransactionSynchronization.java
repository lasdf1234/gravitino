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

package org.apache.gravitino.storage.relational.session;

/**
 * Callbacks invoked when the current thread's outermost SQL session completes.
 *
 * <p>Register with {@link SqlSessions#registerSynchronization(TransactionSynchronization)}. Nested
 * {@code getSqlSession}/{@code commitAndClose} pairs do not fire these callbacks; only the boundary
 * that drives {@code sessionCount} to zero does, after the JDBC commit or rollback.
 */
public interface TransactionSynchronization {

  /** Called after a successful outermost JDBC commit. */
  default void afterCommit() {}

  /** Called after an outermost JDBC rollback (or a plain close that drops the session). */
  default void afterRollback() {}
}
