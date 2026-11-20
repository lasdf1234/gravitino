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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlSessions is a utility class to maintain the MyBatis's {@link SqlSession} object. It is a
 * thread local class and should be used to get the {@link SqlSession} object. It also provides the
 * methods to commit, rollback and close the {@link SqlSession} object.
 *
 * <p>Work registered with {@link #afterSuccessfulCommit(Runnable)} runs once, after the outermost
 * session's JDBC commit has returned. Nested session boundaries do not run it. Rollback, a plain
 * close, and a commit that throws drop it.
 */
public final class SqlSessions {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessions.class);
  private static final ThreadLocal<SqlSession> sessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> sessionCount =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));
  private static final ThreadLocal<List<Runnable>> afterCommitActions =
      ThreadLocal.withInitial(ArrayList::new);

  private SqlSessions() {}

  /**
   * Runs {@code action} after the current thread's outermost SQL session commits successfully.
   *
   * <p>The action runs after the session has been closed and removed, so it must not use this
   * session. It does not run when the outermost boundary rolls back, closes without committing, or
   * when {@link SqlSession#commit()} throws. A nested {@code commitAndCloseSqlSession} only
   * decrements the session count and leaves the action queued. An action that throws is logged; the
   * transaction is already committed, and later actions from the same commit still run.
   *
   * @param action the work to run after a successful outermost commit
   * @throws IllegalArgumentException if {@code action} is null
   * @throws IllegalStateException if the current thread has no open SQL session
   */
  public static void afterSuccessfulCommit(Runnable action) {
    Preconditions.checkArgument(action != null, "action cannot be null");
    if (sessions.get() == null) {
      throw new IllegalStateException(
          "No active SQL session on the current thread; cannot defer work until commit");
    }
    afterCommitActions.get().add(action);
  }

  @VisibleForTesting
  static ThreadLocal<SqlSession> getSessions() {
    return sessions;
  }

  @VisibleForTesting
  static Integer getSessionCount() {
    return sessionCount.get().get();
  }

  /**
   * Get the SqlSession object. If the SqlSession object is not present in the thread local, then
   * create a new SqlSession object and set it in the thread local. This method also increments the
   * session count.
   *
   * @return SqlSession object from the thread local storage.
   */
  public static SqlSession getSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      sqlSession =
          SqlSessionFactoryHelper.getInstance()
              .getSqlSessionFactory()
              .openSession(TransactionIsolationLevel.READ_COMMITTED);
      sessions.set(sqlSession);
    }
    sessionCount.get().incrementAndGet();
    return sqlSession;
  }

  /**
   * Commit the SqlSession object and close it. It also removes the SqlSession object from the
   * thread local storage.
   */
  public static void commitAndCloseSqlSession() {
    handleSessionClose(true /* commit */, false /* rollback */);
  }

  /**
   * Rollback the SqlSession object and close it. It also removes the SqlSession object from the
   * thread local storage.
   */
  public static void rollbackAndCloseSqlSession() {
    handleSessionClose(false /* commit */, true /* rollback */);
  }

  /** Close the SqlSession object and remove it from the thread local storage. */
  public static void closeSqlSession() {
    handleSessionClose(false /* commit */, false /* rollback */);
  }

  /**
   * Get the Mapper object from the SqlSession object. This method will open a session if one is not
   * already opened.
   *
   * @param <T> the type of the mapper interface.
   * @param className the class name of the Mapper object.
   * @return the Mapper object.
   */
  public static <T> T getMapper(Class<T> className) {
    // getSqlSession() is called to ensure a session exists and increment the count.
    return getSqlSession().getMapper(className);
  }

  /**
   * Returns whether the current thread already owns an open SQL session.
   *
   * @return {@code true} when an SQL session is active on the current thread
   */
  public static boolean hasActiveSession() {
    return sessions.get() != null;
  }

  /**
   * Returns the connection of the current thread's open session without changing the session count.
   *
   * <p>Callers must not commit, roll back, or close the connection. The session owner commits it.
   *
   * @return the connection bound to the current session
   * @throws IllegalStateException if the current thread has no open session
   * @throws SQLException if the session cannot provide its connection
   */
  public static Connection currentConnection() throws SQLException {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      throw new IllegalStateException("No active SQL session on the current thread");
    }
    return sqlSession.getConnection();
  }

  private static void handleSessionClose(boolean commit, boolean rollback) {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      // Nothing committed. Drop queued work so it cannot attach to a later session on this thread.
      afterCommitActions.remove();
      return;
    }

    int count = sessionCount.get().decrementAndGet();
    if (count == 0) {
      boolean committed = false;
      try {
        if (commit) {
          sqlSession.commit();
          committed = true;
        } else if (rollback) {
          // Force the JDBC rollback. A policy lifecycle hook can write on this connection without
          // going
          // through a mapped statement, and MyBatis skips rollback when the session looks clean.
          // Closing that session then turns auto-commit back on, which would commit the hook write.
          sqlSession.rollback(true);
        }
      } finally {
        try {
          // Ensure the session is always closed
          sqlSession.close();
        } finally {
          // Drop the session before the callbacks so they observe "no open transaction".
          // `committed` is true only after SqlSession.commit() returns. A plain close rolls the
          // connection back (auto-commit is off) and must not apply cache side effects.
          sessions.remove();
          sessionCount.remove();
          runAfterCommit(committed);
        }
      }
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      sessions.remove();
      sessionCount.remove();
      afterCommitActions.remove();
    }
  }

  private static void runAfterCommit(boolean committed) {
    List<Runnable> actions = afterCommitActions.get();
    afterCommitActions.remove();
    if (!committed || actions.isEmpty()) {
      return;
    }
    for (Runnable action : actions) {
      try {
        action.run();
      } catch (RuntimeException e) {
        LOG.error(
            "Post-commit callback failed after the SQL transaction committed; "
                + "later callbacks on this commit still run",
            e);
      }
    }
  }
}
