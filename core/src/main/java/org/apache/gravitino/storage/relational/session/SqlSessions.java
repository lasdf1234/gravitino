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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlSessions is a utility class to maintain the MyBatis's {@link SqlSession} object. It is a
 * thread local class and should be used to get the {@link SqlSession} object. It also provides the
 * methods to commit, rollback and close the {@link SqlSession} object.
 *
 * <p>{@link #wrapOutermostCommit(Consumer, Runnable)} runs only when this thread's outermost
 * session commits or aborts. Nested boundaries do not run it.
 */
public final class SqlSessions {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessions.class);
  private static final ThreadLocal<SqlSession> sessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> sessionCount =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));
  private static final ThreadLocal<Consumer<Runnable>> outermostCommit = new ThreadLocal<>();
  private static final ThreadLocal<Runnable> outermostAbort = new ThreadLocal<>();

  private SqlSessions() {}

  /**
   * Wraps the JDBC commit of the current thread's outermost SQL session.
   *
   * <p>{@code commit} is called only when that session commits. It is given the JDBC commit and
   * must call it once. Cache entries that the commit must not leave behind are removed before that
   * call, so they are gone before the database change is visible. Nested commits do not call {@code
   * commit}. Rollback, a plain close, and a failure before the JDBC commit call {@code abort} and
   * do not commit.
   *
   * @param commit wraps the JDBC commit of the outermost session
   * @param abort drops work that must not be applied when the session does not commit
   * @throws IllegalArgumentException if either argument is null
   * @throws IllegalStateException if the current thread has no open SQL session, or a wrapper is
   *     already registered
   */
  public static void wrapOutermostCommit(Consumer<Runnable> commit, Runnable abort) {
    Preconditions.checkArgument(commit != null, "commit cannot be null");
    Preconditions.checkArgument(abort != null, "abort cannot be null");
    if (sessions.get() == null) {
      throw new IllegalStateException(
          "No active SQL session on the current thread; cannot wrap its commit");
    }
    if (outermostCommit.get() != null) {
      throw new IllegalStateException("An outermost commit wrapper is already registered");
    }
    outermostCommit.set(commit);
    outermostAbort.set(abort);
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
      abortOutermost();
      return;
    }

    int count = sessionCount.get().decrementAndGet();
    if (count == 0) {
      AtomicBoolean committed = new AtomicBoolean(false);
      RuntimeException failure = null;
      try {
        if (commit) {
          failure = commitOutermost(sqlSession, committed);
        } else if (rollback) {
          // Force the JDBC rollback. A policy lifecycle hook can write on this connection without
          // going through a mapped statement, and MyBatis skips rollback when the session looks
          // clean. Closing that session then turns auto-commit back on, which would commit the
          // hook write.
          sqlSession.rollback(true);
        }
      } finally {
        try {
          // Ensure the session is always closed. A plain close rolls the connection back
          // (auto-commit is off).
          sqlSession.close();
        } catch (RuntimeException closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else {
            failure.addSuppressed(closeFailure);
          }
        } finally {
          sessions.remove();
          sessionCount.remove();
          if (committed.get()) {
            outermostCommit.remove();
            outermostAbort.remove();
          } else {
            abortOutermost();
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      sessions.remove();
      sessionCount.remove();
      abortOutermost();
    }
  }

  private static RuntimeException commitOutermost(SqlSession sqlSession, AtomicBoolean committed) {
    Consumer<Runnable> hook = outermostCommit.get();
    try {
      if (hook != null) {
        hook.accept(
            () -> {
              sqlSession.commit();
              committed.set(true);
            });
        // Returning without calling the JDBC commit leaves the transaction uncommitted.
        if (!committed.get()) {
          rollbackQuietly(sqlSession);
        }
      } else {
        sqlSession.commit();
        committed.set(true);
      }
      return null;
    } catch (RuntimeException failure) {
      if (!committed.get()) {
        rollbackQuietly(sqlSession);
      }
      return failure;
    }
  }

  private static void rollbackQuietly(SqlSession sqlSession) {
    try {
      sqlSession.rollback(true);
    } catch (RuntimeException rollbackFailure) {
      LOG.warn("Rollback after a failed SQL commit failed", rollbackFailure);
    }
  }

  private static void abortOutermost() {
    Runnable abort = outermostAbort.get();
    outermostCommit.remove();
    outermostAbort.remove();
    if (abort == null) {
      return;
    }
    try {
      abort.run();
    } catch (RuntimeException failure) {
      LOG.error("Failed to discard cache work for an aborted SQL transaction", failure);
    }
  }
}
