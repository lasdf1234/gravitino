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
 */
public final class SqlSessions {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessions.class);
  private static final ThreadLocal<SqlSession> sessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> sessionCount =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));
  private static final ThreadLocal<List<CloseAction>> closeActions =
      ThreadLocal.withInitial(ArrayList::new);

  private SqlSessions() {}

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
   * Runs {@code beforeCommit} before this thread's outermost session commits or rolls back, and
   * {@code afterClose} after that session is closed. Each call adds one pair. Every pair registered
   * for the session runs, in registration order, on the outermost close.
   *
   * @param beforeCommit the action to run before commit or rollback
   * @param afterClose the action to run after the session is closed when {@code beforeCommit}
   *     returned
   * @throws IllegalStateException if the current thread has no open session
   */
  public static void onClose(Runnable beforeCommit, Runnable afterClose) {
    if (!hasActiveSession()) {
      throw new IllegalStateException("No active SQL session on the current thread");
    }
    closeActions.get().add(new CloseAction(beforeCommit, afterClose));
  }

  /**
   * Closes the session bound for tests and runs the outer-close action.
   *
   * @param commit whether the close commits; otherwise it rolls back
   */
  @VisibleForTesting
  public static void closeOuterSessionForTest(boolean commit) {
    handleSessionClose(commit, !commit);
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
      return;
    }

    int count = sessionCount.get().decrementAndGet();
    if (count == 0) {
      finishOuterSession(sqlSession, commit, rollback);
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      finishOuterSession(sqlSession, false, true);
    }
  }

  /**
   * Runs every registered before-commit action, then commits or rolls back, then closes the
   * session. A before-commit failure rolls the session back and closes it, and that action's
   * after-close is skipped. A failure while finishing the session is propagated. Cleanup that fails
   * after the session has already been committed or rolled back and closed is logged and does not
   * fail the caller.
   */
  private static void finishOuterSession(SqlSession sqlSession, boolean commit, boolean rollback) {
    List<CloseAction> actions = closeActions.get();
    closeActions.remove();
    RuntimeException failure = null;
    try {
      for (CloseAction action : actions) {
        if (action.beforeCommit == null) {
          action.beforeCommitCompleted = true;
          continue;
        }
        try {
          action.beforeCommit.run();
          action.beforeCommitCompleted = true;
        } catch (RuntimeException exception) {
          failure = exception;
          break;
        }
      }
      try {
        if (failure != null) {
          failure = rollbackPreserving(sqlSession, failure);
        } else if (commit) {
          try {
            sqlSession.commit();
          } catch (RuntimeException exception) {
            failure = rollbackPreserving(sqlSession, exception);
          }
        } else if (rollback) {
          // Force the JDBC rollback. A policy lifecycle hook can write on this connection without
          // going through a mapped statement, and MyBatis skips rollback when the session looks
          // clean. Closing that session then turns auto-commit back on, which would commit the
          // hook write.
          try {
            sqlSession.rollback(true);
          } catch (RuntimeException exception) {
            failure = exception;
          }
        }
      } finally {
        try {
          sqlSession.close();
        } catch (RuntimeException exception) {
          failure = suppress(failure, exception);
        } finally {
          sessions.remove();
          sessionCount.remove();
        }
      }
    } finally {
      RuntimeException sessionFailure = failure;
      for (CloseAction action : actions) {
        if (!action.beforeCommitCompleted || action.afterClose == null) {
          continue;
        }
        try {
          action.afterClose.run();
        } catch (RuntimeException exception) {
          if (sessionFailure != null) {
            failure = suppress(failure, exception);
          } else {
            LOG.warn(
                "Cleanup after the SQL session closed failed. The transaction was already"
                    + " finished.",
                exception);
          }
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static RuntimeException rollbackPreserving(
      SqlSession sqlSession, RuntimeException primary) {
    try {
      sqlSession.rollback(true);
    } catch (RuntimeException exception) {
      primary.addSuppressed(exception);
    }
    return primary;
  }

  private static RuntimeException suppress(RuntimeException primary, RuntimeException extra) {
    if (primary == null) {
      return extra;
    }
    primary.addSuppressed(extra);
    return primary;
  }

  private static final class CloseAction {
    private final Runnable beforeCommit;
    private final Runnable afterClose;
    private boolean beforeCommitCompleted;

    private CloseAction(Runnable beforeCommit, Runnable afterClose) {
      this.beforeCommit = beforeCommit;
      this.afterClose = afterClose;
    }
  }
}
