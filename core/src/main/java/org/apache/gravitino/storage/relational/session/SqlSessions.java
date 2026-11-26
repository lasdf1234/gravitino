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
  private static final ThreadLocal<List<Runnable>> beforeCommitActions =
      ThreadLocal.withInitial(ArrayList::new);
  private static final ThreadLocal<List<Runnable>> afterCloseActions =
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
   * {@code afterClose} after that session is closed. Each call adds one pair. Pairs run in
   * registration order on the outermost close.
   *
   * <p>A {@code beforeCommit} that throws skips its own {@code afterClose} and every later {@code
   * beforeCommit}. {@code afterClose} actions registered after that failure still run, so a
   * participant can drop registration state. Those actions must tolerate their {@code beforeCommit}
   * not having run.
   *
   * @param beforeCommit the action to run before commit or rollback
   * @param afterClose the action to run after the session is closed
   * @throws IllegalStateException if the current thread has no open session
   */
  public static void onClose(Runnable beforeCommit, Runnable afterClose) {
    if (!hasActiveSession()) {
      throw new IllegalStateException("No active SQL session on the current thread");
    }
    beforeCommitActions.get().add(beforeCommit);
    afterCloseActions.get().add(afterClose);
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
      closeOuterSession(sqlSession, commit, rollback);
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      closeOuterSession(sqlSession, false, true);
    }
  }

  /**
   * Closes the outermost SQL session.
   *
   * <p>Order: run each {@code beforeCommit}, commit or roll back, close the session, then run
   * {@code afterClose} actions. A {@code beforeCommit} that throws skips its own {@code afterClose}
   * and later {@code beforeCommit} actions, and the session is rolled back. {@code afterClose} for
   * pairs registered after the failure still runs. If commit, rollback, or close throws, that
   * exception is thrown. If the session finished and a later {@code afterClose} throws, the
   * exception is logged and the caller still succeeds.
   */
  private static void closeOuterSession(SqlSession sqlSession, boolean commit, boolean rollback) {
    List<Runnable> beforeCommits = beforeCommitActions.get();
    List<Runnable> afterCloses = afterCloseActions.get();
    beforeCommitActions.remove();
    afterCloseActions.remove();

    List<Runnable> afterClosesToRun = new ArrayList<>();
    RuntimeException failure = runBeforeCommits(beforeCommits, afterCloses, afterClosesToRun);
    failure = commitOrRollback(sqlSession, commit, rollback, failure);
    failure = closeSession(sqlSession, failure);
    runAfterCloses(afterClosesToRun, failure);
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Runs before-commit callbacks in order. A failure skips later before-commit callbacks and the
   * failing pair's after-close, and schedules the after-close of every later pair.
   */
  private static RuntimeException runBeforeCommits(
      List<Runnable> beforeCommits, List<Runnable> afterCloses, List<Runnable> afterClosesToRun) {
    for (int i = 0; i < beforeCommits.size(); i++) {
      Runnable beforeCommit = beforeCommits.get(i);
      Runnable afterClose = afterCloses.get(i);
      if (beforeCommit == null) {
        if (afterClose != null) {
          afterClosesToRun.add(afterClose);
        }
        continue;
      }
      try {
        beforeCommit.run();
      } catch (RuntimeException exception) {
        scheduleSkippedAfterCloses(afterCloses, afterClosesToRun, i + 1);
        return exception;
      }
      if (afterClose != null) {
        afterClosesToRun.add(afterClose);
      }
    }
    return null;
  }

  /** Schedules after-close actions for pairs whose before-commit callback did not run. */
  private static void scheduleSkippedAfterCloses(
      List<Runnable> afterCloses, List<Runnable> afterClosesToRun, int fromIndex) {
    for (int i = fromIndex; i < afterCloses.size(); i++) {
      Runnable afterClose = afterCloses.get(i);
      if (afterClose != null) {
        afterClosesToRun.add(afterClose);
      }
    }
  }

  /**
   * Commits, rolls back, or rolls back because a before-commit callback failed. A failed commit is
   * also rolled back so the connection does not keep the partial write.
   */
  private static RuntimeException commitOrRollback(
      SqlSession sqlSession, boolean commit, boolean rollback, RuntimeException failure) {
    if (failure != null || rollback) {
      return rollbackSession(sqlSession, failure);
    }
    if (!commit) {
      return null;
    }
    try {
      sqlSession.commit();
      return null;
    } catch (RuntimeException exception) {
      return rollbackSession(sqlSession, exception);
    }
  }

  /**
   * Rolls the session back and keeps {@code failure} as the exception the caller sees. A rollback
   * that throws is attached to {@code failure}, or becomes the caller exception when {@code
   * failure} is null.
   *
   * <p>The rollback is forced. A policy hook can write on this connection without a mapped
   * statement, and MyBatis skips rollback when the session looks clean. Closing that session turns
   * auto-commit back on, which would commit the hook write.
   */
  private static RuntimeException rollbackSession(SqlSession sqlSession, RuntimeException failure) {
    try {
      sqlSession.rollback(true);
      return failure;
    } catch (RuntimeException exception) {
      if (failure == null) {
        return exception;
      }
      failure.addSuppressed(exception);
      return failure;
    }
  }

  /** Closes the session and removes it from this thread. */
  private static RuntimeException closeSession(SqlSession sqlSession, RuntimeException failure) {
    try {
      sqlSession.close();
      return failure;
    } catch (RuntimeException exception) {
      if (failure == null) {
        return exception;
      }
      failure.addSuppressed(exception);
      return failure;
    } finally {
      sessions.remove();
      sessionCount.remove();
    }
  }

  /**
   * Runs cleanup after the session is gone. When the session itself failed, a cleanup failure is
   * attached to that exception. When the session succeeded, a cleanup failure is only logged.
   */
  private static void runAfterCloses(List<Runnable> afterCloses, RuntimeException sessionFailure) {
    for (Runnable afterClose : afterCloses) {
      try {
        afterClose.run();
      } catch (RuntimeException exception) {
        if (sessionFailure != null) {
          sessionFailure.addSuppressed(exception);
        } else {
          LOG.warn(
              "Cleanup after the SQL session closed failed. The transaction was already finished.",
              exception);
        }
      }
    }
  }
}
