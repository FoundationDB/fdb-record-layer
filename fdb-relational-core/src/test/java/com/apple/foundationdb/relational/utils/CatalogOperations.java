/*
 * CatalogOperations.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.Utils;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test-only synchronization helpers for catalog-mutating DDL (CREATE/DROP DATABASE,
 * CREATE/DROP SCHEMA TEMPLATE, CREATE/DROP SCHEMA).
 * <p>
 * Under parallel JUnit class execution, every test class's {@code @BeforeEach}/{@code @AfterEach}
 * hooks race on the cluster-wide catalog metadata stored in {@code /__SYS/CATALOG}. The FDB
 * commit layer surfaces those races as SQLSTATE 40001 transaction conflicts. The
 * {@link #runLockedWithRetry(ThrowingRunnable)} wrapper serializes the operations within the JVM
 * (mirroring the {@code FRL.catalogLock()} pattern used by yaml-tests' {@code SetupBlock}) and
 * retries any residual conflicts caused by metadata writes the lock doesn't cover
 * (cluster-global version-stamp updates, etc.).
 * <p>
 * The wrapper also retries on {@link RecordStoreDoesNotExistException}: under concurrent
 * extension setup, a {@link SchemaRule}'s {@code @BeforeEach} can race the very first call to
 * {@code /__SYS} and observe the record store before another thread's bootstrap has fully
 * propagated. The retry covers that window.
 * <p>
 * Wrap CREATE/DROP DDL in {@code @BeforeEach}/{@code @AfterEach} hooks with this helper. Drop
 * statements should use {@code DROP ... IF EXISTS} so a retry after a partial success doesn't
 * trip an "unknown ..." error.
 */
public final class CatalogOperations {

    /**
     * JVM-wide monitor guarding catalog-mutating DDL issued from test rules. Local to this
     * fdb-relational-core test classpath; FRL construction in fdb-relational-server has its own
     * separate {@code FRL.catalogLock()} and is also retry-guarded.
     */
    private static final Object CATALOG_LOCK = new Object();

    private static final int MAX_ATTEMPTS = 5;

    /**
     * URI of the system catalog database every {@link #runDdl} invocation connects against.
     */
    private static final URI SYS_CATALOG_URI = URI.create("jdbc:embed:/__SYS");

    private CatalogOperations() {
    }

    /**
     * Runs one or more catalog-mutating DDL statements under the JVM-wide catalog lock with
     * retry on transient races. Each call opens a fresh connection to {@code /__SYS}, sets the
     * schema to {@code CATALOG}, applies {@code connectionOptions}, and runs every statement in
     * order. Drop statements should use {@code DROP ... IF EXISTS} so a retry after a partial
     * success doesn't trip an "unknown ..." error.
     * <p>
     * This is the preferred entry point for test code that needs to issue catalog DDL — prefer
     * it over calling {@link #runLockedWithRetry(ThrowingRunnable)} directly, which only exists
     * for callers that need to issue catalog operations on an existing connection or compose
     * multiple steps that must share a transaction.
     */
    public static void runDdl(@Nonnull final RelationalDriver driver,
                              @Nonnull final Options connectionOptions,
                              @Nonnull final String... statements) throws SQLException {
        runLockedWithRetry(() -> {
            try (Connection connection = driver.connect(SYS_CATALOG_URI)) {
                connection.setSchema("CATALOG");
                Utils.setConnectionOptions(connection, connectionOptions);
                try (Statement statement = connection.createStatement()) {
                    for (final String ddl : statements) {
                        statement.executeUpdate(ddl);
                    }
                }
            }
        });
    }

    /**
     * Convenience overload of {@link #runDdl(RelationalDriver, Options, String...)} with
     * {@link Options#NONE}.
     */
    public static void runDdl(@Nonnull final RelationalDriver driver,
                              @Nonnull final String... statements) throws SQLException {
        runDdl(driver, Options.NONE, statements);
    }

    /**
     * Runs {@code action} on an open connection to {@code /__SYS/CATALOG} under the JVM-wide
     * catalog lock with retry. Use this for tests that interleave DDL and queries against the
     * catalog and need to share a single connection across both.
     * <p>
     * Because the whole body runs under the lock + retry, the {@code action} should be
     * idempotent on retry — typically achieved by using {@code DROP ... IF EXISTS} for cleanup
     * and choosing test data names that don't collide across reruns. If the {@code action}
     * throws a non-retriable {@link SQLException}, it propagates after no retry.
     */
    public static void runOnCatalog(@Nonnull final RelationalDriver driver,
                                    @Nonnull final ThrowingConnectionConsumer action) throws SQLException {
        runLockedWithRetry(() -> {
            try (Connection connection = driver.connect(SYS_CATALOG_URI)) {
                connection.setSchema("CATALOG");
                action.accept(connection);
            }
        });
    }

    /**
     * Functional interface for actions that may throw {@link SQLException}, since most
     * catalog-mutating DDL runs through JDBC.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws SQLException;
    }

    /**
     * Functional interface for catalog actions that need access to the open
     * {@link Connection}. Used by {@link #runOnCatalog} for tests that interleave DDL with
     * queries against the catalog.
     */
    @FunctionalInterface
    public interface ThrowingConnectionConsumer {
        void accept(@Nonnull Connection connection) throws SQLException;
    }

    /**
     * Functional interface for catalog actions that may throw {@link RelationalException}
     * (used by extensions that talk to the catalog via the lower-level Transaction API rather
     * than JDBC, e.g. {@code EmbeddedRelationalExtension.makeDatabase}).
     */
    @FunctionalInterface
    public interface RelationalThrowingRunnable {
        void run() throws RelationalException;
    }

    /**
     * Runs {@code action} under the JVM-wide catalog lock with a small retry loop on SQLSTATE
     * 40001 transaction conflicts. Use for any CREATE/DROP DATABASE, CREATE/DROP SCHEMA TEMPLATE,
     * or CREATE/DROP SCHEMA executed against {@code /__SYS/CATALOG}.
     *
     * @param action the catalog DDL to run
     * @throws SQLException if the action ultimately fails (after retries, or with a
     *                      non-retriable error)
     */
    public static void runLockedWithRetry(@Nonnull final ThrowingRunnable action) throws SQLException {
        synchronized (CATALOG_LOCK) {
            for (int attempt = 1; ; attempt++) {
                try {
                    action.run();
                    return;
                } catch (SQLException e) {
                    // TODO these retries were added when other code didn't pass through here, can it be removed now
                    if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                        throw e;
                    }
                    sleepBeforeRetry(attempt, e);
                }
            }
        }
    }

    /**
     * {@link RelationalException}-throwing variant of {@link #runLockedWithRetry(ThrowingRunnable)}.
     * Same lock and retry policy; detects retriable failures via either {@link SQLException}
     * SQLSTATE 40001 or {@link RelationalException} carrying {@link ErrorCode#SERIALIZATION_FAILURE},
     * plus {@link RecordStoreDoesNotExistException}.
     * Named distinctly because Java can't disambiguate lambdas between the two
     * functional-interface overloads (their abstract methods differ only in their throws clause).
     *
     * @param action the catalog operation to run
     * @throws RelationalException if the action ultimately fails (after retries, or with a
     *                             non-retriable error)
     */
    public static void runLockedWithRelationalRetry(@Nonnull final RelationalThrowingRunnable action) throws RelationalException {
        synchronized (CATALOG_LOCK) {
            for (int attempt = 1; ; attempt++) {
                try {
                    action.run();
                    return;
                } catch (RelationalException e) {
                    if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                        throw e;
                    }
                    sleepBeforeRetry(attempt, e);
                }
            }
        }
    }

    private static <E extends Exception> void sleepBeforeRetry(final int attempt, @Nonnull final E pending) throws E {
        try {
            Thread.sleep(10L * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw pending;
        }
    }

    /**
     * Decides whether a thrown exception is worth retrying. We retry on:
     * <ul>
     *   <li>SQLSTATE 40001 / {@link ErrorCode#SERIALIZATION_FAILURE} — FDB transaction conflicts
     *       (commits that lost the optimistic-concurrency race against a sibling test).
     *   <li>{@link RecordStoreDoesNotExistException} — a transient visibility race during the
     *       first concurrent extension setup, where one thread's bootstrap commit hasn't fully
     *       propagated by the time another thread connects to {@code /__SYS}. The committed
     *       state catches up within a few milliseconds, so the small retry loop covers it.
     * </ul>
     * Walks the exception causal chain so wrapped variants (e.g. {@code ContextualSQLException}
     * wrapping a {@code RelationalException} wrapping a {@code RecordCoreException}) are caught.
     */
    private static boolean isRetriable(@Nonnull final Throwable t) {
        Throwable cursor = t;
        while (cursor != null) {
            if (cursor instanceof SQLException
                    && ErrorCode.SERIALIZATION_FAILURE.getErrorCode().equals(((SQLException) cursor).getSQLState())) {
                return true;
            }
            if (cursor instanceof RelationalException
                    && ((RelationalException) cursor).getErrorCode() == ErrorCode.SERIALIZATION_FAILURE) {
                return true;
            }
            if (cursor instanceof RecordStoreDoesNotExistException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }
}
