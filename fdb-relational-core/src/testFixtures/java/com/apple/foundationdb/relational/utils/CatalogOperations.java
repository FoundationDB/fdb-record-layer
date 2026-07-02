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
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

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
 * extension setup, a {@code SchemaRule}'s {@code @BeforeEach} can race the very first call to
 * {@code /__SYS} and observe the record store before another thread's bootstrap has fully
 * propagated. The retry covers that window.
 * <p>
 * Lives in {@code testFixtures} so downstream modules (fdb-relational-jdbc, etc.) can serialize
 * their catalog DDL against the same JVM-wide monitor.
 * <p>
 * Wrap CREATE/DROP DDL in {@code @BeforeEach}/{@code @AfterEach} hooks with this helper. Drop
 * statements should use {@code DROP ... IF EXISTS} so a retry after a partial success doesn't
 * trip an "unknown ..." error.
 */
public final class CatalogOperations {

    /**
     * JVM-wide monitor guarding catalog-mutating DDL issued from test rules. FRL construction
     * in fdb-relational-server has its own separate {@code FRL.catalogLock()} and is also
     * retry-guarded — a separate lock is fine because FRL bootstrap is a one-time-per-JVM step
     * that runs before any tests contend for this monitor.
     */
    private static final Object CATALOG_LOCK = new Object();

    private static final int MAX_ATTEMPTS = 5;

    /**
     * URI of the system catalog database every {@link #runDdl} invocation connects against
     * when going through a {@link RelationalDriver}. The {@code jdbc:embed:} scheme is only
     * meaningful for {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver}.
     */
    private static final URI SYS_CATALOG_URI = URI.create("jdbc:embed:/__SYS");

    private CatalogOperations() {
    }

    /**
     * Runs one or more catalog-mutating DDL statements under the JVM-wide catalog lock with
     * retry on transient races. Each call opens a fresh connection to {@code /__SYS} via the
     * given driver, sets the schema to {@code CATALOG}, applies {@code connectionOptions}, and
     * runs every statement in order. Drop statements should use {@code DROP ... IF EXISTS} so
     * a retry after a partial success doesn't trip an "unknown ..." error.
     * <p>
     * This is the preferred entry point for tests that already hold a {@link RelationalDriver}
     * reference (e.g. via {@code EmbeddedRelationalExtension.getDriver()}) — prefer it over
     * calling {@link #runLockedWithRetry(ThrowingRunnable)} directly, which only exists for
     * callers that need to compose multiple steps on an existing connection.
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param connectionOptions options to apply to the connection before running the statements
     * @param statements DDL statements to run in order
     * @throws SQLException if the operation ultimately fails (after retries, or with a
     *                      non-retriable error)
     */
    public static void runDdl(@Nonnull final RelationalDriver driver,
                              @Nonnull final Options connectionOptions,
                              @Nonnull final String... statements) throws SQLException {
        runLockedWithRetry(() -> {
            try (Connection connection = driver.connect(SYS_CATALOG_URI)) {
                connection.setSchema("CATALOG");
                applyConnectionOptions(connection, connectionOptions);
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
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param statements DDL statements to run in order
     * @throws SQLException if the operation ultimately fails
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
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param action the body to run against the open catalog connection
     * @throws SQLException if the action ultimately fails
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
     * Applies each entry of {@code options} to {@code connection} via
     * {@link RelationalConnection#setOption(Options.Name, Object)}. Inlined here (rather than
     * imported from a test-source utility) so that this class can live in {@code testFixtures}
     * and be consumed from downstream modules.
     *
     * @param connection the connection to configure
     * @param options options to set (may be {@link Options#NONE} for a no-op)
     * @throws SQLException if any {@code setOption} call fails
     */
    private static void applyConnectionOptions(@Nonnull final Connection connection,
                                               @Nonnull final Options options) throws SQLException {
        final RelationalConnection relational = connection.unwrap(RelationalConnection.class);
        for (final var entry : options.entries()) {
            relational.setOption(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Functional interface for actions that may throw {@link SQLException}, since most
     * catalog-mutating DDL runs through JDBC.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        /**
         * Runs the catalog action.
         *
         * @throws SQLException if the action fails
         */
        void run() throws SQLException;
    }

    /**
     * Functional interface for catalog actions that need access to the open
     * {@link Connection}. Used by {@link #runOnCatalog} for tests that interleave DDL with
     * queries against the catalog.
     */
    @FunctionalInterface
    public interface ThrowingConnectionConsumer {
        /**
         * Runs the catalog action against the given connection.
         *
         * @param connection the open connection to {@code /__SYS/CATALOG}
         * @throws SQLException if the action fails
         */
        void accept(@Nonnull Connection connection) throws SQLException;
    }

    /**
     * Functional interface for catalog actions that may throw {@link RelationalException}
     * (used by extensions that talk to the catalog via the lower-level Transaction API rather
     * than JDBC, e.g. {@code EmbeddedRelationalExtension.makeDatabase}).
     */
    @FunctionalInterface
    public interface RelationalThrowingRunnable {
        /**
         * Runs the catalog action.
         *
         * @throws RelationalException if the action fails
         */
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
        for (int attempt = 1; ; attempt++) {
            final SQLException failure;
            synchronized (CATALOG_LOCK) {
                try {
                    action.run();
                    return;
                } catch (SQLException e) {
                    // TODO these retries were added when other code didn't pass through here, can it be removed now
                    if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                        throw e;
                    }
                    failure = e;
                }
            }
            // Backoff with the lock RELEASED so other catalog ops can make progress between
            // retries. Sleeping under the monitor triggers SpotBugs SWL_SLEEP_WITH_LOCK_HELD
            // and unnecessarily serialises unrelated work.
            sleepBeforeRetry(attempt, failure);
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
        for (int attempt = 1; ; attempt++) {
            final RelationalException failure;
            synchronized (CATALOG_LOCK) {
                try {
                    action.run();
                    return;
                } catch (RelationalException e) {
                    if (attempt >= MAX_ATTEMPTS || !isRetriable(e)) {
                        throw e;
                    }
                    failure = e;
                }
            }
            sleepBeforeRetry(attempt, failure);
        }
    }

    private static <E extends Exception> void sleepBeforeRetry(final int attempt, @Nonnull final E pending) throws E {
        try {
            Thread.sleep(10L * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            pending.addSuppressed(ie);
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
