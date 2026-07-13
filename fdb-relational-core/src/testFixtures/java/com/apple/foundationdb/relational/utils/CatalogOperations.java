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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test-only helpers for running catalog-mutating DDL (CREATE/DROP DATABASE, CREATE/DROP SCHEMA
 * TEMPLATE, CREATE/DROP SCHEMA) against {@code /__SYS/CATALOG}.
 * <p>
 * Historically this class also owned a JVM-wide monitor + retry loop that serialised catalog
 * DDL across tests to work around two contention sources:
 * <ol>
 *   <li>Concurrent FRL construction racing on the catalog-init commit (now removed — the
 *       proactive initializer in {@code YamlTestCatalogInitializer} covers that once per test
 *       class before any FRL is constructed).</li>
 *   <li>Every {@code FDBRecordStore.deleteStore} bumping the shared meta-data-version stamp,
 *       which then conflicted with every concurrent catalog read (now conditional on the
 *       deleted store being cacheable, which the SYS catalog is but transient user stores
 *       are not — so most concurrent DDL no longer contends).</li>
 * </ol>
 * With both root causes addressed, the runners here execute their action directly. The
 * method names are kept ({@link #runLockedWithRetry}, {@link #runLockedWithRelationalRetry})
 * so existing test call sites don't need mechanical updates.
 */
public final class CatalogOperations {

    /**
     * URI of the system catalog database every {@link #runDdl} invocation connects against
     * when going through a {@link RelationalDriver}. The {@code jdbc:embed:} scheme is only
     * meaningful for {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver}.
     */
    private static final URI SYS_CATALOG_URI = URI.create("jdbc:embed:/__SYS");

    private CatalogOperations() {
    }

    /**
     * Runs one or more catalog-mutating DDL statements. Each call opens a fresh connection to
     * {@code /__SYS} via the given driver, sets the schema to {@code CATALOG}, applies
     * {@code connectionOptions}, and runs every statement in order.
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param connectionOptions options to apply to the connection before running the statements
     * @param statements DDL statements to run in order
     * @throws SQLException if the operation fails
     */
    public static void runDdl(@Nonnull final RelationalDriver driver,
                              @Nonnull final Options connectionOptions,
                              @Nonnull final String... statements) throws SQLException {
        try (Connection connection = driver.connect(SYS_CATALOG_URI)) {
            connection.setSchema("CATALOG");
            applyConnectionOptions(connection, connectionOptions);
            try (Statement statement = connection.createStatement()) {
                for (final String ddl : statements) {
                    statement.executeUpdate(ddl);
                }
            }
        }
    }

    /**
     * Convenience overload of {@link #runDdl(RelationalDriver, Options, String...)} with
     * {@link Options#NONE}.
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param statements DDL statements to run in order
     * @throws SQLException if the operation fails
     */
    public static void runDdl(@Nonnull final RelationalDriver driver,
                              @Nonnull final String... statements) throws SQLException {
        runDdl(driver, Options.NONE, statements);
    }

    /**
     * Runs {@code action} on an open connection to {@code /__SYS/CATALOG}. Use this for tests
     * that interleave DDL and queries against the catalog and need to share a single connection
     * across both.
     *
     * @param driver the driver to use for the {@code /__SYS} connection
     * @param action the body to run against the open catalog connection
     * @throws SQLException if the action fails
     */
    public static void runOnCatalog(@Nonnull final RelationalDriver driver,
                                    @Nonnull final ThrowingConnectionConsumer action) throws SQLException {
        try (Connection connection = driver.connect(SYS_CATALOG_URI)) {
            connection.setSchema("CATALOG");
            action.accept(connection);
        }
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
     * Runs {@code action}. Historically this method wrapped the action in a JVM-wide
     * monitor + retry loop; that infrastructure has been removed now that the underlying
     * contention (catalog-init commits, meta-data-version-stamp bumps on non-cacheable
     * {@code deleteStore}) has been fixed upstream. The method is preserved so existing test
     * call sites keep compiling; new call sites can skip the wrapper and call their action
     * directly.
     *
     * @param action the catalog DDL to run
     * @throws SQLException if the action fails
     */
    public static void runLockedWithRetry(@Nonnull final ThrowingRunnable action) throws SQLException {
        action.run();
    }

    /**
     * {@link RelationalException}-throwing variant of {@link #runLockedWithRetry(ThrowingRunnable)}.
     * As with that method, no JVM-wide lock or retry is applied any more; the wrapper is kept
     * only to avoid mass-editing every existing call site.
     *
     * @param action the catalog operation to run
     * @throws RelationalException if the action fails
     */
    public static void runLockedWithRelationalRetry(@Nonnull final RelationalThrowingRunnable action) throws RelationalException {
        action.run();
    }
}
