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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.sql.SQLException;

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

    private CatalogOperations() {
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
                    if (attempt >= MAX_ATTEMPTS || !isTransactionConflict(e)) {
                        throw e;
                    }
                    sleepBeforeRetry(attempt, e);
                }
            }
        }
    }

    /**
     * {@link RelationalException}-throwing variant of {@link #runLockedWithRetry(ThrowingRunnable)}.
     * Same lock and retry policy; detects conflicts via either {@link SQLException} SQLSTATE
     * 40001 or {@link RelationalException} carrying {@link ErrorCode#SERIALIZATION_FAILURE}.
     * Named distinctly because Java can't disambiguate lambdas between the two
     * functional-interface overloads (their abstract methods differ only in their throws clause).
     */
    public static void runLockedWithRelationalRetry(@Nonnull final RelationalThrowingRunnable action) throws RelationalException {
        synchronized (CATALOG_LOCK) {
            for (int attempt = 1; ; attempt++) {
                try {
                    action.run();
                    return;
                } catch (RelationalException e) {
                    if (attempt >= MAX_ATTEMPTS || !isTransactionConflict(e)) {
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

    private static boolean isTransactionConflict(@Nonnull final Throwable t) {
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
            cursor = cursor.getCause();
        }
        return false;
    }
}
