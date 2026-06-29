/*
 * ConnectionUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;

/** Utility for interacting with easily connections/statements. */
public final class ConnectionUtils {
    public static final String SYS_DATABASE = "/__SYS";
    public static final String CATALOG_SCHEMA = "CATALOG";
    @Nonnull
    private final SqlFunction<String, RelationalConnection> getConnection;

    public ConnectionUtils(@Nonnull RelationalDriver driver) {
        getConnection = databaseName -> driver.connect(URI.create("jdbc:embed:" + databaseName)).unwrap(RelationalConnection.class);
    }

    public void runAgainstCatalog(@Nonnull final SQLConsumer<RelationalConnection> action) throws SQLException, RelationalException {
        // Catalog operations go through the JVM-wide catalog lock + retry. See {@link CatalogOperations}.
        CatalogOperations.runLockedWithRetry(() -> {
            try {
                runAgainstConnection(SYS_DATABASE, CATALOG_SCHEMA, action);
            } catch (RelationalException e) {
                // CatalogOperations.runLockedWithRetry only accepts SQLException; surface this as one.
                throw e.toSqlException();
            }
        });
    }

    @Nullable
    public <R> R getFromCatalog(@Nonnull final SQLFunction<RelationalConnection, R> action) throws SQLException, RelationalException {
        // Read-only catalog access — wrap in the catalog lock anyway since concurrent reads alongside
        // sibling writes can race on the FDB read-version protocol under load.
        @SuppressWarnings("unchecked")
        final R[] result = (R[]) new Object[1];
        CatalogOperations.runLockedWithRetry(() -> {
            try {
                result[0] = getFromConnection(SYS_DATABASE, CATALOG_SCHEMA, action);
            } catch (RelationalException e) {
                throw e.toSqlException();
            }
        });
        return result[0];
    }

    @Nullable
    public <R> R getFromConnection(@Nonnull final String databaseName,
                                   @Nonnull final String schemaName,
                                   @Nonnull final SQLFunction<RelationalConnection, R> action) throws SQLException, RelationalException {
        try (RelationalConnection conn = getConnection.apply(databaseName).unwrap(RelationalConnection.class)) {
            conn.setSchema(schemaName);
            return action.apply(conn);
        }
    }

    public void runAgainstConnection(@Nonnull final String databaseName,
                                     @Nonnull final String schemaName,
                                     @Nonnull final SQLConsumer<RelationalConnection> action) throws SQLException, RelationalException {
        getFromConnection(databaseName, schemaName, conn -> {
            action.accept(conn);
            return null;
        });
    }

    public void runCatalogStatement(@Nonnull final SQLConsumer<RelationalStatement> action) throws SQLException, RelationalException {
        // Catalog DDL — wrap in {@link CatalogOperations#runLockedWithRetry} so all CREATE/DROP
        // DATABASE/SCHEMA TEMPLATE/SCHEMA operations across the test suite are serialised on a
        // JVM-wide lock and transparently retried on transient races (SQLSTATE 40001, etc.).
        CatalogOperations.runLockedWithRetry(() -> {
            try {
                runStatement(SYS_DATABASE, CATALOG_SCHEMA, action);
            } catch (RelationalException e) {
                throw e.toSqlException();
            }
        });
    }

    public void runStatement(@Nonnull final String databaseName,
                             @Nonnull final String schemaName,
                             @Nonnull final SQLConsumer<RelationalStatement> action) throws SQLException, RelationalException {
        runAgainstConnection(databaseName, schemaName, conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                action.accept(stmt);
            }
        });
    }

    public void runStatementUpdate(@Nonnull final String databaseName,
                                   @Nonnull String schemaName,
                                   @Nonnull String statement) throws SQLException, RelationalException {
        runStatement(databaseName, schemaName, stmt -> stmt.execute(statement));
    }

    public interface SQLConsumer<T> {
        void accept(T t) throws SQLException, RelationalException;
    }

    public interface SQLFunction<T, R> {
        @Nullable
        R apply(T t) throws SQLException, RelationalException;
    }
}
