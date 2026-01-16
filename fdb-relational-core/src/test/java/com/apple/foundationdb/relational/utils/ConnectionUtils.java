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
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Utility for interacting with easily connections/statements. */
public final class ConnectionUtils {
    public static final String SYS_DATABASE = "/__SYS";
    public static final String CATALOG_SCHEMA = "CATALOG";

    private ConnectionUtils() {
    }

    public static void runAgainstCatalog(@Nonnull final SQLConsumer<RelationalConnection> action) throws SQLException, RelationalException {
        runAgainstConnection(SYS_DATABASE, CATALOG_SCHEMA, action);
    }

    @Nullable
    public static <R> R getFromCatalog(@Nonnull final SQLFunction<RelationalConnection, R> action) throws SQLException, RelationalException {
        return getFromConnection(SYS_DATABASE, CATALOG_SCHEMA, action);
    }

    @Nullable
    public static <R> R getFromConnection(@Nonnull final String databaseName,
                                          @Nonnull final String schemaName,
                                          @Nonnull final SQLFunction<RelationalConnection, R> action) throws SQLException, RelationalException {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:" + databaseName).unwrap(RelationalConnection.class)) {
            conn.setSchema(schemaName);
            return action.apply(conn);
        }
    }

    public static void runAgainstConnection(@Nonnull final String databaseName,
                                            @Nonnull final String schemaName,
                                            @Nonnull final SQLConsumer<RelationalConnection> action) throws SQLException, RelationalException {
        getFromConnection(databaseName, schemaName, conn -> {
            action.accept(conn);
            return null;
        });
    }

    public static void runCatalogStatement(@Nonnull final SQLConsumer<RelationalStatement> action) throws SQLException, RelationalException {
        runStatement(SYS_DATABASE, CATALOG_SCHEMA, action);
    }

    public static void runStatement(@Nonnull final String databaseName,
                                    @Nonnull final String schemaName,
                                    @Nonnull final SQLConsumer<RelationalStatement> action) throws SQLException, RelationalException {
        runAgainstConnection(databaseName, schemaName, conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                action.accept(stmt);
            }
        });
    }

    public static void runStatementUpdate(@Nonnull final String databaseName,
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
