/*
 * JDBCDatabaseConnection.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Testing tool that implements the Relational API using standard JDBC. Useful for doing comparison testing
 */
@SuppressWarnings("checkstyle:abbreviationsaswordinname")
public class JDBCDatabaseConnection implements RelationalConnection {
    private final RelationalCatalog catalog;

    private final Connection relationalConn;

    public JDBCDatabaseConnection(RelationalCatalog catalog, Connection relationalConn) {
        this.catalog = catalog;
        this.relationalConn = relationalConn;
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return new JDBCStatement(catalog, relationalConn.createStatement());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        relationalConn.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return relationalConn.getAutoCommit();
    }

    @Override
    public void beginTransaction(@Nullable TransactionConfig config) throws RelationalException {
    }

    @Override
    @Nonnull
    public Options getOptions() {
        return Options.none();
    }

    @Override
    public void commit() throws SQLException {
        relationalConn.commit();
    }

    @Override
    public void rollback() throws SQLException {
        relationalConn.rollback();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        try (Statement s = relationalConn.createStatement()) {
            s.execute("SET SCHEMA " + schema);
        }
    }

    @Override
    public String getSchema() throws SQLException {
        return relationalConn.getSchema();
    }

    @Override
    public void close() throws SQLException {
        relationalConn.close();
    }

    @Nonnull
    @Override
    public RelationalDatabaseMetaData getMetaData() throws SQLException {
        return new JDBCDatabaseMetaData(relationalConn.getMetaData());
    }

    @Override
    public boolean isClosed() throws SQLException {
        return relationalConn.isClosed();
    }
}
