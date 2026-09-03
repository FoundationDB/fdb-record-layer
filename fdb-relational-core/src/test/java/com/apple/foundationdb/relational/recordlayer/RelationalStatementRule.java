/*
 * RelationalStatementRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class RelationalStatementRule implements BeforeEachCallback, AfterEachCallback, RelationalStatement {
    RelationalConnection connection;
    RelationalStatement statement;

    // statement is set later in beforeEach(), not in this constructor.
    @SuppressWarnings("NullAway.Init")
    public RelationalStatementRule(RelationalConnection connection) {
        this.connection = connection;
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        statement.close();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws SQLException {
        statement = connection.createStatement();
    }

    @Override
    public RelationalResultSet executeScan(String tableName, KeySet prefix, Options options) throws SQLException {
        return statement.executeScan(tableName, prefix, options);
    }

    @Override
    public RelationalResultSet executeGet(String tableName, KeySet key, Options options) throws SQLException {
        return statement.executeGet(tableName, key, options);
    }

    @Override
    public int executeInsert(String tableName, List<RelationalStruct> data, Options options) throws SQLException {
        return statement.executeInsert(tableName, data, options);
    }

    @Override
    public int executeDelete(String tableName, Iterator<KeySet> keys, Options options) throws SQLException {
        return statement.executeDelete(tableName, keys);
    }

    @Override
    public void executeDeleteRange(String tableName, KeySet prefix, Options options) throws SQLException {
        statement.executeDeleteRange(tableName, prefix, options);
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        return statement.executeQuery(sql);
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }
}
