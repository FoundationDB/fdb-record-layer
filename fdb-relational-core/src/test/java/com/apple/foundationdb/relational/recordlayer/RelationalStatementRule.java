/*
 * RelationalStatementRule.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;

import com.google.protobuf.Message;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;


public class RelationalStatementRule implements BeforeEachCallback, AfterEachCallback, RelationalStatement {
    RelationalConnection connection;
    RelationalStatement statement;

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

    @Nonnull
    @Override
    public RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
        return statement.executeScan(tableName, prefix, options);
    }

    @Nonnull
    @Override
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws SQLException {
        return statement.executeGet(tableName, key, options);
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException {
        return statement.getDataBuilder(tableName);
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull final String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException {
        return statement.getDataBuilder(maybeQualifiedTableName, nestedFields);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException {
        return statement.executeInsert(tableName, data, options);
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        return statement.executeDelete(tableName, keys);
    }

    @Override
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
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
}
