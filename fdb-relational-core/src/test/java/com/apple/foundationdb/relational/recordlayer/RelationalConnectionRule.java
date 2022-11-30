/*
 * RelationalConnectionRule.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.sql.SQLException;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

public class RelationalConnectionRule implements BeforeEachCallback, AfterEachCallback, RelationalConnection {
    Supplier<URI> connFactory;
    Options options;
    String schema;
    RelationalConnection connection;

    public RelationalConnectionRule(Supplier<URI> dbPathSupplier) {
        this.connFactory = dbPathSupplier;
    }

    public RelationalConnectionRule withOptions(Options options) {
        this.options = options;
        return this;
    }

    public RelationalConnectionRule withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        connection.close();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws RelationalException, SQLException {
        Options opt = options == null ? Options.NONE : options;
        connection = Relational.connect(connFactory.get(), opt);
        connection.beginTransaction();
        if (schema != null) {
            connection.setSchema(schema);
        }
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        connection.rollback();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Nonnull
    @Override
    public RelationalDatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData().unwrap(RelationalDatabaseMetaData.class);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public void beginTransaction() throws SQLException {
        connection.beginTransaction();
    }

    @Override
    @Nonnull
    public Options getOptions() {
        return connection.getOptions();
    }

    @Override
    public void setOption(Options.Name name, Object value) throws SQLException {
        connection.setOption(name, value);
    }

    @Override
    public URI getPath() {
        return connection.getPath();
    }

    public RelationalConnection getUnderlying() {
        return connection;
    }
}
