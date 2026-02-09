/*
 * RelationalConnectionRule.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.SqlSupplier;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.function.Supplier;

public class RelationalConnectionRule implements BeforeEachCallback, AfterEachCallback, RelationalConnection {
    private final Supplier<URI> dbPathSupplier;
    private final SqlSupplier<RelationalDriver> driverSupplier;
    Options options;
    String schema;
    RelationalConnection connection;


    public RelationalConnectionRule(SqlSupplier<RelationalDriver> driverSupplier, Supplier<URI> dbPathSupplier) {
        this.driverSupplier = driverSupplier;
        this.dbPathSupplier = dbPathSupplier;
    }

    public RelationalConnectionRule(Supplier<URI> dbPathSupplier) {
        this(() -> (RelationalDriver) DriverManager.getDriver(dbPathSupplier.get().toString()),
                dbPathSupplier);
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
        final RelationalDriver driver = driverSupplier.get();
        connection = driver.connect(dbPathSupplier.get(), opt);
        if (schema != null) {
            connection.setSchema(schema);
        }
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
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
    public void setTransactionIsolation(int level) throws SQLException {
        connection.setTransactionIsolation(level);

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Nonnull
    @Override
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

    /**
     * Returns the underlying {@link EmbeddedRelationalConnection}, which is currently the only connection
     * type we run our JUnit tests against.
     * @return The underlying {@link EmbeddedRelationalConnection} connection.
     */
    @Nonnull
    public EmbeddedRelationalConnection getUnderlyingEmbeddedConnection() {
        return Assert.castUnchecked(connection, EmbeddedRelationalConnection.class);
    }
}
