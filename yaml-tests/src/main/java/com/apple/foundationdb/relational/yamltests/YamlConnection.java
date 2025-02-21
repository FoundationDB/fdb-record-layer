/*
 * YamlConnection.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * A wrapper around {@link java.sql.Connection} to support yaml tests.
 */
public class YamlConnection implements AutoCloseable {
    RelationalConnection underlying;

    public YamlConnection(final Connection connection) throws SQLException {
        underlying = connection.unwrap(RelationalConnection.class);
    }

    @Override
    public void close() throws SQLException {
        underlying.close();
    }

    public boolean supportsCacheCheck() {
        return underlying instanceof EmbeddedRelationalConnection;
    }

    public RelationalStatement createStatement() throws SQLException {
        return underlying.createStatement();
    }

    public RelationalPreparedStatement prepareStatement(final String sql) throws SQLException {
        return underlying.prepareStatement(sql);
    }

    public RelationalConnection getUnderlying() {
        return underlying;
    }

    public MetricCollector getMetricCollector() {
        return ((EmbeddedRelationalConnection) underlying).getMetricCollector();
    }
    //
}
