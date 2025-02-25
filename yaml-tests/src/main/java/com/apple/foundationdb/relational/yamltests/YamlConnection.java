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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;

/**
 * A wrapper around {@link java.sql.Connection} to support yaml tests.
 */
public interface YamlConnection extends AutoCloseable {
    /**
     * String for representing the current version of the code.
     */
    String CURRENT_VERSION = "!currentVersion";

    /**
     * Close this connection.
     * @throws SQLException if there was an issue closing the underlying connection(s).
     */
    @Override
    void close() throws SQLException;

    /**
     * Creates a statement (see {@link RelationalConnection#createStatement()}).
     * @return a new statement
     * @throws SQLException if something goes wrong
     */
    RelationalStatement createStatement() throws SQLException;

    /**
     * Creates a prepared statement (see {@link RelationalConnection#prepareStatement(String)}).
     * @param sql the query
     * @return a new prepared statement
     * @throws SQLException if something goes wrong
     */
    RelationalPreparedStatement prepareStatement(String sql) throws SQLException;

    /**
     * Returns true if this connection supports getting the metrics collector.
     * @return true if it is possible to get the metrics collector
     */
    boolean supportsMetricCollector();

    /**
     * Return the underlying metrics collector if possible.
     *
     * @return the underlying metrics collector
     */
    @Nullable
    MetricCollector getMetricCollector();

    /**
     * Try to get the underlying embedded relational connection in support of a few specific setup methods.
     * @return the underlying embedded connection, or {@code null} if one is not (easily) available.
     */
    @Nullable
    EmbeddedRelationalConnection tryGetEmbedded();

    /**
     * Return the ordered list of versions that this will test against.
     * <p>
     *     This does not reset as {@link #createStatement}/{@link #prepareStatement}, etc. are called.
     * </p>
     * @return the ordered list of versions
     */
    @Nonnull
    List<String> getVersions();
}
