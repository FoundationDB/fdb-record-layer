/*
 * BenchmarkConnHolder.java
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

package com.apple.foundationdb.relational.recordlayer;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Shared JMH thread-scoped connection holder for relational benchmarks.
 * Subclasses supply the database URI and schema; this class manages the connection lifecycle.
 */
abstract class BenchmarkConnHolder {

    protected final URI dbUri;
    protected final String schema;
    Connection connection;

    protected BenchmarkConnHolder(URI dbUri, String schema) {
        this.dbUri = dbUri;
        this.schema = schema;
    }

    @Setup
    public void init() throws SQLException {
        connection = DriverManager.getConnection("jdbc:embed:" + dbUri);
        connection.setSchema(schema);
    }

    @TearDown
    public void stop() throws SQLException {
        connection.close();
    }
}
