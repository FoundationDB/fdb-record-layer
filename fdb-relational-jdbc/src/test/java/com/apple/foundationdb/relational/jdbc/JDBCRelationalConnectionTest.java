/*
 * JDBCRelationalConnectionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

class JDBCRelationalConnectionTest {

    private JDBCRelationalConnection connection;

    @BeforeEach
    void setUp() {
        connection = new JDBCRelationalConnection(URI.create("jdbc:relational://localhost/__SYS"),
                Options.NONE);
    }

    @Test
    void onlySerializableConnectionIsAllowed() throws SQLException {
        // Default isolation level
        Assertions.assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Assertions.assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

        RelationalAssertions.assertThrowsSqlException(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
        Assertions.assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);
    }
}
