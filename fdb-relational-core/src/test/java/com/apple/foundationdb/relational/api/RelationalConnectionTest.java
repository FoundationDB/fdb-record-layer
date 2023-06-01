/*
 * RelationalConnectionTest.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

class RelationalConnectionTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void wrongScheme() {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("foo"), Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PATH);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("foo:foo"), Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PATH);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:foo"), Options.NONE))
                .hasErrorCode(ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed"), Options.NONE))
                .hasErrorCode(ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:/i_am_not_a_database"), Options.NONE))
                .hasErrorCode(ErrorCode.UNDEFINED_DATABASE);
    }

    @Test
    void missingLeadingSlash() {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:i_am_not_a_database"), Options.NONE))
                .hasErrorCode(ErrorCode.UNDEFINED_DATABASE)
                .containsInMessage("<i_am_not_a_database>")
                .doesNotContainInMessage("<null>")
        ;

    }

    @Test
    void setWrongSchema() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            RelationalAssertions.assertThrowsSqlException(() -> conn.setSchema("foo"))
                    .hasErrorCode(ErrorCode.UNDEFINED_SCHEMA);
        }
    }

    @Test
    void canConnectDirectlyToSchema() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS?schema=CATALOG"), Options.NONE)) {
            Assertions.assertThat(conn.getSchema()).isEqualTo("CATALOG");
        }
    }

    @Test
    void connectDirectlyToNonexistentDatabaseBlowsUp() throws RelationalException, SQLException {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:/notADatabase?schema=CATALOG"), Options.NONE))
                .hasErrorCode(ErrorCode.UNDEFINED_DATABASE);
    }

    @Test
    void connectDirectlyToNonexistentSchemaBlowsUp() throws RelationalException, SQLException {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:/__SYS?schema=noSuchSchema"), Options.NONE))
                .hasErrorCode(ErrorCode.UNDEFINED_SCHEMA);
    }

    @Test
    void setIsolationLevel() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            // Default isolation level
            Assertions.assertThat(conn.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            Assertions.assertThat(conn.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            Assertions.assertThat(conn.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
        }
    }
}
