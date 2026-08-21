/*
 * OptionScopeTest.java
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
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.test.BooleanSource;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OptionScopeTest {

    private static final String INSERT_QUERY = "INSERT INTO BOOKS VALUES (1, 'Iliad', -750)";
    private static final String INSERT_QUERY_DRY_RUN = "INSERT INTO BOOKS VALUES (1, 'Iliad', -750) OPTIONS(DRY RUN)";
    private static final String SELECT_QUERY = "SELECT COUNT(*) FROM BOOKS";
    /** A range scan over the seeded rows, deliberately carrying no {@code OPTIONS} clause. */
    private static final String BOOKS_SCAN_QUERY = "SELECT id FROM BOOKS WHERE id < 1000";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(UniqueIndexTests.class, TestSchemas.books());

    @Test
    public void optionTakenFromConnection() throws SQLException, RelationalException {
        final var driver = (RelationalDriver) DriverManager.getDriver(db.getConnectionUri().toString());
        try (Connection conn = driver.connect(db.getConnectionUri(), Options.builder().withOption(Options.Name.DRY_RUN, true).build())) {
            conn.setSchema(db.getSchemaName());
            try (Statement statement = conn.createStatement()) {
                Assertions.assertThat(statement.executeUpdate(INSERT_QUERY)).isOne();
                try (ResultSet rs = statement.executeQuery(SELECT_QUERY)) {
                    ResultSetAssert.assertThat((RelationalResultSet) rs).hasNextRow().isRowExactly(0L);
                }
            }
        }
    }

    @Test
    public void optionTakenFromQuery() throws SQLException {
        try (Connection conn = DriverManager.getConnection(db.getConnectionUri().toString())) {
            conn.setSchema(db.getSchemaName());
            try (Statement statement = conn.createStatement()) {
                Assertions.assertThat(statement.executeUpdate(INSERT_QUERY_DRY_RUN)).isOne();
                try (ResultSet rs = statement.executeQuery(SELECT_QUERY)) {
                    ResultSetAssert.assertThat((RelationalResultSet) rs).hasNextRow().isRowExactly(0L);
                }
            }
        }
    }

    @Test
    public void optionSetInConnectionButOverriddenInQuery() throws SQLException, RelationalException {
        final var driver = (RelationalDriver) DriverManager.getDriver(db.getConnectionUri().toString());
        try (Connection conn = driver.connect(db.getConnectionUri(), Options.builder().withOption(Options.Name.DRY_RUN, false).build())) {
            conn.setSchema(db.getSchemaName());
            try (Statement statement = conn.createStatement()) {
                Assertions.assertThat(statement.executeUpdate(INSERT_QUERY_DRY_RUN)).isOne();
                try (ResultSet rs = statement.executeQuery(SELECT_QUERY)) {
                    ResultSetAssert.assertThat((RelationalResultSet) rs).hasNextRow().isRowExactly(0L);
                }
            }
        }
    }

    /**
     * {@code ISOLATION_LEVEL_SNAPSHOT} is supported as a connection option: a {@code SELECT} on the connection
     * runs (at snapshot isolation) without needing a per-query {@code OPTIONS} clause. This lets a user
     * set the option on the connection, run a few reads, and switch back. The reader has to clear the option before it
     * can write at all, because a connection with the option set rejects DML (see
     * {@link #snapshotIsolationConnectionOptionRejectsDml}).
     */
    @ParameterizedTest
    @BooleanSource("snapshotOnConnection")
    void snapshotIsolationTakenFromConnection(boolean snapshotOnConnection) throws SQLException {
        final var driver = (RelationalDriver) DriverManager.getDriver(db.getConnectionUri().toString());

        // Seed the rows the reader will scan.
        try (RelationalConnection setup = driver.connect(db.getConnectionUri(), Options.NONE)) {
            setup.setSchema(db.getSchemaName());
            try (Statement statement = setup.createStatement()) {
                statement.executeUpdate("INSERT INTO BOOKS VALUES (1, 'Iliad', 1000), (2, 'Odyssey', 1000), (4, 'Aeneid', 1000)");
            }
        }

        final var readerOptions = snapshotOnConnection
                ? Options.builder().withOption(Options.Name.ISOLATION_LEVEL_SNAPSHOT, true).build()
                : Options.NONE;
        try (RelationalConnection reader = driver.connect(db.getConnectionUri(), readerOptions);
                RelationalConnection writer = driver.connect(db.getConnectionUri(), Options.NONE)) {
            reader.setSchema(db.getSchemaName());
            writer.setSchema(db.getSchemaName());
            reader.setAutoCommit(false);
            writer.setAutoCommit(false);

            // Note the absence of an OPTIONS clause: the isolation level can only have come from the connection.
            try (Statement statement = reader.createStatement();
                    ResultSet rs = statement.executeQuery(BOOKS_SCAN_QUERY)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                Assertions.assertThat(rowCount).isEqualTo(3);
            }

            // Concurrent insert into the range the reader just scanned.
            try (Statement statement = writer.createStatement()) {
                statement.executeUpdate("INSERT INTO BOOKS VALUES (3, 'Metamorphoses', 1000)");
            }
            writer.commit();

            // Clear the option so the reader may write, making it a read-write transaction that can conflict.
            reader.setOption(Options.Name.ISOLATION_LEVEL_SNAPSHOT, false);
            try (Statement statement = reader.createStatement()) {
                statement.executeUpdate("INSERT INTO BOOKS VALUES (1000, 'Reader', 1000)");
            }
            if (snapshotOnConnection) {
                reader.commit();
            } else {
                RelationalAssertions.assertThrowsSqlException(reader::commit)
                        .hasErrorCode(ErrorCode.SERIALIZATION_FAILURE);
            }
        }
    }

    /**
     * Because {@code ISOLATION_LEVEL_SNAPSHOT} is rejected on mutations, setting it as a connection option makes
     * every non-{@code SELECT} statement on that connection fail: the option applies to the {@code INSERT}
     * just as it would to a {@code SELECT}, and mutations cannot run at snapshot isolation. A connection
     * with the option set is therefore effectively read-only until the option is cleared.
     */
    @Test
    void snapshotIsolationConnectionOptionRejectsDml() throws SQLException {
        final var driver = (RelationalDriver) DriverManager.getDriver(db.getConnectionUri().toString());
        try (Connection conn = driver.connect(db.getConnectionUri(), Options.builder().withOption(Options.Name.ISOLATION_LEVEL_SNAPSHOT, true).build())) {
            conn.setSchema(db.getSchemaName());
            try (Statement statement = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(INSERT_QUERY))
                        .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
            }
        }
    }
}
