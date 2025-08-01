/*
 * OptionScopeTest.java
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
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OptionScopeTest {

    private static final String INSERT_QUERY = "INSERT INTO BOOKS VALUES (1, 'Iliad', -750)";
    private static final String INSERT_QUERY_DRY_RUN = "INSERT INTO BOOKS VALUES (1, 'Iliad', -750) OPTIONS(DRY RUN)";
    private static final String SELECT_QUERY = "SELECT COUNT(*) FROM BOOKS";

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
}
