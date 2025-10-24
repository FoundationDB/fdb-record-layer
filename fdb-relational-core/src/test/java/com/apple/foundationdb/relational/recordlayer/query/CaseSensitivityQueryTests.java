/*
 * CaseSensitivityQueryTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;

public class CaseSensitivityQueryTests {

    public CaseSensitivityQueryTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void caseSensitiveConnectionTest() throws Exception {
        final String schemaTemplate = "create type as struct LoCaTiOn (address string, latitude string, longitude string)" +
                " create table ReStAuRaNt(rest_no bigint, name string, location LoCaTiOn, primary key(rest_no))";
        try (var extensionResource = EmbeddedRelationalExtension.newAsResource(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
                var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(extensionResource.getUnderlyingExtension())
                        .withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true)
                        .schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into ReStAuRaNt values (1, 'bla', ('addr', 'a', 'b'))");
                Assertions.assertTrue(statement.execute("select * from ReStAuRaNt"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    verifyResultSet(resultSet);
                }
            }

            var connection = ddl.getConnection();
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from restaurant"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"RESTAURANT\"");
            }

            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from restaurant"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"restaurant\"");
            }
        }
    }

    @Test
    void caseSensitiveConnectionTestCase2() throws Exception {
        final String schemaTemplate = "create type as struct LoCaTiOn (address string, latitude string, longitude string)" +
                " create table ReStAuRaNt(rest_no bigint, name string, location LoCaTiOn, primary key(rest_no))";
        try (var extensionResource = EmbeddedRelationalExtension.newAsResource(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false).build());
                 var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(extensionResource.getUnderlyingExtension())
                         .withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false)
                         .schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into RESTAURANT values (1, 'bla', ('addr', 'a', 'b'))");
                Assertions.assertTrue(statement.execute("select * from restaurant"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    verifyResultSet(resultSet);
                }
            }

            var connection = ddl.getConnection();
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from \"restaurant\""))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"restaurant\"");
            }

            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from ResTaurant"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"ResTaurant\"");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void caseSensitiveConnectionTestCase3(boolean isCaseSensitive) throws Exception {
        final String schemaTemplate = "create type as struct \"Location\" (address string, latitude string, longitude string)" +
                " create table \"Restaurant\"(rest_no bigint, name string, location \"Location\", primary key(rest_no))";
        try (var extensionResource = EmbeddedRelationalExtension.newAsResource(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, isCaseSensitive).build());
                var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(extensionResource.getUnderlyingExtension())
                        .withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, isCaseSensitive)
                        .schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into \"Restaurant\" values (1, 'bla', ('addr', 'a', 'b'))");
                Assertions.assertTrue(statement.execute("select * from \"Restaurant\""), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    verifyResultSet(resultSet);
                }
            }

            var connection = ddl.getConnection();
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from restaurant"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"RESTAURANT\"");
            }

            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from restaurant"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table \"restaurant\"");
            }
        }
    }

    private static void verifyResultSet(@Nonnull final RelationalResultSet resultSet) throws SQLException {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1, resultSet.getLong(1));
        Assertions.assertEquals("bla", resultSet.getString(2));
        final var struct = resultSet.getStruct(3);
        Assertions.assertEquals("addr", struct.getString(1));
        Assertions.assertEquals("a", struct.getString(2));
        Assertions.assertEquals("b", struct.getString(3));
    }
}
