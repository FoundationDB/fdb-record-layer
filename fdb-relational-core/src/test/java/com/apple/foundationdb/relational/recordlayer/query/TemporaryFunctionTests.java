/*
 * TemporaryFunctionTests.java
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

import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * This is for testing different aspects of temporary SQL functions. This test suite can migrate to YAML once we have
 *  <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3366">support for multi-statement transactions in YAML</a>.
 */
public class TemporaryFunctionTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void createTemporaryFunctionWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void createTemporaryFunctionAcrossTransactionsWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.commit();
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void temporaryFunctionVisibilityAcrossTransactions() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT"))
                .relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.commit();
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> invokeAndVerifyTempFunction(statement))
                        .hasErrorCode(ErrorCode.UNDEFINED_FUNCTION);
            }
            connection.rollback();
        }
    }

    @Test
    void createOrReplaceTemporaryFunctionWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 60 + x ");
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void createTemporaryFunctionSameNameThrows() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute(
                        "create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x "))
                        .hasErrorCode(ErrorCode.DUPLICATE_FUNCTION);
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void createTemporaryFunctionWithPreparedParameters() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < ? + x ")) {
                statement.setLong(1, 40L);
                statement.executeUpdate();
            }
            try (var statement = connection.prepareStatement("select * from sq1(x => ?)")) {
                statement.setLong(1, 2L);
                try (var resultSet = statement.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 10L)
                            .hasNextRow()
                            .isRowExactly(2L, 20L)
                            .hasNextRow()
                            .isRowExactly(3L, 30L)
                            .hasNextRow()
                            .isRowExactly(4L, 40L)
                            .hasNoNextRow();
                }
            }
            connection.rollback();
        }
    }

    @Test
    void createNestedTemporaryFunctionWithPreparedParameters() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) on commit drop function as select * from t1 where a < ? + x ")) {
                statement.setLong(1, 40L);
                statement.executeUpdate();
            }

            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from sq0(x) ");
            }
            try (var statement = connection.prepareStatement("select * from sq1(x => ?)")) {
                statement.setLong(1, 2L);
                try (var resultSet = statement.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 10L)
                            .hasNextRow()
                            .isRowExactly(2L, 20L)
                            .hasNextRow()
                            .isRowExactly(3L, 30L)
                            .hasNextRow()
                            .isRowExactly(4L, 40L)
                            .hasNoNextRow();
                }
            }
            connection.rollback();
        }
    }

    @Test
    void createTemporaryFunctionWithPreparedParametersWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < ?param + x")) {
                statement.setLong("param", 40);
                statement.execute();
            }
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                invokeAndVerifyTempFunction(statement);
            }
            invokeAndVerifyTempFunction(sql -> {
                try {
                    return ddl.setSchemaAndGetConnection().prepareStatement(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            connection.rollback();
        }
    }

    private static void invokeAndVerifyTempFunction(final RelationalStatement statement) throws SQLException {
        Assertions.assertTrue(statement.execute("select * from sq1(x => 2)"));
        try (var resultSet = statement.getResultSet()) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly(1L, 10L)
                    .hasNextRow()
                    .isRowExactly(2L, 20L)
                    .hasNextRow()
                    .isRowExactly(3L, 30L)
                    .hasNextRow()
                    .isRowExactly(4L, 40L)
                    .hasNoNextRow();
        }
    }

    private static void invokeAndVerifyTempFunction(final Function<String, RelationalPreparedStatement> preparedStatementFunction) throws SQLException {
        try (var preparedStatement = preparedStatementFunction.apply("select * from sq1(x => 2) where a > ?param2")) {
            preparedStatement.setLong("param2", 25L);
            try (var resultSet = preparedStatement.executeQuery()) {
                ResultSetAssert.assertThat(resultSet).hasNextRow()
                        .isRowExactly(3L, 30L)
                        .hasNextRow()
                        .isRowExactly(4L, 40L)
                        .hasNoNextRow();
            }
        }
    }
}
