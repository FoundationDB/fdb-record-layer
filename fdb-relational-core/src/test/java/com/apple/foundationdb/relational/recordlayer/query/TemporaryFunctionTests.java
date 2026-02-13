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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.CreateTemporaryFunctionConstantAction;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is for testing different aspects of temporary SQL functions. This test suite can migrate to YAML once we have
 *  <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3366">support for multi-statement transactions in YAML</a>.
 */
public class TemporaryFunctionTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final LogAppenderRule logAppender = new LogAppenderRule("TemporaryFunctionTests", PlanGenerator.class, Level.INFO);

    public TemporaryFunctionTests() {
        Utils.enableCascadesDebugger();
    }

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
    void createTemporaryFunctionWithNameCollisionsThrows() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) " +
                "create function foo() as select * from t1 where a < 43"; // add non-temporary function called foo
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("create temporary function foo(in x bigint) " + // attempt to create function with the same name.
                        "on commit drop function as select * from t1 where a < 40 + x "))
                        .hasErrorCode(ErrorCode.DUPLICATE_FUNCTION);
            }
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("create or replace temporary function foo(in x bigint) " + // attempt to create function with the same name.
                                "on commit drop function as select * from t1 where a < 40 + x "))
                        .hasErrorCode(ErrorCode.INVALID_FUNCTION_DEFINITION);

            }
            connection.rollback();
        }
    }

    @Test
    void temporaryFunctionVisibilityAcrossTransactionAfterCommit() throws Exception {
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
    void temporaryFunctionVisibilityAcrossTransactionsAfterRollback() throws Exception {
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
            connection.rollback();
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
    void createOrReplaceTemporaryFunctionAndInvokeMultipleTimesWorks() throws Exception {
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
                invokeAndVerifyTempFunction(statement);
                invokeAndVerifyTempFunction(statement);
                invokeAndVerifyTempFunction(statement);
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void temporaryFunctionIsMemoizedAcrossInvocations() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // at least 1 function is defined
            try (var statement = connection.createStatement()) {
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");

                final var firstCall = getUserDefinedFunction(connection, "SQ1");
                Assertions.assertInstanceOf(CompiledSqlFunction.class, firstCall);

                // more calls
                Assertions.assertSame(firstCall, getUserDefinedFunction(connection, "SQ1"));
                Assertions.assertSame(firstCall, getUserDefinedFunction(connection, "SQ1"));
                Assertions.assertSame(firstCall, getUserDefinedFunction(connection, "SQ1"));
            }
            connection.rollback();
        }
    }

    private UserDefinedFunction getUserDefinedFunction(@Nonnull RelationalConnection connection, @Nonnull String name) throws RelationalException {
        final var boundSchemaTemplateMaybe = ((EmbeddedRelationalConnection) connection).getTransaction().getBoundSchemaTemplateMaybe();
        Assertions.assertTrue(boundSchemaTemplateMaybe.isPresent());
        final var invokedRoutineMaybe = boundSchemaTemplateMaybe.get().findInvokedRoutineByName(name);
        Assertions.assertTrue(invokedRoutineMaybe.isPresent());
        Assertions.assertTrue(invokedRoutineMaybe.get().isTemporary());
        Assertions.assertInstanceOf(RecordLayerInvokedRoutine.class, invokedRoutineMaybe.get());
        return ((RecordLayerInvokedRoutine) invokedRoutineMaybe.get()).getUserDefinedFunctionProvider().apply(true);
    }

    @Test
    void dropTemporaryFunctionWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // at least 1 function is defined
            try (var statement = connection.createStatement()) {
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq2"))
                        .containsInMessage("Attempt to DROP an undefined temporary function: SQ2");
                statement.execute("drop temporary function sq1");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from sq1(x => 34)"))
                        .containsInMessage("Unknown function SQ1");
            }
            connection.rollback();
            // no temporary functions defined
            try (var statement = connection.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq1"))
                        .containsInMessage("Attempt to DROP an undefined temporary function: SQ1");
            }
            connection.rollback();
        }
    }

    @Test
    void dropNonTemporaryFunctionFails() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) " +
                "create function sq0(in x bigint) as select * from t1 where a > x";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // at least 1 function is defined
            try (var statement = connection.createStatement()) {
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
                statement.execute("drop temporary function sq1");
                // this should fail as sq0 is not temporary in nature
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq0"))
                        .containsInMessage("Attempt to DROP an non-temporary function: SQ0");
            }
            connection.rollback();
        }
    }

    @Test
    void dropTemporaryFunctionIfExistsWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // at least 1 function is defined
            try (var statement = connection.createStatement()) {
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
                statement.execute("drop temporary function if exists sq2");
                statement.execute("drop temporary function if exists sq1");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from sq1(x => 34)"))
                        .containsInMessage("Unknown function SQ1");
            }
            connection.rollback();
            // function is not defined
            try (var statement = connection.createStatement()) {
                statement.execute("drop temporary function if exists sq1");
            }
            connection.rollback();
        }
    }

    @Test
    void dropTemporaryFunctionMultipleCallsWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // at least 1 function is defined
            try (var statement = connection.createStatement()) {
                statement.execute("create or replace temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
                statement.execute("drop temporary function sq1");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq1"))
                        .containsInMessage("Attempt to DROP an undefined temporary function: SQ1");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq1"))
                        .containsInMessage("Attempt to DROP an undefined temporary function: SQ1");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("drop temporary function sq1"))
                        .containsInMessage("Attempt to DROP an undefined temporary function: SQ1");
            }
            connection.rollback();
            // function is not defined
            try (var statement = connection.createStatement()) {
                statement.execute("drop temporary function if exists sq1");
            }
            connection.rollback();
        }
    }

    // This tests defines nested temporary functions and then tries to query the outer function while the inner function
    // is dropped. This execution is expected to fail however it doesn't owing to the following missing bit about
    // understanding the metadata object dependency.
    // See: https://github.com/FoundationDB/fdb-record-layer/issues/3493.
    @Disabled
    @Test
    void dropNestedTemporaryFunctionCallsWorks() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select * from t1 where a < ?param + x + 3")) {
                statement.setLong("param", 40);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > ?param + x + 3")) {
                statement.setLong("param", 5);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select * from t1 where a < 40 + 3 + 3 and a > 5 + 3 + 3 and a > 5
            //                        ^                  ^                 ^
            //                        |                  |                 |
            //                        |                  |                 |
            //                    from sq0           from sq1           from the query
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 2L, 20L, 3L, 30L, 4L, 40L);
            }
            // drop parent function (sq0)
            try (var statement = connection.createStatement()) {
                statement.execute("drop temporary function sq0");
            }
            // should fail now that function sq0 is not present
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                RelationalAssertions.assertThrowsSqlException(statement::execute);
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
            final var preparedStatement = ddl.setSchemaAndGetConnection().prepareStatement("select * from sq1(x => 2) where a > ?param2");
            preparedStatement.setLong("param2", 25L);
            invokeAndVerify(preparedStatement::executeQuery, 3L, 30L, 4L, 40L);
            connection.rollback();
        }
    }

    @Test
    void createNestedTemporaryFunctionsWithPreparedParameters() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select * from t1 where a < ?param + x + 3")) {
                statement.setLong("param", 40);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > ?param + x + 3")) {
                statement.setLong("param", 5);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select * from t1 where a < 40 + 3 + 3 and a > 5 + 3 + 3 and a > 5
            //                        ^                  ^                 ^
            //                        |                  |                 |
            //                        |                  |                 |
            //                    from sq0           from sq1           from the query
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 2L, 20L, 3L, 30L, 4L, 40L);
            }
            connection.rollback();

            // try with a different query, we should get a cache hit now.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select * from t1 where a < ?param + x + 3")) {
                statement.setLong("param", 40);
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > ?param + x + 3")) {
                statement.setLong("param", 5);
                statement.execute();
            }
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_HIT, 2L, 20L, 3L, 30L, 4L, 40L);
            }
            connection.rollback();
        }
    }

    @Test
    void createNestedTemporaryFunctionsWithVariousLiterals() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select * from t1 where a < 40 + x + 1")) {
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(8) where a > 5 + x + 2")) {
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select * from t1 where a < 40 + 8 + 1 and a > 5 + 4 + 2 and a > 6
            //                        ^                  ^                 ^
            //                        |                  |                 |
            //                        |                  |                 |
            //                    from sq0           from sq1           from the query
            try (var statement = connection.prepareStatement("select * from sq1(x => 4) where a > 6 options (log query)")) {
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 2L, 20L, 3L, 30L, 4L, 40L);
            }
            connection.rollback();

            // try with a different query, we should get a cache hit now.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select * from t1 where a < 40 + x + 3")) {
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > 6 + x + 2")) {
                statement.execute();
            }
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > 5 options (log query)")) {
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_HIT, 2L, 20L, 3L, 30L, 4L, 40L);
            }
            connection.rollback();
        }
    }

    @Test
    void createNestedTemporaryFunctionsWithPreparedParametersOptimizationConstraintPertainingNestedFunction() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) create index indexOnA as select a from t1 where a < 35";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                statement.setLong("param", 20);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select a + 3, pk from t1 where a < 20 and a > 2 + 3 + 3 and a > 5
            //            ^                   ^          ^                 ^
            //            |                   |          |                 |
            //            |                   |          |                 |
            //         from sq0             from sq0    from sq1         from the query
            // this predicate leverages the index indexOnA, therefore, we must a plan constraint on sq0's bound prepared
            // parameter 'param' requiring its value to be less than 35.
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 13L, 1L);
            }
            connection.rollback();

            // try with a different query, BUT with a different binding of SQ0's param this time.
            // the cache must NOT be hit because it violates the generated plan constraint.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                statement.setLong("param", 40); // <----------- THIS violates the plan cache constraint
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select a + 3, pk from t1 where a < 40 and a > 2 + 3 + 3 and a > 5
            //            ^                   ^          ^                 ^
            //            |                   |          |                 |
            //            |                   |          |                 |
            //         from sq0             from sq0    from sq1         from the query
            // the predicate can NOT leverage the index indexOnA because it is not implied by the index predicate
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 13L, 1L, 23L, 2L, 33L, 3L);
            }
            connection.rollback();
        }
    }

    @Test
    void createNestedTemporaryFunctionsWithLiteralsOptimizationConstraintPertainingNestedFunction() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) create index indexOnA as select a from t1 where a < 35";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < 19")) {
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > 2 + x + 4")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select a + 3, pk from t1 where a < 19 and a > 2 + 3 + 4 and a > 6
            //            ^                   ^          ^                 ^
            //            |                   |          |                 |
            //            |                   |          |                 |
            //         from sq0             from sq0    from sq1         from the query
            // this predicate leverages the index indexOnA, therefore, we must a plan constraint on sq0's bound prepared
            // parameter 'param' requiring its value to be less than 35.
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > 6 options (log query)")) {
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 13L, 1L);
            }
            connection.rollback();

            // try with a different query, BUT with a different binding of SQ0's param this time.
            // the cache must NOT be hit because it violates the generated plan constraint.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < 34")) { // <--- does not violate the index constraint.
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0), using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select * from sq0(3) where a > 2 + x + 3")) {
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following query:
            // select a + 3, pk from t1 where a < 34 and a > 2 + 3 + 3 and a > 5
            //            ^                   ^          ^                 ^
            //            |                   |          |                 |
            //            |                   |          |                 |
            //         from sq0             from sq0    from sq1         from the query
            // the predicate can NOT leverage the index indexOnA because it is not implied by the index predicate
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) where a > 5 options (log query)")) {
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_HIT, 13L, 1L, 23L, 2L, 33L, 3L);
            }
            connection.rollback();
        }
    }

    @Test
    void useMultipleReferencesOfTemporaryFunctionsWithPreparedParameters() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) create index indexOnA as select a from t1 where a < 35";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 20);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following, relatively complex, rewrite:
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // union all
            //               (select Y.a, Y.pk from                                         <---- sq1(3)
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as R
            // where Q.a > 5 options (log query)
            // this predicate leverages the index indexOnA, therefore, we must a plan constraint on sq0's bound prepared
            // parameter 'param' requiring its value to be less than 35.
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q union all select * from sq1(x => 3) as R where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_MISS, 13L, 1L, 13L, 1L);
            }
            connection.rollback();

            // try with a different query, BUT with a different binding of SQ0's param this time.
            // the cache must be hit now, because the value of the prepared parameter of the parent function
            // imply the plan constraint.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 15); // <---- satisfies the plan cache constraint coming from the filtered index.
                statement.execute();
            }
            // re-create child function (sq1) referencing the re-recreated parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following, relatively complex, rewrite:
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // union all
            //               (select Y.a, Y.pk from                                         <---- sq1(3)
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as R
            // where Q.a > 5 options (log query)
            // this predicate leverages the index indexOnA, therefore, we must a plan constraint on sq0's bound prepared
            // parameter 'param' requiring its value to be less than 35.
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q union all select * from sq1(x => 3) as R where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                invokeAndVerify(statement::executeQuery, CheckPlanCache.SHOULD_HIT, 13L, 1L, 13L, 1L);
            }
            connection.rollback();
        }
    }

    @Test
    void useMultipleReferencesOfTemporaryFunctionsWithPreparedParametersAcrossContinuations() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) create index indexOnA as select a from t1 where a < 35";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 15)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 20);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following, relatively complex, rewrite:
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // options (log query)
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                statement.setMaxRows(1);
                invokeAndVerifyAcrossContinuations(statement::executeQuery, CheckPlanCache.SHOULD_MISS, connection, 13L, 1L, 18L, 2L, 13L, 1L, 18L, 2L);
            }
            connection.rollback();

            // try with a different query, BUT with a different binding of SQ0's param this time.
            // the cache must be hit now, because the value of the prepared parameter of the parent function
            // imply the plan constraint.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 20);
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // re-call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following, relatively complex, rewrite:
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // options (log query)
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                statement.setMaxRows(1);
                invokeAndVerifyAcrossContinuations(statement::executeQuery, CheckPlanCache.SHOULD_HIT, connection, 13L, 1L, 18L, 2L, 13L, 1L, 18L, 2L);
            }
            connection.rollback();
        }
    }

    @Test
    void useMultipleReferencesOfTemporaryFunctionsWithPreparedParametersContinuationsAcrossTransactions() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk)) create index indexOnA as select a from t1 where a < 35";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1(pk, a) values (1, 10), (2, 15)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            // create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 20);
                statement.execute();
            }
            // create child function (sq1) referencing parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // call the function (sq1) now, again, using the same literal identifiers.
            // expansion should result in the following, relatively complex, rewrite:
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // options (log query)
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                statement.setMaxRows(1);
                invokeAndVerifyAcrossContinuations(statement::executeQuery, CheckPlanCache.SHOULD_MISS, connection, 13L, 1L, 18L, 2L, 13L, 1L, 18L, 2L);
            }
            connection.rollback();

            // try with a different query, BUT with a different binding of SQ0's param this time.
            // the cache must be hit now, because the value of the prepared parameter of the parent function
            // imply the plan constraint.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            // re-create parent function (sq0)
            try (var statement = connection.prepareStatement("create temporary function sq0(in x bigint) " +
                    "on commit drop function as select a + x as a, pk from t1 where a < ?param")) {
                // the preparation of this parameter is fixed regardless how many times the function sq0 is referenced
                // in the future.
                statement.setLong("param", 20);
                statement.execute();
            }
            // re-create child function (sq1) referencing parent function (sq0) two times, using similar literal identifiers.
            try (var statement = connection.prepareStatement("create temporary function sq1(in x bigint) " +
                    "on commit drop function as select Y.a, Y.pk from sq0(3) as Y, sq0(3) as Z where Y.a > ?param + x + 3")) {
                statement.setLong("param", 2);
                statement.execute();
            }
            // re-call the function (sq1) now, again, using the same literal identifiers.
            // select Q.a, Q.pk from                                                        <---- the query
            //               (select Y.a, Y.pk from                                         <---- sq1(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Y,   <---- sq0(3) expansion
            //                           (select a + 3 as a, pk from t1 where a < 20) as Z    <---- sq0(3) expansion
            //                where Y.a > 2 + 3 + 3) as Q
            // options (log query)
            Continuation continuation = null;
            try (var statement = connection.prepareStatement("select * from sq1(x => 3) as Q where a > ?param options (log query)")) {
                statement.setLong("param", 5);
                statement.setMaxRows(2);
                try (var resultSet = statement.executeQuery()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13L, resultSet.getLong(1));
                    Assertions.assertEquals(1L, resultSet.getLong(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(18L, resultSet.getLong(1));
                    Assertions.assertEquals(2L, resultSet.getLong(2));
                    Assertions.assertFalse(resultSet.next());
                    continuation = resultSet.getContinuation();
                }
            }
            connection.rollback();

            // resume the continuation in another transaction.
            connection.unwrap(EmbeddedRelationalConnection.class).createNewTransaction();
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                statement.setBytes("continuation", continuation.serialize());
                try (var resultSet = statement.executeQuery()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(13L, resultSet.getLong(1));
                    Assertions.assertEquals(1L, resultSet.getLong(2));
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(18L, resultSet.getLong(1));
                    Assertions.assertEquals(2L, resultSet.getLong(2));
                    Assertions.assertFalse(resultSet.next());
                    continuation = resultSet.getContinuation();
                }
                Assertions.assertTrue(continuation.atEnd());
            }
            connection.rollback();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void createTemporaryFunctionCaseSensitivityOption(boolean isCaseSensitive) throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, isCaseSensitive)
                .database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, isCaseSensitive);

            try (var statement = connection.createStatement()) {
                statement.execute("create temporary function sq1(in x bigint) on commit drop function as select * from t1 where a < 40 + x ");
                invokeAndVerifyTempFunction(statement);
            }
            connection.rollback();
        }
    }

    @Test
    void attemptToCreateTemporaryFunctionWithDifferentCaseSensitivityOptionCase1() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true)
                .database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false);

            try (var statement = connection.createStatement()) {
                // create temporary function succeeds getting registered, as we do not (yet) compile it.
                statement.execute("create temporary function sq1(in x bigint) " +
                        "on commit drop function as select * from t1 where a < 40 + x ");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from sq1(34)"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table T1");
            }
            connection.rollback();
        }
    }

    @Test
    void attemptToCreateTemporaryFunctionWithDifferentCaseSensitivityOptionCase2() throws Exception {
        final String schemaTemplate = "create table t1(pk bigint, a bigint, primary key(pk))";
        try (var ddl = Ddl.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, false)
                .database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            connection.setOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);

            try (var statement = connection.createStatement()) {
                // create temporary function succeeds getting registered, as we do not (yet) compile it.
                statement.execute("create temporary function sq1(in x bigint) " +
                        "on commit drop function as select * from t1 where a < 40 + x ");
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from sq1(23)"))
                        .hasErrorCode(ErrorCode.UNDEFINED_TABLE)
                        .hasMessageContaining("Unknown table t1");
            }
            connection.rollback();
        }
    }

    @Test
    void createTemporaryFunctionDoNotParseBodyPreemptively() throws Exception {
        final var statement = "CREATE TEMPORARY FUNCTION blah() ON COMMIT DROP FUNCTION AS SELECT * FROM FOO ";
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .addTable(RecordLayerTable.newBuilder(false)
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("BAR")
                                .setDataType(DataType.Primitives.INTEGER.type())
                                .build())
                        .setName("FOO").build()).build();
        final var called = new AtomicBoolean(false);
        final var metadataOperationsFactory = new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull final SchemaTemplate template, final boolean throwIfExists, @Nonnull final RecordLayerInvokedRoutine invokedRoutine) {
                return new CreateTemporaryFunctionConstantAction(template, throwIfExists, invokedRoutine);
            }
        };
        final var visitor = new BaseVisitor(new MutablePlanGenerationContext(PreparedParams.empty(), PlanHashable.PlanHashMode.VC0, statement, statement, 42),
                schemaTemplate, NoOpQueryFactory.INSTANCE, metadataOperationsFactory, URI.create("/FDB/FRL1"), false) {
            @Override
            public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
                called.set(true);
                return null;
            }
        };

        // Execute visitor and assert that the plan is actually a ProceduralPlan and the body of the temporary function is not yet processed.
        final var plan = visitor.visit(QueryParser.parse(statement).getRootContext());
        Assertions.assertInstanceOf(ProceduralPlan.class, plan);
        Assertions.assertFalse(called.get());

        // Now, try executing the plan and again asserting that the body is not processed.
        final var mockedTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockedTransaction.getBoundSchemaTemplateMaybe()).thenReturn(Optional.empty());
        ArgumentCaptor<SchemaTemplate> captor = ArgumentCaptor.forClass(SchemaTemplate.class);
        final var executionContext = Plan.ExecutionContext.of(mockedTransaction, Options.none(), Mockito.mock(RelationalConnection.class), Mockito.mock(MetricCollector.class));
        ((ProceduralPlan) plan).executeInternal(executionContext);
        Mockito.verify(mockedTransaction).setBoundSchemaTemplate(captor.capture());
        Assertions.assertFalse(captor.getValue().getTemporaryInvokedRoutines().isEmpty());
        Assertions.assertFalse(called.get());
    }

    @Test
    void unpivotRepeatedFieldInSqlFunctionWorksCorrectly() throws Exception {
        final String schemaTemplate = "create type as struct city(name string, population bigint) " +
                "create table country(id bigint, name string, continent string, cities city array, primary key(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into country values " +
                        "(1, 'USA', 'North America', [('New York' ,8419600), ('Los Angeles', 3980400)]), " +
                        "(2, 'Canada', 'North America', [('Toronto', 2731571), ('Montreal', 1760400)]), " +
                        "(3, 'Brazil', 'South America', [('Rio de Janeiro', 6795900), ('Sao Paulo', 12303800)]), " +
                        "(4, 'France', 'Europe', [('Paris', 2148327), ('Lyon', 516855)])");
            }
            final var connection = ddl.getConnection();
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("create temporary function northAmericaCountries() " +
                    "on commit drop function as select * from country where continent = 'North America'")) {
                statement.execute();
            }
            try (var statement = connection.prepareStatement("select A.name from northAmericaCountries, (select * from northAmericaCountries.cities) as A where population > 8000000")) {
                try (var resultSet = statement.executeQuery()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals("New York", resultSet.getString(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
            connection.rollback();
        }
    }

    private void invokeAndVerifyTempFunction(final RelationalStatement statement) throws SQLException {
        Assertions.assertTrue(statement.execute("select * from sq1(x => 2)"));
        invokeAndVerify(statement::getResultSet, 1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L);
    }

    private void invokeAndVerify(Supplier<RelationalResultSet> resultSupplier, Object... expectedResults) throws SQLException {
        invokeAndVerify(resultSupplier, CheckPlanCache.DO_NOT_CARE, expectedResults);
    }

    private void invokeAndVerify(Supplier<RelationalResultSet> resultSupplier, CheckPlanCache checkPlanCache,
                                        Object... expectedResults) throws SQLException {
        invokeAndVerify(resultSupplier, checkPlanCache, false, Optional.empty(), expectedResults);
    }

    private void invokeAndVerifyAcrossContinuations(Supplier<RelationalResultSet> resultSupplier, CheckPlanCache checkPlanCache,
                                                    RelationalConnection connection, Object... expectedResults) throws SQLException {
        invokeAndVerify(resultSupplier, checkPlanCache, true, Optional.of(connection), expectedResults);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void invokeAndVerify(Supplier<RelationalResultSet> resultSupplier, CheckPlanCache checkPlanCache,
                                 boolean acrossContinuations, Optional<RelationalConnection> connection, Object... expectedResults) throws SQLException {
        assert (expectedResults.length > 0 && expectedResults.length % 2 == 0);
        var resultSet = resultSupplier.get();
        switch (checkPlanCache) {
            case SHOULD_HIT:
                Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
                break;
            case SHOULD_MISS:
                Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
                break;
            case DO_NOT_CARE: // fallthrough
            default:
                break;
        }
        if (!acrossContinuations) {
            for (int row = 0; row <= expectedResults.length - 2; row += 2) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow()
                        .as("checking result %d", row / 2)
                        .isRowExactly(expectedResults[row], expectedResults[row + 1]);
            }
        } else {
            Assertions.assertTrue(connection.isPresent());

            try (var statement = connection.get().prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                statement.setMaxRows(1);
                for (int row = 0; row <= expectedResults.length - 2; row += 2) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .isRowExactly(expectedResults[row], expectedResults[row + 1])
                            .hasNoNextRow();
                    var continuation = resultSet.getContinuation();
                    if (row + 2 < expectedResults.length) {
                        statement.setBytes("continuation", continuation.serialize());
                        Assertions.assertTrue(statement.execute(), "Did not return a result set from a select statement!");
                        resultSet = Assertions.assertInstanceOf(RelationalResultSet.class, statement.getResultSet());
                    }
                }
            }
        }
        ResultSetAssert.assertThat(resultSet).hasNoNextRow();
    }

    private enum CheckPlanCache {
        SHOULD_HIT,
        SHOULD_MISS,
        DO_NOT_CARE
    }

    @FunctionalInterface
    private interface Supplier<T> {
        T get() throws SQLException;
    }
}
