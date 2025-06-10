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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;

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

    private void invokeAndVerifyTempFunction(final RelationalStatement statement) throws SQLException {
        Assertions.assertTrue(statement.execute("select * from sq1(x => 2)"));
        invokeAndVerify(statement::getResultSet, 1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L);
    }

    private void invokeAndVerify(Supplier<RelationalResultSet> resultSupplier, Object... expectedResults) throws SQLException {
        invokeAndVerify(resultSupplier, CheckPlanCache.DO_NOT_CARE, expectedResults);
    }

    private void invokeAndVerify(Supplier<RelationalResultSet> resultSupplier, CheckPlanCache checkPlanCache,
                                        Object... expectedResults) throws SQLException {
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
        for (int row = 0; row <= expectedResults.length - 2; row += 2) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .isRowExactly(expectedResults[row], expectedResults[row + 1]);
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
