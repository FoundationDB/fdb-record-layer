/*
 * GroupByQueryTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Base64;

import static com.apple.foundationdb.relational.recordlayer.query.QueryTestUtils.insertT1Record;
import static com.apple.foundationdb.relational.recordlayer.query.QueryTestUtils.insertT1RecordColAIsNull;

public class GroupByQueryTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public GroupByQueryTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void groupByWithScanLimit() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var conn = ddl.setSchemaAndGetConnection()) {
                Continuation continuation = null;
                conn.setOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 2);
                try (var statement = conn.createStatement()) {
                    insertT1Record(statement, 2, 1, 1, 20);
                    insertT1Record(statement, 3, 1, 2, 5);
                    insertT1Record(statement, 4, 1, 2, 15);
                    insertT1Record(statement, 5, 1, 2, 5);
                    insertT1Record(statement, 6, 2, 1, 10);
                    insertT1Record(statement, 7, 2, 1, 40);
                    insertT1Record(statement, 8, 2, 1, 20);
                    insertT1Record(statement, 9, 2, 1, 90);

                    String query = "SELECT a AS OK, b, MAX(c) FROM T1 GROUP BY a, b";
                    // scan pk = 2 and pk = 3 and hit SCAN_LIMIT_REACHED
                    Assertions.assertTrue(statement.execute(query), "Did not return a result set from a select statement!");
                    try (final RelationalResultSet resultSet = statement.getResultSet()) {
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .isRowExactly(1L, 1L, 20L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                }
                try (var preparedStatement = conn.prepareStatement("EXECUTE CONTINUATION ?param")) {
                    conn.setOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 2);
                    // scan pk = 5 and pk = 4 rows, hit SCAN_LIMIT_REACHED
                    preparedStatement.setBytes("param", continuation.serialize());
                    try (final RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                    // scan pk = 6 and pk = 8 rows, hit SCAN_LIMIT_REACHED
                    preparedStatement.setBytes("param", continuation.serialize());
                    try (final RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .isRowExactly(1L, 2L, 15L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                    // scan pk = 7 and pk = 9 rows, hit SCAN_LIMIT_REACHED
                    preparedStatement.setBytes("param", continuation.serialize());
                    try (final RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                    // hit SOURCE_EXHAUSTED
                    preparedStatement.setBytes("param", continuation.serialize());
                    try (final RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .isRowExactly(2L, 1L, 90L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                    Assertions.assertTrue(continuation.atEnd());
                }
            }
        }
    }

    @Test
    void groupByWithRowLimit() throws Exception {
        final String schemaTemplate =
                "create table t1(pk bigint, a bigint, b bigint, c bigint, primary key(pk))\n" +
                        "create index i1 as select a from t1";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var conn = ddl.setSchemaAndGetConnection()) {
                conn.setOption(Options.Name.MAX_ROWS, 1);
                try (var statement = conn.createStatement()) {
                    insertT1Record(statement, 1, 10, 1, 20);
                    insertT1Record(statement, 2, 10, 2, 20);
                    insertT1Record(statement, 3, 10, 3, 5);
                    insertT1Record(statement, 4, 10, 4, 15);
                    insertT1Record(statement, 5, 10, 5, 5);
                    insertT1Record(statement, 6, 20, 6, 10);
                    insertT1Record(statement, 7, 20, 7, 40);
                    insertT1Record(statement, 8, 20, 8, 20);
                    insertT1Record(statement, 9, 20, 9, 90);
                    insertT1Record(statement, 10, 20, 10, 10);
                    insertT1Record(statement, 11, 20, 11, 40);
                    insertT1Record(statement, 12, 20, 12, 20);
                    insertT1Record(statement, 13, 20, 13, 90);

                    String query = "select AVG(x.b) from (select a,b from t1) as x group by x.a";
                    Continuation continuation = null;
                    // scan pk = 2 and pk = 3 and hit SCAN_LIMIT_REACHED
                    Assertions.assertTrue(statement.execute(query), "Did not return a result set from a select statement!");
                    try (final RelationalResultSet resultSet = statement.getResultSet()) {
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .isRowExactly(3.0)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                    String postfix = " WITH CONTINUATION B64'" + Base64.getEncoder().encodeToString(continuation.serialize()) + "'";
                    Assertions.assertTrue(statement.execute(query + postfix), "Did not return a result set from a select statement!");
                    try (final RelationalResultSet resultSet = statement.getResultSet()) {
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .isRowExactly(9.5)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                }
            }
        }
    }

    @Test
    void isNullPredicateUsesGroupIndex() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))\n" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c\n" +
                        "CREATE INDEX sum_idx as select sum(c) from t1 group by a\n";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                insertT1RecordColAIsNull(statement, 10, 3, 10);
                insertT1RecordColAIsNull(statement, 11, 4, 35);
                Assertions.assertTrue(statement.execute("EXPLAIN SELECT sum(c) FROM T1 WHERE a is null"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumn("PLAN", "AISCAN(SUM_IDX [[null],[null]] BY_GROUP -> [_0: KEY:[0], _1: VALUE:[0]]) | MAP ((_._1 AS _0) AS _0) | ON EMPTY NULL | MAP (_._0._0 AS _0)");
                }
                Assertions.assertTrue(statement.execute("EXPLAIN SELECT sum(c) FROM T1 WHERE a = 1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumn("PLAN", "AISCAN(SUM_IDX [EQUALS promote(@c11 AS LONG)] BY_GROUP -> [_0: KEY:[0], _1: VALUE:[0]]) | MAP ((_._1 AS _0) AS _0) | ON EMPTY NULL | MAP (_._0._0 AS _0)");
                }
            }
        }
    }

    @Test
    void groupByClauseWithPredicateWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a AS OK, b, MAX(c) FROM T1 WHERE a > 1 GROUP BY a, b HAVING MIN(c) < 50"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithPredicateMultipleAggregationsWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from T1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, SUM(c), b, MAX(c) FROM T1 WHERE a > 1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2L, 160L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from T1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(1L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksWithSubquery() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from T1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c) FROM (select * from T1) AS T2 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(1L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksWithSubqueryAliases() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from T1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT max(T2.c), b, MAX(c) FROM (select * from T1) AS T2 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(20L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(15L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(90L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // Although Relational is generating a proper plan, this query fails because we don't have sophisticated mechanism in RecordLayer for understanding identifiers' derivations https://github.com/FoundationDB/fdb-record-layer/issues/1212
    void groupByClauseWorksWithSubqueryAliasesComplex() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT max(T2.x), y, MAX(x) FROM (select a AS x, b AS y from T1) AS T2 GROUP BY x, y"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(20L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(15L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(90L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksDifferentAggregations() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c), MIN(c), COUNT(c), AVG(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L, 20L, 20L, 1L, 20.0)
                            .hasNextRow()
                            .isRowExactly(1L, 2L, 15L, 5L, 3L, 10.0)
                            .hasNextRow()
                            .isRowExactly(2L, 1L, 90L, 10L, 4L, 40.0)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksComplexGrouping() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, b, a+b, MAX(c), MIN(c), COUNT(c), AVG(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L, 2L, 20L, 20L, 1L, 20.0)
                            .hasNextRow()
                            .isRowExactly(1L, 2L, 3L, 15L, 5L, 3L, 10.0)
                            .hasNextRow()
                            .isRowExactly(2L, 1L, 3L, 90L, 10L, 4L, 40.0)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseSingleGroup() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithoutGroupingColumnsInProjectionList() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithoutAggregationsInProjectionList() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT a, b FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 1L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByInSubSelectWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("select x.a, x.m from (SELECT a, b, max(c) as m FROM T1 GROUP BY a, b) x"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // TODO (Support constant expressions in GROUP BY).
    void groupByConstantColumn() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 3, 15);
                insertT1Record(statement, 5, 2, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT max(c) as m FROM T1 GROUP BY 42, 55"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void aggregationWithoutGroupBy() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT MAX(c) FROM T1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // this does not work, presumably because of https://github.com/FoundationDB/fdb-record-layer/issues/1212
    void nestedGroupByStatements() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b from t1 order by a, b";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                Assertions.assertTrue(statement.execute("SELECT COUNT(H) FROM (SELECT SUM(c) AS H FROM T1 GROUP BY a, b) AS X"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksComplexAggregations() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, SUM(c) / COUNT(c), b, MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 20L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(1L, 10L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(2L, 40L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithNestedAggregationsIsSupported() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, SUM(c) / COUNT(c), b, MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 20L, 1L, 20L)
                            .hasNextRow()
                            .isRowExactly(1L, 10L, 2L, 15L)
                            .hasNextRow()
                            .isRowExactly(2L, 40L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void expansionOfStarWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a from t1";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT * from (select a from t1) as X group by a"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L)
                            .hasNextRow()
                            .isRowExactly(2L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithNamedGroupingColumns() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT SUM(c) / COUNT(c), MAX(c) FROM T1 GROUP BY a as x, b as y"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(20L, 20L)
                            .hasNextRow()
                            .isRowExactly(10L, 15L)
                            .hasNextRow()
                            .isRowExactly(40L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // (yhatem) check ordering requirements of join.
    void groupByOverJoinWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE TABLE T2(pk bigint, x bigint, y bigint, z bigint, primary key(pk))" +
                        "CREATE INDEX idx1 AS SELECT B FROM T1";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT max(y) from (select y, b as L from t1, t2) as q group by l"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L)
                            .hasNextRow()
                            .isRowExactly(2L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByWithNamedGroups() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))" +
                        "CREATE INDEX idx1 as select a, b, c from t1 order by a, b, c";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT X AS OK, b, MAX(c) FROM T1 WHERE a > 1 GROUP BY a as X, b HAVING MIN(c) < 50"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

}
