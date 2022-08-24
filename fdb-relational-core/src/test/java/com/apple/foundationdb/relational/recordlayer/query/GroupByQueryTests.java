/*
 * GroupByQueryTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.apple.foundationdb.relational.recordlayer.query.QueryTestUtils.insertT1Record;

public class GroupByQueryTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public GroupByQueryTests() {
        if (Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    @Test
    void groupByClauseWithPredicateWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b, c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 5);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c) FROM T1 WHERE a > 1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithPredicateMultipleAggregationsWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b, c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(2L, 160L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(1L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksWithSubquery() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(1L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(2L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksWithSubqueryAliases() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(20L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(15L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(90L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // Although Relational is generating a proper plan, this query fails because we don't have sophisticated mechanism in RecordLayer for understanding identifiers' derivations https://github.com/FoundationDB/fdb-record-layer/issues/1212
    void groupByClauseWorksWithSubqueryAliasesComplex() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(20L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(15L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(90L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksDifferentAggregations() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 1L, 20L, 20L, 1L, 20.0)
                            .hasNextRow()
                            .hasRowExactly(1L, 2L, 15L, 5L, 3L, 10.0)
                            .hasNextRow()
                            .hasRowExactly(2L, 1L, 90L, 10L, 4L, 40.0)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksComplexGrouping() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 1L, 2L, 20L, 20L, 1L, 20.0)
                            .hasNextRow()
                            .hasRowExactly(1L, 2L, 3L, 15L, 5L, 3L, 10.0)
                            .hasNextRow()
                            .hasRowExactly(2L, 1L, 3L, 90L, 10L, 4L, 40.0)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseSingleGroup() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT a, b, MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(1L, 1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithoutGroupingColumnsInProjectionList() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT MAX(c) FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithoutAggregationsInProjectionList() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT a, b FROM T1 GROUP BY a, b"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(1L, 1L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByInSubSelectWorks() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("select x.a, x.m from (SELECT a, b, max(c) as m FROM T1 GROUP BY a, b) x"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // TODO (Support constant expressions in GROUP BY).
    void groupByConstantColumn() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 3, 15);
                insertT1Record(statement, 5, 2, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT max(c) as m FROM T1 GROUP BY 42, 55"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(1L, 2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void aggregationWithoutGroupBy() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 1, 15);
                insertT1Record(statement, 5, 1, 1, 5);
                Assertions.assertTrue(statement.execute("SELECT MAX(c) FROM T1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(2000L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // this does not work, presumably because of https://github.com/FoundationDB/fdb-record-layer/issues/1212
    void nestedGroupByStatements() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 2000);
                insertT1Record(statement, 3, 1, 1, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                Assertions.assertTrue(statement.execute("SELECT COUNT(H) FROM (SELECT SUM(c) AS H FROM T1 GROUP BY a, b) AS X"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(2L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWorksComplexAggregations() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b, c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 20L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(1L, 10L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(2L, 40L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void groupByClauseWithNestedAggregationsIsSupported() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b, c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
                            .hasRowExactly(1L, 20L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(1L, 10L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(2L, 40L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled // we require a fix for https://github.com/FoundationDB/fdb-record-layer/issues/1212 to make this work.
    void groupByClauseWithNamedGroupingColumns() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE T1(pk int64, a int64, b int64, c int64, PRIMARY KEY(pk))" +
                        "CREATE VALUE INDEX idx1 on T1(a, b, c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertT1Record(statement, 2, 1, 1, 20);
                insertT1Record(statement, 3, 1, 2, 10);
                insertT1Record(statement, 4, 1, 2, 15);
                insertT1Record(statement, 5, 1, 2, 5);
                insertT1Record(statement, 6, 2, 1, 10);
                insertT1Record(statement, 7, 2, 1, 40);
                insertT1Record(statement, 8, 2, 1, 20);
                insertT1Record(statement, 9, 2, 1, 90);
                Assertions.assertTrue(statement.execute("SELECT SUM(c) / COUNT(c), MAX(c) FROM T1 GROUP BY as x, b as y"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(1L, 20L, 1L, 20L)
                            .hasNextRow()
                            .hasRowExactly(1L, 10L, 2L, 15L)
                            .hasNextRow()
                            .hasRowExactly(2L, 40L, 1L, 90L)
                            .hasNoNextRow();
                }
            }
        }
    }

}
