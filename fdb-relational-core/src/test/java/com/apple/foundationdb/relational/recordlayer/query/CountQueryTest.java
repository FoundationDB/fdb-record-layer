/*
 * CountQueryTest.java
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
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Base64;

/**
 * Some basic count query tests. These are not too dissimilar from queries that are in some of the YAML tests,
 * but they are included here as a kind of smoke test that runs earlier in the build that also can be more easily
 * run with a debugger.
 */
public class CountQueryTest {
    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1 (id bigint, a string, b bigint, primary key (id)) " +
                    "CREATE INDEX i1 AS SELECT a FROM t1 " +
                    "CREATE INDEX i2 AS SELECT count(*) FROM t1 GROUP BY a " +
                    "CREATE INDEX i3 AS SELECT count(a) FROM t1 GROUP BY b";

    @RegisterExtension
    @Order(0)
    final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    final SimpleDatabaseRule database = new SimpleDatabaseRule(CountQueryTest.class, SCHEMA_TEMPLATE);

    @RegisterExtension
    @Order(2)
    final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri).withSchema(database.getSchemaName());

    @RegisterExtension
    @Order(3)
    final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private void insertTestData() throws SQLException {
        statement.executeUpdate("INSERT INTO T1 VALUES (1, 'foo', 100), (2, 'foo', 200), (3, 'bar', 300), (4, 'bar', 400)");
    }

    @BeforeEach
    void setUp() throws SQLException {
        insertTestData();
    }

    @Test
    void countForA() throws SQLException {
        try (RelationalResultSet resultSet = statement.executeQuery("SELECT count(*) AS \"c\" FROM t1 WHERE a = 'foo' GROUP BY a")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("c", 2L)
                    .hasNoNextRow()
                    .continuationReasonIs(Continuation.Reason.CURSOR_AFTER_LAST);
        }
    }

    @Test
    void countForAByStreaming() throws SQLException {
        try (RelationalResultSet resultSet = statement.executeQuery("SELECT count(*) AS c FROM t1 USE INDEX (i1) WHERE a = 'foo' GROUP BY a")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("C", 2L)
                    .hasNoNextRow()
                    .continuationReasonIs(Continuation.Reason.CURSOR_AFTER_LAST);
        }
    }

    @Test
    void countNotNullAByB() throws SQLException {
        try (RelationalResultSet resultSet = statement.executeQuery("SELECT b, count(a) AS a FROM t1 GROUP BY b")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("B", 100L)
                    .hasColumn("A", 1L)
                    .hasNextRow()
                    .hasColumn("B", 200L)
                    .hasColumn("A", 1L)
                    .hasNextRow()
                    .hasColumn("B", 300L)
                    .hasColumn("A", 1L)
                    .hasNextRow()
                    .hasColumn("B", 400L)
                    .hasColumn("A", 1L)
                    .hasNoNextRow()
                    .continuationReasonIs(Continuation.Reason.CURSOR_AFTER_LAST);
        }
    }

    @Test
    void countWithoutAliasNamePreservedAfterContinuation() throws SQLException {
        statement.setMaxRows(1);
        Continuation continuation;
        try (RelationalResultSet resultSet = statement.executeQuery("SELECT b, count(a) FROM t1 GROUP BY b")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("_1", 1L)
                    .hasNoNextRow()
                    .continuationReasonIs(Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED);
            continuation = resultSet.getContinuation();
        }
        statement.setMaxRows(1);
        try (RelationalResultSet resultSet = statement.executeQuery("EXECUTE CONTINUATION B64'" + Base64.getEncoder().encodeToString(continuation.serialize()) + "'")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("_1", 1L)
                    .hasNoNextRow()
                    .continuationReasonIs(Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED);
        }
    }
}
