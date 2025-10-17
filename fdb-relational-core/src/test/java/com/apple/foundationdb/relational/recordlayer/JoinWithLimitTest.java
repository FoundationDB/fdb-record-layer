/*
 * JoinWithLimitTest.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Map;

public class JoinWithLimitTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String getTemplate_definition =
            "CREATE TABLE R(rpk bigint, ra bigint, primary key(rpk))" +
                    "CREATE TABLE S(spk bigint, sa bigint, primary key(spk))" +
                    "CREATE TYPE AS STRUCT D ( e bigint )" +
                    "CREATE TABLE Q(qpk bigint, d D array, PRIMARY KEY(qpk))";

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(UniqueIndexTests.class, getTemplate_definition);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public JoinWithLimitTest() throws SQLException {
    }

    @BeforeAll
    public static void beforeAll() {
        Utils.enableCascadesDebugger();
    }

    @BeforeEach
    void setup() throws Exception {
        statement.execute("INSERT INTO R VALUES (1, 1), (2, 2)");
        statement.execute("INSERT INTO S VALUES (1, 10), (2, 20)");
        statement.execute("INSERT INTO Q VALUES (1, [(100), (200), (300)]), (2, [(400), (500), (600)])");
    }

    @Test
    void innerCorrelatedJoinNoLimit() throws Exception {
        try (var resultSet = statement.executeQuery("SELECT qpk, M.e FROM (SELECT * FROM Q, Q.d) as M")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 100L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 200L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 300L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 400L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 500L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 600L))
                    .hasNoNextRow();
        }
    }

    @Test
    void innerCorrelatedJoinLimit1() throws Exception {
        statement.setMaxRows(1);
        try (var resultSet = statement.executeQuery("SELECT M.e FROM (SELECT * FROM Q, Q.d) as M")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("e", 100L))
                    .hasNoNextRow();
        }
    }

    @Test
    void innerCorrelatedJoinWithContinuationAndLimit() throws Exception {
        Continuation continuation;
        statement.setMaxRows(1);
        try (var resultSet = statement.executeQuery("SELECT M.e FROM (SELECT * FROM Q, Q.d) as M")) {
            Assertions.assertThat(resultSet.next()).isTrue();
            Assertions.assertThat(resultSet.getLong("e")).isEqualTo(100L);
            continuation = resultSet.getContinuation();
        }

        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setMaxRows(3);
            preparedStatement.setBytes("param", continuation.serialize());
            try (final var resultSet = preparedStatement.executeQuery()) {
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("e")).isEqualTo(200L);
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("e")).isEqualTo(300L);
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("e")).isEqualTo(400L);
                continuation = resultSet.getContinuation();
            }
        }

        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setMaxRows(3);
            preparedStatement.setBytes("param", continuation.serialize());
            try (final var resultSet = preparedStatement.executeQuery()) {
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("e")).isEqualTo(500L);
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("e")).isEqualTo(600L);
                Assertions.assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void correlatedJoinNoLimit() throws Exception {
        try (var resultSet = statement.executeQuery("SELECT qpk, e FROM Q, Q.d")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 100L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 200L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 300L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 400L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 500L))
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 2L, "e", 600L))
                    .hasNoNextRow();
        }
    }

    @Test
    void correlatedJoinWithLimit() throws Exception {
        statement.setMaxRows(1);
        try (var resultSet = statement.executeQuery("SELECT qpk, e FROM Q, Q.d")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("qpk", 1L, "e", 100L))
                    .hasNoNextRow();
        }
    }

    @Test
    void joinWithNoLimit() throws Exception {
        try (var resultSet = statement.executeQuery("SELECT * FROM R, S")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 1L, "sa", 10L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 2L, "sa", 20L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 2L, "ra", 2L, "spk", 1L, "sa", 10L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 2L, "ra", 2L, "spk", 2L, "sa", 20L))
                    .hasNoNextRow();
        }
    }

    @Test
    void joinWithLimit1() throws Exception {
        statement.setMaxRows(1);
        try (var resultSet = statement.executeQuery("SELECT * FROM R, S")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 1L, "sa", 10L))
                    .hasNoNextRow();
        }
    }

    @Test
    void joinWithLimit2() throws Exception {
        statement.setMaxRows(2);
        try (var resultSet = statement.executeQuery("SELECT * FROM R, S")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 1L, "sa", 10L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 2L, "sa", 20L))
                    .hasNoNextRow();
        }
    }

    @Test
    void joinWithLimitLargerThanTableSize() throws Exception {
        statement.setMaxRows(20);
        try (var resultSet = statement.executeQuery("SELECT * FROM R, S")) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 1L, "sa", 10L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 1L, "ra", 1L, "spk", 2L, "sa", 20L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 2L, "ra", 2L, "spk", 1L, "sa", 10L))
                    .hasNextRow()
                    .hasColumns(Map.of("rpk", 2L, "ra", 2L, "spk", 2L, "sa", 20L))
                    .hasNoNextRow();
        }
    }

    @Test
    void joinWithContinuationAndLimit() throws Exception {
        Continuation continuation;
        statement.setMaxRows(1);
        try (var resultSet = statement.executeQuery("SELECT rpk, sa FROM R, S")) {
            Assertions.assertThat(resultSet.next()).isTrue();
            Assertions.assertThat(resultSet.getLong("rpk")).isEqualTo(1L);
            Assertions.assertThat(resultSet.getLong("sa")).isEqualTo(10L);
            continuation = resultSet.getContinuation();
        }

        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setMaxRows(2);
            preparedStatement.setBytes("param", continuation.serialize());
            try (final var resultSet = preparedStatement.executeQuery()) {
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("rpk")).isEqualTo(1L);
                Assertions.assertThat(resultSet.getLong("sa")).isEqualTo(20L);
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("rpk")).isEqualTo(2L);
                Assertions.assertThat(resultSet.getLong("sa")).isEqualTo(10L);
                continuation = resultSet.getContinuation();
            }
        }

        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setMaxRows(2);
            preparedStatement.setBytes("param", continuation.serialize());
            try (final var resultSet = preparedStatement.executeQuery()) {
                Assertions.assertThat(resultSet.next()).isTrue();
                Assertions.assertThat(resultSet.getLong("rpk")).isEqualTo(2L);
                Assertions.assertThat(resultSet.getLong("sa")).isEqualTo(20L);
                Assertions.assertThat(resultSet.next()).isFalse();
            }
        }
    }
}
