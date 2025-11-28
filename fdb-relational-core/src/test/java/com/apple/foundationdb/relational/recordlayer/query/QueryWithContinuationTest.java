/*
 * QueryWithContinuationTest.java
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.continuation.ContinuationProto;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

public class QueryWithContinuationTest {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx as select name from RestaurantComplexRecord" +
                    " CREATE INDEX reviewer_name_idx as select name from RestaurantReviewer" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public QueryWithContinuationTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void preparedStatement() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                ps.setMaxRows(2);
                continuation = assertResult(ps, 10L, 11L);
                assertContinuation(continuation, false, false);
            }
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                ps.setMaxRows(2);
                ps.setBytes("continuation", continuation.serialize());
                continuation = assertResult(ps, 12L, 13L);
                assertContinuation(continuation, false, false);

                ps.setBytes("continuation", continuation.serialize());
                continuation = assertResult(ps, 14L);
                assertContinuation(continuation, false, true);
            }
        }
    }

    @Test
    void preparedStatementWithExecuteContinuation() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (var statement = connection.prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                    statement.setMaxRows(2);
                    continuation = assertResult(statement, 10L, 11L);
                    assertContinuation(continuation, false, false);
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 12L, 13L);
                    assertContinuation(continuation, false, false);

                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 14L);
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void preparedStatementWithLimit() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                ps.setMaxRows(2);
                continuation = assertResult(ps, 10L, 11L);
                assertContinuation(continuation, false, false);
            }
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                ps.setMaxRows(2);
                continuation = assertResult(ps, 12L, 13L);
                assertContinuation(continuation, false, false);

                ps.setBytes("continuation", continuation.serialize());
                continuation = assertResult(ps, 14L);
                assertContinuation(continuation, false, true);
            }
        }
    }

    @Test
    void preparedStatementWithParam() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (var statement = connection.prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO > ?p")) {
                    statement.setMaxRows(2);
                    statement.setInt("p", 9);
                    continuation = assertResult(statement, 10L, 11L);
                    assertContinuation(continuation, false, false);
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setBytes("continuation", continuation.serialize());
                    statement.setMaxRows(2);
                    continuation = assertResult(statement, 12L, 13L);
                    assertContinuation(continuation, false, false);

                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 14L);
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void preparedStatementWithLiteral() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (var statement = connection.prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO > 9")) {
                    statement.setMaxRows(2);
                    continuation = assertResult(statement, 10L, 11L);
                    assertContinuation(continuation, false, false);
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 12L, 13L);
                    assertContinuation(continuation, false, false);

                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 14L);
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void preparedStatementWithDifferentLimit() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (var statement = connection.prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO > 9")) {
                    statement.setMaxRows(2);
                    continuation = assertResult(statement, 10L, 11L);
                    assertContinuation(continuation, false, false);
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(4);
                    statement.setBytes("continuation", continuation.serialize());
                    continuation = assertResult(statement, 12L, 13L, 14L);
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void preparedStatementWithDifferentLimitParam() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (var statement = connection.prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO > 9")) {
                    statement.setMaxRows(2);
                    continuation = assertResult(statement, 10L, 11L);
                    assertContinuation(continuation, false, false);
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setBytes("continuation", continuation.serialize());
                    statement.setMaxRows(4);
                    continuation = assertResult(statement, 12L, 13L, 14L);
                    assertContinuation(continuation, false, true);
                }
            }
        }
    }

    @Test
    void standardStatement() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (RelationalStatement statement = connection.createStatement()) {
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord")) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 10L)
                                .hasNextRow().hasColumn("REST_NO", 11L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);
                    }
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());
                    try (final RelationalResultSet resultSet = statement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 12L)
                                .hasNextRow().hasColumn("REST_NO", 13L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);
                    }

                    statement.setBytes("continuation", continuation.serialize());
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 14L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, true);
                    }
                }
            }
        }
    }

    @Test
    void standardStatementWithDifferentPlanHashModes() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (RelationalConnection connection = ddl.setSchemaAndGetConnection()) {
                // legacy version 0
                connection.setOption(Options.Name.CURRENT_PLAN_HASH_MODE, PlanHashable.PlanHashMode.VL0.name());
                try (RelationalStatement statement = connection.createStatement()) {
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord")) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 10L)
                                .hasNextRow().hasColumn("REST_NO", 11L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);

                        final var embeddedRelationalConnection = (EmbeddedRelationalConnection) connection;
                        final var metricCollector = Objects.requireNonNull(embeddedRelationalConnection.getMetricCollector());
                        Assertions.assertThat(metricCollector.hasCounter(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED)).isFalse();
                        Assertions.assertThat(metricCollector.hasCounter(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL)).isFalse();
                    }
                }

                connection.setOption(Options.Name.VALID_PLAN_HASH_MODES,
                        PlanHashable.PlanHashMode.VL0.name() + "," + PlanHashable.PlanHashMode.VC0.name());
                connection.setOption(Options.Name.CURRENT_PLAN_HASH_MODE, PlanHashable.PlanHashMode.VC0.name());
                try (RelationalStatement statement = connection.createStatement()) {
                    String continuationString = java.util.Base64.getEncoder().encodeToString(continuation.serialize());
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery("EXECUTE CONTINUATION B64'" + continuationString + "'")) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 12L)
                                .hasNextRow().hasColumn("REST_NO", 13L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);

                        final var embeddedRelationalConnection = (EmbeddedRelationalConnection) connection;
                        final var metricCollector = Objects.requireNonNull(embeddedRelationalConnection.getMetricCollector());
                        Assertions.assertThat(metricCollector.hasCounter(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED)).isTrue();
                        Assertions.assertThat(metricCollector.getCountsForCounter(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED)).isEqualTo(1L);
                        Assertions.assertThat(metricCollector.hasCounter(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL)).isTrue();
                        Assertions.assertThat(metricCollector.getCountsForCounter(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL)).isEqualTo(1L);
                    }
                }

                connection.setOption(Options.Name.VALID_PLAN_HASH_MODES, PlanHashable.PlanHashMode.VC0.name());
                connection.setOption(Options.Name.CURRENT_PLAN_HASH_MODE, PlanHashable.PlanHashMode.VC0.name());
                try (RelationalStatement statement = connection.createStatement()) {
                    String continuationString = java.util.Base64.getEncoder().encodeToString(continuation.serialize());
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery("EXECUTE CONTINUATION B64'" + continuationString + "'")) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 14L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, true);

                        final var embeddedRelationalConnection = (EmbeddedRelationalConnection) connection;
                        final var metricCollector = Objects.requireNonNull(embeddedRelationalConnection.getMetricCollector());
                        Assertions.assertThat(metricCollector.hasCounter(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED)).isTrue();
                        Assertions.assertThat(metricCollector.getCountsForCounter(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED)).isEqualTo(1L);
                        // No down-level in this execution (continuation was created with VC0, executed with VC0)
                    }
                }
            }
        }
    }

    @Test
    void standardStatementWithLiterals() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (RelationalStatement statement = connection.createStatement()) {
                    statement.setMaxRows(2);
                    try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE REST_NO > 9")) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 10L)
                                .hasNextRow().hasColumn("REST_NO", 11L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);
                    }
                }
                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());
                    try (final RelationalResultSet resultSet = statement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 12L)
                                .hasNextRow().hasColumn("REST_NO", 13L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, false);
                    }
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());
                    try (final RelationalResultSet resultSet = statement.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("REST_NO", 14L)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        assertContinuation(continuation, false, true);
                    }
                }
            }
        }
    }

    private Continuation assertResult(RelationalPreparedStatement ps, Long... values) throws SQLException {
        Continuation continuation;
        try (final RelationalResultSet resultSet = ps.executeQuery()) {
            ResultSetAssert assertResult = ResultSetAssert.assertThat(resultSet);
            for (Long value : values) {
                assertResult.hasNextRow().hasColumn("REST_NO", value);
            }
            assertResult.hasNoNextRow();
            continuation = resultSet.getContinuation();
        }
        return continuation;
    }

    private void assertContinuation(Continuation continuation, boolean atBegin, boolean atEnd) throws Exception {
        ContinuationImpl impl = (ContinuationImpl) continuation;
        Assertions.assertThat(impl.atBeginning()).isEqualTo(atBegin);
        Assertions.assertThat(impl.atEnd()).isEqualTo(atEnd);
        Assertions.assertThat(impl.getVersion()).isEqualTo(1);
        ContinuationProto proto = ContinuationProto.parseFrom(continuation.serialize());
        Assertions.assertThat(proto.getBindingHash()).isNotNull();
        Assertions.assertThat(proto.getBindingHash()).isNotZero();
        Assertions.assertThat(proto.getPlanHash()).isNotNull();
        Assertions.assertThat(proto.getPlanHash()).isNotZero();
    }

    private void executeInsert(Ddl ddl) throws SQLException {
        try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
            final int updateCount = statement.executeUpdate("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            Assertions.assertThat(updateCount).isEqualTo(5);
        }
    }
}
