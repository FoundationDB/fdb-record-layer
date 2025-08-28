/*
 * RecordLayerMetricCollectorTest.java
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

package com.apple.foundationdb.relational.recordlayer.metric;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.function.Consumer;

public class RecordLayerMetricCollectorTest {

    private static final String schemaTemplate =
            "CREATE TABLE simple_table(a bigint, PRIMARY KEY(a))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void testPlanCacheMetrics() throws Exception {
        setupAndExecuteWithConnection((connection) -> {
            executeSimpleSelectAndReturnContinuation(connection, false, false);
            executeSimpleSelectAndReturnContinuation(connection, false, true);
        });
    }

    @Test
    void testContinuedPlanMetrics() throws Exception {
        setupAndExecuteWithConnection((connection) -> {
            Continuation continuation = executeSimpleSelectAndReturnContinuation(connection, true, false);
            try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                statement.setBytes("continuation", continuation.serialize());
                Assertions.assertTrue(statement.execute(), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                    for (int i = 0; i < 5; i++) {
                        resultSetAssert.hasNextRow();
                    }
                    resultSetAssert.hasNoNextRow();
                    var collector = connection.getMetricCollector();
                    testGeneralMetrics(collector);
                    testExecuteContinuationSpecificMetrics(collector);
                }
            } catch (SQLException sql) {
                Assertions.fail(sql);
            }
        });
    }

    private Continuation executeSimpleSelectAndReturnContinuation(EmbeddedRelationalConnection connection, boolean limitMaxRows, boolean hitCache) {
        Continuation continuation = null;
        try (var statement = connection.createStatement()) {
            if (limitMaxRows) {
                statement.setMaxRows(5);
            }
            Assertions.assertTrue(statement.execute("SELECT * FROM simple_table"), "Did not return a result set from a select statement!");
            try (final RelationalResultSet resultSet = statement.getResultSet()) {
                final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                for (int i = 0; i < (limitMaxRows ? 5 : 10); i++) {
                    resultSetAssert.hasNextRow();
                }
                resultSetAssert.hasNoNextRow();
                continuation = resultSet.getContinuation();
                var collector = connection.getMetricCollector();
                testGeneralMetrics(collector);
                if (!hitCache) {
                    testCacheMissSpecificMetrics(collector);
                } else {
                    testCacheHitSpecificMetrics(collector);
                }
            }
        } catch (SQLException sql) {
            Assertions.fail(sql);
        }
        return continuation;
    }

    private void setupAndExecuteWithConnection(Consumer<EmbeddedRelationalConnection> execute) throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/METRIC_COLLECTOR_TESTS")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection().unwrap(EmbeddedRelationalConnection.class);
            try (var statement = connection.createStatement()) {
                for (int i = 0; i < 10; i++) {
                    statement.execute("INSERT INTO simple_table(a) VALUES (" + i + ")");
                }
            }
            execute.accept(connection);
        }
    }

    private static void testGeneralMetrics(@Nonnull MetricCollector collector) {
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.LEX_PARSE),
                "LEX_PARSE event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.NORMALIZE_QUERY),
                "NORMALIZE_QUERY event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.EXECUTE_RECORD_QUERY_PLAN),
                "EXECUTE_RECORD_QUERY_PLAN event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.CREATE_RESULT_SET_ITERATOR),
                "CREATE_RESULT_SET_ITERATOR event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.TOTAL_EXECUTE_QUERY),
                "TOTAL_EXECUTE_QUERY event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.TOTAL_PROCESS_QUERY),
                "TOTAL_PROCESS_QUERY event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.TOTAL_GET_PLAN_QUERY),
                "TOTAL_GET_PLAN_QUERY event should be registered with the metricCollector");
    }

    private static void testCacheMissSpecificMetrics(@Nonnull MetricCollector collector) {
        // true event
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.CACHE_LOOKUP),
                "CACHE_LOOKUP event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_LOGICAL_PLAN),
                "GENERATE_LOGICAL_PLAN event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.OPTIMIZE_PLAN),
                "OPTIMIZE_PLAN event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS),
                "PLAN_CACHE_SECONDARY_MISS event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS),
                "PLAN_CACHE_TERTIARY_MISS event should be registered with the metricCollector");
        // false events
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_CONTINUED_PLAN),
                "GENERATE_CONTINUED_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT),
                "PLAN_CACHE_TERTIARY_HIT event should not be registered with the metricCollector");
    }

    private static void testCacheHitSpecificMetrics(@Nonnull MetricCollector collector) {
        // false events
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_CONTINUED_PLAN),
                "GENERATE_CONTINUED_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_LOGICAL_PLAN),
                "GENERATE_LOGICAL_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.OPTIMIZE_PLAN),
                "OPTIMIZE_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS),
                "PLAN_CACHE_PRIMARY_MISS event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS),
                "PLAN_CACHE_SECONDARY_MISS event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS),
                "PLAN_CACHE_TERTIARY_MISS event should not be registered with the metricCollector");
        // true event
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.CACHE_LOOKUP),
                "CACHE_LOOKUP event should be registered with the metricCollector");
        Assertions.assertDoesNotThrow(() -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT),
                "PLAN_CACHE_TERTIARY_HIT event should be registered with the metricCollector");
    }

    private static void testExecuteContinuationSpecificMetrics(@Nonnull MetricCollector collector) {
        // false events
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_LOGICAL_PLAN),
                "GENERATE_LOGICAL_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.OPTIMIZE_PLAN),
                "OPTIMIZE_PLAN event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS),
                "PLAN_CACHE_PRIMARY_MISS event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS),
                "PLAN_CACHE_SECONDARY_MISS event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS),
                "PLAN_CACHE_TERTIARY_MISS event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.CACHE_LOOKUP),
                "CACHE_LOOKUP event event should not be registered with the metricCollector");
        Assertions.assertThrows(UncheckedRelationalException.class, () -> collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT),
                "PLAN_CACHE_TERTIARY_HIT event should not be registered with the metricCollector");
        // true event
        Assertions.assertDoesNotThrow(() -> collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.GENERATE_CONTINUED_PLAN),
                "GENERATE_CONTINUED_PLAN event should be registered with the metricCollector");
    }
}
