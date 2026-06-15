/*
 * OfflineMetricCollectorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OfflineMetricCollectorTest {

    @Test
    void hasCounterReturnsFalseBeforeAndTrueAfterIncrement() {
        final var collector = new OfflineMetricCollector(new MetricRegistry());
        Assertions.assertFalse(collector.hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));

        collector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT, 1);
        Assertions.assertTrue(collector.hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
    }

    @Test
    void getCountsForCounterReturnsAccumulatedValue() {
        final var collector = new OfflineMetricCollector(new MetricRegistry());
        collector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT, 2);
        collector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT, 3);
        Assertions.assertEquals(5L, collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
    }

    @Test
    void getCountsForCounterThrowsWhenAbsent() {
        final var collector = new OfflineMetricCollector(new MetricRegistry());
        Assertions.assertThrows(UncheckedRelationalException.class, () ->
                collector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
    }

    @Test
    void getAverageTimeMicrosForEventReturnsAverageOfClockedSamples() throws Exception {
        final var collector = new OfflineMetricCollector(new MetricRegistry());
        collector.clock(RelationalMetric.RelationalEvent.LEX_PARSE, () -> null);
        collector.clock(RelationalMetric.RelationalEvent.LEX_PARSE, () -> null);
        // Two samples were recorded; both ran to completion, so the average must be a finite non-negative number.
        final double averageMicros = collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.LEX_PARSE);
        Assertions.assertTrue(averageMicros >= 0.0,
                "average time should be non-negative, got: " + averageMicros);
    }

    @Test
    void getAverageTimeMicrosForEventThrowsWhenAbsent() {
        final var collector = new OfflineMetricCollector(new MetricRegistry());
        Assertions.assertThrows(UncheckedRelationalException.class, () ->
                collector.getAverageTimeMicrosForEvent(RelationalMetric.RelationalEvent.LEX_PARSE));
    }
}
