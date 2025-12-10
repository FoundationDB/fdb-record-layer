/*
 * MetricsStatisticsTest.java
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

package com.apple.foundationdb.relational.yamltests;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

class MetricsStatisticsTest {

    @Test
    void testEmptyStatistics() {
        final var stats = MetricsStatistics.newBuilder().build();
        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats).isEqualTo(MetricsStatistics.FieldStatistics.EMPTY);
        assertThat(fieldStats.hasChanges()).isFalse();
        assertThat(fieldStats.getChangedCount()).isZero();
        assertThat(fieldStats.getMean()).isZero();
        assertThat(fieldStats.getMedian()).isZero();
        assertThat(fieldStats.getQuantile(0.1)).isZero();
        assertThat(fieldStats.getStandardDeviation()).isZero();
        assertThat(fieldStats.getMin()).isZero();
        assertThat(fieldStats.getMax()).isZero();
    }

    @Test
    void testSingleValueStatistics() {
        final var stats = MetricsStatistics.newBuilder()
                .addDifference("task_count", 10L, 20L)
                .build();

        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.hasChanges()).isTrue();
        assertThat(fieldStats.getChangedCount()).isOne();
        assertThat(fieldStats.getMean()).isEqualTo(10.0);
        assertThat(fieldStats.getMedian()).isEqualTo(10);
        assertThat(fieldStats.getStandardDeviation()).isZero(); // Single value has no deviation
        assertThat(fieldStats.getQuantile(0.1)).isEqualTo(10L);
        assertThat(fieldStats.getMin()).isEqualTo(10L);
        assertThat(fieldStats.getMax()).isEqualTo(10L);
    }

    @Test
    void testMultipleValuesStatistics() {
        // Add values: 2, 4, 6, 8, 10
        // Mean = 6.0, Median = 6.0, StdDev = sqrt(8) ≈ 2.83
        final var stats = MetricsStatistics.newBuilder()
                .addDifference("task_count", 2L, 4L)
                .addDifference("task_count", 4L, 8L)
                .addDifference("task_count", 6L, 12L)
                .addDifference("task_count", 8L, 16L)
                .addDifference("task_count", 10L, 20L)
                .build();

        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.hasChanges()).isTrue();
        assertThat(fieldStats.getChangedCount()).isEqualTo(5);
        assertThat(fieldStats.getMean()).isEqualTo(6.0);
        assertThat(fieldStats.getMedian()).isEqualTo(6L);
        assertThat(fieldStats.getStandardDeviation()).isCloseTo(2.83, offset(0.01));
        assertThat(fieldStats.getQuantile(0.2)).isEqualTo(4L);
        assertThat(fieldStats.getMin()).isEqualTo(2L);
        assertThat(fieldStats.getMax()).isEqualTo(10L);
    }

    @Test
    void testEvenNumberOfValuesMedian() {
        // Add values: 1, 3, 5, 7
        // Median should be (3 + 5) / 2 = 4.0
        final var stats = MetricsStatistics.newBuilder()
                .addDifference("task_count", 1L, 2L)
                .addDifference("task_count", 3L, 6L)
                .addDifference("task_count", 5L, 10L)
                .addDifference("task_count", 7L, 14L)
                .build();

        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.getMedian()).isEqualTo(5L);
        assertThat(fieldStats.getMean()).isEqualTo(4.0);
    }

    @Test
    void testNegativeValues() {
        // Add both positive and negative values: -10, -5, 0, 5, 10
        // Mean = 0.0, Median = 0.0
        final var stats = MetricsStatistics.newBuilder()
                .addDifference("task_count", 50L, 40L)
                .addDifference("task_count", 40L, 35L)
                .addDifference("task_count", 30L, 30L)
                .addDifference("task_count", 20L, 25L)
                .addDifference("task_count", 10L, 20L)
                .build();

        // For field statistics, the sign matters, and the positive and negative
        // cancel out
        final var fieldStats = stats.getFieldStatistics("task_count");
        assertThat(fieldStats.getChangedCount()).isEqualTo(5);
        assertThat(fieldStats.getMean()).isZero();
        assertThat(fieldStats.getMedian()).isZero();
        assertThat(fieldStats.getMin()).isEqualTo(-10L);
        assertThat(fieldStats.getMax()).isEqualTo(10L);

        // For regressions, we look only at positive changes, ignoring any negative
        // or zero values
        final var regressionStats = stats.getRegressionStatistics("task_count");
        assertThat(regressionStats.getChangedCount()).isEqualTo(2);
        assertThat(regressionStats.getMean()).isCloseTo(7.5, offset(0.01));
        assertThat(regressionStats.getMedian()).isEqualTo(10L);
        assertThat(regressionStats.getMin()).isEqualTo(5L);
        assertThat(regressionStats.getMax()).isEqualTo(10L);
    }

    @Test
    void testMultipleFields() {
        final var builder = new MetricsStatistics.Builder();

        // Add different values for different fields
        builder.addDifference("task_count", 10L, 20L);
        builder.addDifference("task_count", 10L, 30L);

        builder.addDifference("transform_count", 5L, 10L);
        builder.addDifference("transform_count", 5L, 20L);
        builder.addDifference("transform_count", 5L, 30L);

        final var stats = builder.build();

        // Check task_count statistics
        final var taskStats = stats.getFieldStatistics("task_count");
        assertThat(taskStats.getChangedCount()).isEqualTo(2);
        assertThat(taskStats.getMean()).isCloseTo(15.0, offset(0.01));
        assertThat(taskStats.getMedian()).isEqualTo(20L);

        // Check transform_count statistics
        final var transformStats = stats.getFieldStatistics("transform_count");
        assertThat(transformStats.getChangedCount()).isEqualTo(3);
        assertThat(transformStats.getMean()).isCloseTo(15.0, offset(0.01));
        assertThat(transformStats.getMedian()).isEqualTo(15L);

        // Check non-existent field
        final var emptyStats = stats.getFieldStatistics("non_existent");
        assertThat(emptyStats).isEqualTo(MetricsStatistics.FieldStatistics.EMPTY);
    }

    @Test
    void testLargeDataset() {
        final var builder = new MetricsStatistics.Builder();

        // Add 1000 values from 1 to 1000
        for (long i = 1; i <= 1000; i++) {
            builder.addDifference("task_count", 10L, 10L + i);
        }

        final var stats = builder.build();
        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.getChangedCount()).isEqualTo(1000);
        assertThat(fieldStats.getMean()).isCloseTo(500.5, offset(0.1));
        assertThat(fieldStats.getMedian()).isEqualTo(501L);
        assertThat(fieldStats.getMin()).isOne();
        assertThat(fieldStats.getMax()).isEqualTo(1000);

        // Standard deviation for 1 to 1000 should be approximately 288.7
        assertThat(fieldStats.getStandardDeviation()).isCloseTo(288.7, offset(1.0));
    }

    @Test
    void testDuplicateValues() {
        final var builder = new MetricsStatistics.Builder();

        // Add same value multiple times
        builder.addDifference("task_count", 10L, 15L);
        builder.addDifference("task_count", 20L, 25L);
        builder.addDifference("task_count", 30L, 35L);
        builder.addDifference("task_count", 40L, 50L);
        builder.addDifference("task_count", 50L, 60L);

        final var stats = builder.build();
        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.getChangedCount()).isEqualTo(5);
        assertThat(fieldStats.getMean()).isCloseTo(7.0, offset(0.01)); // (5+5+5+10+10)/5 = 7
        assertThat(fieldStats.getMedian()).isEqualTo(5L); // Middle value when sorted: [5,5,5,10,10]
        assertThat(fieldStats.getQuantile(0.55)).isEqualTo(5L);
        assertThat(fieldStats.getQuantile(0.65)).isEqualTo(10L);
        assertThat(fieldStats.getMin()).isEqualTo(5L);
        assertThat(fieldStats.getMax()).isEqualTo(10L);
    }

    @Test
    void testZeroValues() {
        final var builder = new MetricsStatistics.Builder();

        // Add zeros and non-zeros
        builder.addDifference("task_count", 0L, 0L);
        builder.addDifference("task_count", 0L, 1L);
        builder.addDifference("task_count", 1L, 2L);

        final var stats = builder.build();
        final var fieldStats = stats.getFieldStatistics("task_count");

        assertThat(fieldStats.getChangedCount()).isEqualTo(3);
        assertThat(fieldStats.getMean()).isCloseTo(0.67, offset(0.01));
        assertThat(fieldStats.getMedian()).isOne();
        assertThat(fieldStats.getMin()).isZero();
        assertThat(fieldStats.getMax()).isOne();
    }
}
