/*
 * MetricsStatistics.java
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Statistical analysis of metrics differences for a set of queries.
 * Provides mean, median, standard deviation, and range calculations for each metric field.
 */
public final class MetricsStatistics {
    private final Map<String, FieldStatistics> fieldStatistics;
    private final Map<String, FieldStatistics> regressionFieldStatistics;

    private MetricsStatistics(@Nonnull final Map<String, FieldStatistics> fieldStatistics,
                              @Nonnull final Map<String, FieldStatistics> regressionFieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        this.regressionFieldStatistics = regressionFieldStatistics;
    }

    @Nonnull
    public FieldStatistics getFieldStatistics(@Nonnull final String fieldName) {
        return fieldStatistics.getOrDefault(fieldName, FieldStatistics.EMPTY);
    }

    @Nonnull
    public FieldStatistics getRegressionStatistics(@Nonnull final String fieldName) {
        return regressionFieldStatistics.getOrDefault(fieldName, FieldStatistics.EMPTY);
    }

    /**
     * Statistics for a single metrics field across all queries.
     */
    public static class FieldStatistics {
        @Nonnull
        public static final FieldStatistics EMPTY = new FieldStatistics(ImmutableList.of(), 0.0, 0.0);

        @Nonnull
        public final List<Long> sortedValues;
        public final double mean;
        public final double standardDeviation;

        private FieldStatistics(@Nonnull final List<Long> sortedValues,
                                final double mean,
                                final double standardDeviation) {
            this.sortedValues = ImmutableList.copyOf(sortedValues);
            this.mean = mean;
            this.standardDeviation = standardDeviation;
        }

        public boolean hasChanges() {
            return !sortedValues.isEmpty();
        }

        public int getChangedCount() {
            return sortedValues.size();
        }

        public double getMean() {
            return mean;
        }

        public long getMin() {
            return hasChanges() ? sortedValues.get(0) : 0L;
        }

        public long getMax() {
            return hasChanges() ? sortedValues.get(sortedValues.size() - 1) : 0L;
        }

        public double getStandardDeviation() {
            return standardDeviation;
        }

        /**
         * Returns the value for which {@code quantile} proportion of elements
         * are smaller. This is a generalization of the concept of "percentiles"
         * to real numbers.
         *
         * @param quantile the quantile to calculate
         * @return a value that separates the smallest {@code quantile} proportion of elements
         */
        public long getQuantile(double quantile) {
            if (sortedValues.isEmpty()) {
                return 0L;
            } else {
                int index = (int)(getChangedCount() * quantile);
                return sortedValues.get(index);
            }
        }

        public long getMedian() {
            return getQuantile(0.5);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for collecting metric differences and calculating statistics.
     */
    public static class Builder {
        @Nonnull
        private final Map<String, List<Long>> differences = new HashMap<>();
        @Nonnull
        private final Map<String, List<Long>> regressions = new HashMap<>();

        @Nonnull
        public Builder addDifference(@Nonnull final String fieldName, final long baseValue, final long headValue) {
            long difference = headValue - baseValue;
            differences.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(difference);
            if (difference > 0) {
                regressions.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(difference);
            }
            return this;
        }

        @Nonnull
        private Map<String, FieldStatistics> buildStats(@Nonnull final Map<String, List<Long>> baseMap) {
            final ImmutableMap.Builder<String, FieldStatistics> builder = ImmutableMap.builder();

            for (final var entry : baseMap.entrySet()) {
                final String fieldName = entry.getKey();
                final List<Long> values = entry.getValue();

                if (values.isEmpty()) {
                    continue;
                }

                // Sort values for quantile calculations
                Collections.sort(values);

                // Calculate statistics for values
                final var mean = values.stream().mapToLong(Long::longValue).average().orElse(0.0);
                final var variance = values.stream()
                        .mapToDouble(v -> Math.pow(v - mean, 2))
                        .average().orElse(0.0);
                final var standardDeviation = Math.sqrt(variance);

                builder.put(fieldName, new FieldStatistics(values, mean, standardDeviation));
            }

            return builder.build();
        }

        @Nonnull
        public MetricsStatistics build() {
            return new MetricsStatistics(buildStats(differences), buildStats(regressions));
        }
    }
}
