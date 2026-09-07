/*
 * MetricsInfo.java
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

import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.protobuf.Descriptors;

import java.nio.file.Path;
import java.util.Objects;

public final class MetricsInfo {
    private final PlannerMetricsProto.Info underlying;
    private final Path filePath;
    private final int lineNumber;

    MetricsInfo(PlannerMetricsProto.Info metricsInfo,
                Path filePath,
                int lineNumber) {
        this.underlying = metricsInfo;
        this.filePath = filePath;
        this.lineNumber = lineNumber;
    }

    public PlannerMetricsProto.Info getUnderlying() {
        return underlying;
    }

    public Path getFilePath() {
        return filePath;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public String getExplain() {
        return underlying.getExplain();
    }

    public PlannerMetricsProto.CountersAndTimers getCountersAndTimers() {
        return underlying.getCountersAndTimers();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        final MetricsInfo that = (MetricsInfo)object;
        return lineNumber == that.lineNumber && Objects.equals(underlying, that.underlying) && Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(underlying, filePath, lineNumber);
    }

    /**
     * Compares two CountersAndTimers and determines if any of the tracked metrics are different.
     * This method checks the core metrics that are used for planner comparison but excludes timing
     * information as those can vary between runs.
     *
     * @param expected the expected metrics values
     * @param actual the actual metrics values
     * @return true if any of the tracked metrics differ
     */
    public static boolean areMetricsDifferent(final MetricsInfo expected,
                                              final MetricsInfo actual) {
        final var metricsDescriptor = PlannerMetricsProto.CountersAndTimers.getDescriptor();

        return YamlMetricsMaintainer.TRACKED_METRIC_FIELDS.stream()
                .map(metricsDescriptor::findFieldByName)
                .anyMatch(field -> isMetricDifferent(expected, actual, field));
    }

    /**
     * Compares a specific metric field between expected and actual values.
     *
     * @param expected the expected metrics
     * @param actual the actual metrics
     * @param fieldDescriptor the field to compare
     * @return true if the metric values differ
     */
    private static boolean isMetricDifferent(final MetricsInfo expected,
                                             final MetricsInfo actual,
                                             final Descriptors.FieldDescriptor fieldDescriptor) {
        final long expectedMetric = (long) expected.getUnderlying().getCountersAndTimers().getField(fieldDescriptor);
        final long actualMetric = (long) actual.getUnderlying().getCountersAndTimers().getField(fieldDescriptor);
        return expectedMetric != actualMetric;
    }
}
