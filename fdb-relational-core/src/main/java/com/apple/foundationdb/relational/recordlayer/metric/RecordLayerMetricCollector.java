/*
 * RecordLayerMetricCollector.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Supplier;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The RecordLayerMetricCollector is an implementation of MetricCollector that is bound to a {@link FDBRecordContext}
 * object and hence scoped to a single transaction. It hence maintains metrics pertaining to a single ongoing txn.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordLayerMetricCollector implements MetricCollector {

    @Nonnull
    private final FDBRecordContext context;

    public RecordLayerMetricCollector(@Nonnull FDBRecordContext fdbRecordContext) {
        this.context = fdbRecordContext;
    }

    @Override
    public void increment(@Nonnull RelationalMetric.RelationalCount count) {
        context.increment(count);
    }

    @Override
    public <T> T clock(@Nonnull RelationalMetric.RelationalEvent event, Supplier<T> supplier) throws RelationalException {
        final var startTime = System.nanoTime();
        T result;
        try {
            result = supplier.get();
            return result;
        } finally {
            recordSinceNanoTime(event, startTime);
        }
    }

    private void recordSinceNanoTime(@Nonnull RelationalMetric.RelationalEvent event, long startTimeNanos) {
        context.record(event, System.nanoTime() - startTimeNanos);
    }

    @Override
    public double getAverageTimeMicrosForEvent(@Nonnull RelationalMetric.RelationalEvent event) {
        final var maybeCounter = context.getTimer().getCounter(event);
        Assert.notNullUnchecked(maybeCounter, ErrorCode.INTERNAL_ERROR, "Cannot find metrics associated for requested event: %s", event.title());
        if (maybeCounter.getCount() == 0) {
            return 0.0;
        }
        return ((double) TimeUnit.NANOSECONDS.toMicros(maybeCounter.getTimeNanos())) / maybeCounter.getCount();
    }

    @Override
    public long getCountsForCounter(@Nonnull RelationalMetric.RelationalCount count) {
        Assert.thatUnchecked(hasCounter(count), ErrorCode.INTERNAL_ERROR, "Cannot find metrics associated for requested event: %s", count.title());
        final var counter = getCounter(count);
        Assert.thatUnchecked(counter.getTimeNanos() == 0, ErrorCode.INTERNAL_ERROR, "Event: %s records time and is probably a event timer", count.title());
        return counter.getCount();
    }

    @Override
    public boolean hasCounter(@Nonnull RelationalMetric.RelationalCount count) {
        return getCounter(count) != null;
    }

    @Nullable
    private StoreTimer.Counter getCounter(@Nonnull RelationalMetric.RelationalCount count) {
        return getUnderlyingStoreTimer().getCounter(count);
    }

    @Nonnull
    @VisibleForTesting
    public StoreTimer getUnderlyingStoreTimer() {
        return Objects.requireNonNull(context.getTimer());
    }
}
