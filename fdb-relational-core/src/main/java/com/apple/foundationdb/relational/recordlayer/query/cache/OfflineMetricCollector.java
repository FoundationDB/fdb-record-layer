/*
 * OfflineMetricCollector.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.util.MetricRegistryStoreTimer;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Supplier;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * A {@link MetricCollector} for offline planning paths that have no transaction context.
 *
 * <p>Mirrors {@link com.apple.foundationdb.relational.recordlayer.metric.RecordLayerMetricCollector}'s
 * design but owns its own {@link MetricRegistryStoreTimer} (rather than borrowing one from an
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}). Increments and
 * timings are forwarded to the wrapped {@link MetricRegistry} as Codahale Counter/Timer updates,
 * so they end up alongside online-query metrics under the same event names.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class OfflineMetricCollector implements MetricCollector {

    @Nonnull
    private final MetricRegistryStoreTimer storeTimer;

    public OfflineMetricCollector(@Nonnull final MetricRegistry registry) {
        this.storeTimer = new MetricRegistryStoreTimer(registry);
    }

    @Override
    public void increment(@Nonnull final RelationalMetric.RelationalCount count, final int val) {
        storeTimer.increment(count, val);
    }

    @Override
    public <T> T clock(@Nonnull final RelationalMetric.RelationalEvent event, final Supplier<T> supplier) throws RelationalException {
        final long startNanos = System.nanoTime();
        try {
            return supplier.get();
        } finally {
            storeTimer.record(event, System.nanoTime() - startNanos);
        }
    }

    @Override
    public double getAverageTimeMicrosForEvent(@Nonnull final RelationalMetric.RelationalEvent event) {
        final StoreTimer.Counter maybeCounter = storeTimer.getCounter(event);
        Assert.notNullUnchecked(maybeCounter, ErrorCode.INTERNAL_ERROR,
                "Cannot find metrics associated for requested event: %s", event.title());
        if (maybeCounter.getCount() == 0) {
            return 0.0;
        }
        return ((double) TimeUnit.NANOSECONDS.toMicros(maybeCounter.getTimeNanos())) / maybeCounter.getCount();
    }

    @Override
    public long getCountsForCounter(@Nonnull final RelationalMetric.RelationalCount count) {
        Assert.thatUnchecked(hasCounter(count), ErrorCode.INTERNAL_ERROR,
                "Cannot find metrics associated for requested event: %s", count.title());
        final StoreTimer.Counter counter = storeTimer.getCounter(count);
        Assert.thatUnchecked(counter.getTimeNanos() == 0, ErrorCode.INTERNAL_ERROR,
                "Event: %s records time and is probably a event timer", count.title());
        return counter.getCount();
    }

    @Override
    public boolean hasCounter(@Nonnull final RelationalMetric.RelationalCount count) {
        return storeTimer.getCounter(count) != null;
    }
}
