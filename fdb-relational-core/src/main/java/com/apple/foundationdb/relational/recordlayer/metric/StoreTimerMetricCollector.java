/*
 * StoreTimerMetricCollector.java
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

package com.apple.foundationdb.relational.recordlayer.metric;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.util.MetricRegistryStoreTimer;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Supplier;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link MetricCollector} backed by a {@link StoreTimer}. The timer can be:
 * <ul>
 *   <li>borrowed from an {@link FDBRecordContext} via {@link #fromFDBRecordContext(FDBRecordContext)} — the
 *       collector is then scoped to that transaction's metrics; or</li>
 *   <li>built from a Codahale {@link MetricRegistry} via {@link #fromMetricRegistry(MetricRegistry)} —
 *       suitable for non-transactional paths (e.g. engine startup work) that still want their
 *       metrics surfaced on the engine-wide registry.</li>
 * </ul>
 *
 * <p>Increments and timings are forwarded straight to the wrapped {@link StoreTimer}; the
 * accessors ({@link #getAverageTimeMicrosForEvent}, {@link #getCountsForCounter},
 * {@link #hasCounter}) read back from the same source.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class StoreTimerMetricCollector implements MetricCollector {

    @Nullable
    private final StoreTimer storeTimer;

    private StoreTimerMetricCollector(@Nullable final StoreTimer storeTimer) {
        this.storeTimer = storeTimer;
    }

    /**
     * Wraps the timer attached to {@code context}, so this collector's metrics are scoped to the
     * given transaction.
     *
     * <p>If {@code context} has no timer attached (e.g. a transaction-bound database context),
     * the collector silently drops writes and reads throw &mdash; mirroring the way
     * {@link FDBRecordContext#increment(StoreTimer.Count, int)} handles a null timer.</p>
     */
    @Nonnull
    public static StoreTimerMetricCollector fromFDBRecordContext(@Nonnull final FDBRecordContext context) {
        return new StoreTimerMetricCollector(context.getTimer());
    }

    /**
     * Builds a fresh {@link MetricRegistryStoreTimer} over {@code registry} and wraps it, so this
     * collector's metrics land on the supplied Codahale registry.
     */
    @Nonnull
    public static StoreTimerMetricCollector fromMetricRegistry(@Nonnull final MetricRegistry registry) {
        return new StoreTimerMetricCollector(new MetricRegistryStoreTimer(registry));
    }

    @Override
    public void increment(@Nonnull final RelationalMetric.RelationalCount count, final int val) {
        if (storeTimer != null) {
            storeTimer.increment(count, val);
        }
    }

    @Override
    public <T> T clock(@Nonnull final RelationalMetric.RelationalEvent event, final Supplier<T> supplier) throws RelationalException {
        final long startNanos = System.nanoTime();
        try {
            return supplier.get();
        } finally {
            if (storeTimer != null) {
                storeTimer.record(event, System.nanoTime() - startNanos);
            }
        }
    }

    @Override
    public double getAverageTimeMicrosForEvent(@Nonnull final RelationalMetric.RelationalEvent event) {
        Assert.notNullUnchecked(storeTimer, ErrorCode.INTERNAL_ERROR,
                "Cannot read metrics: this collector has no backing store timer");
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
        Assert.notNullUnchecked(storeTimer, ErrorCode.INTERNAL_ERROR,
                "Cannot read metrics: this collector has no backing store timer");
        Assert.thatUnchecked(hasCounter(count), ErrorCode.INTERNAL_ERROR,
                "Cannot find metrics associated for requested event: %s", count.title());
        final StoreTimer.Counter counter = storeTimer.getCounter(count);
        Assert.thatUnchecked(counter.getTimeNanos() == 0, ErrorCode.INTERNAL_ERROR,
                "Event: %s records time and is probably a event timer", count.title());
        return counter.getCount();
    }

    @Override
    public boolean hasCounter(@Nonnull final RelationalMetric.RelationalCount count) {
        return storeTimer != null && storeTimer.getCounter(count) != null;
    }
}
