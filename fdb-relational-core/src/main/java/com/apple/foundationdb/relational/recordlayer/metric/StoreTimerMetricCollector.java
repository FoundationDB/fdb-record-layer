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

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A {@link MetricCollector} backed by a per-event lookup of {@link StoreTimer}. The lookup can be:
 * <ul>
 *   <li>{@link #fromFDBRecordContext(FDBRecordContext)} &mdash; delegates to
 *       {@link FDBRecordContext#getTimerForEvent(StoreTimer.Event)}, which routes an event to the
 *       transaction's normal timer or its delayed timer according to
 *       {@link StoreTimer.Event#isDelayedUntilCommit()}. The collector is then scoped to that
 *       transaction's metrics.</li>
 *   <li>{@link #fromMetricRegistry(MetricRegistry)} &mdash; constructs a single
 *       {@link MetricRegistryStoreTimer} over the supplied Codahale {@link MetricRegistry} and
 *       hands the same instance back for every event. Suitable for non-transactional paths (e.g.
 *       engine startup work) that still want their metrics surfaced on the engine-wide
 *       registry.</li>
 * </ul>
 *
 * <p>Writes ({@link #increment}, {@link #clock}) resolve the timer per call and no-op if the
 * lookup returns {@code null}. Reads ({@link #getAverageTimeMicrosForEvent},
 * {@link #getCountsForCounter}, {@link #hasCounter}) resolve the timer per call too; they throw
 * or return {@code false} if the lookup returns {@code null}.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class StoreTimerMetricCollector implements MetricCollector {

    // The lookup's result is @Nullable: FDBRecordContext#getTimerForEvent(...) can return null when no timer
    // is attached to the context (e.g. a transaction-bound database context) -- see fromFDBRecordContext().
    private final Function<StoreTimer.Event, @Nullable StoreTimer> timerLookup;

    private StoreTimerMetricCollector(final Function<StoreTimer.Event, @Nullable StoreTimer> timerLookup) {
        this.timerLookup = timerLookup;
    }

    /**
     * Routes each event through {@link FDBRecordContext#getTimerForEvent(StoreTimer.Event)} so
     * that events with {@link StoreTimer.Event#isDelayedUntilCommit()} land on the transaction's
     * delayed timer while all others use the normal timer.
     *
     * <p>If {@code context} has no timer attached (e.g. a transaction-bound database context),
     * the lookup returns {@code null} and the collector silently drops writes; reads throw.</p>
     */
    public static StoreTimerMetricCollector fromFDBRecordContext(final FDBRecordContext context) {
        return new StoreTimerMetricCollector(context::getTimerForEvent);
    }

    /**
     * Builds a fresh {@link MetricRegistryStoreTimer} over {@code registry} once, and hands it
     * back for every event lookup. All metrics land on the supplied Codahale registry.
     */
    public static StoreTimerMetricCollector fromMetricRegistry(final MetricRegistry registry) {
        final StoreTimer timer = new MetricRegistryStoreTimer(registry);
        return new StoreTimerMetricCollector(event -> timer);
    }

    @Override
    public void increment(final RelationalMetric.RelationalCount count, final int val) {
        @Nullable final StoreTimer timer = timerLookup.apply(count);
        if (timer != null) {
            timer.increment(count, val);
        }
    }

    @Override
    public <T extends @Nullable Object> T clock(final RelationalMetric.RelationalEvent event, final Supplier<T> supplier) throws RelationalException {
        final long startNanos = System.nanoTime();
        try {
            return supplier.get();
        } finally {
            @Nullable final StoreTimer timer = timerLookup.apply(event);
            if (timer != null) {
                timer.record(event, System.nanoTime() - startNanos);
            }
        }
    }

    @Override
    public double getAverageTimeMicrosForEvent(final RelationalMetric.RelationalEvent event) {
        final StoreTimer timerOrNull = timerLookup.apply(event);
        // Reassign through notNullUnchecked (rather than discarding its result) so the non-null check
        // actually narrows the value used below; Assert.notNullUnchecked can't narrow it on its own since
        // Assert lives in the not-yet-migrated fdb-relational-api module.
        @SuppressWarnings("NullAway")
        final StoreTimer timer = Assert.notNullUnchecked(timerOrNull, ErrorCode.INTERNAL_ERROR,
                "Cannot read metrics: no backing store timer for event %s", event.title());
        final StoreTimer.Counter maybeCounterOrNull = timer.getCounter(event);
        @SuppressWarnings("NullAway")
        final StoreTimer.Counter maybeCounter = Assert.notNullUnchecked(maybeCounterOrNull, ErrorCode.INTERNAL_ERROR,
                "Cannot find metrics associated for requested event: %s", event.title());
        if (maybeCounter.getCount() == 0) {
            return 0.0;
        }
        return ((double) TimeUnit.NANOSECONDS.toMicros(maybeCounter.getTimeNanos())) / maybeCounter.getCount();
    }

    @Override
    public long getCountsForCounter(final RelationalMetric.RelationalCount count) {
        final StoreTimer timerOrNull = timerLookup.apply(count);
        @SuppressWarnings("NullAway")
        final StoreTimer timer = Assert.notNullUnchecked(timerOrNull, ErrorCode.INTERNAL_ERROR,
                "Cannot read metrics: no backing store timer for count %s", count.title());
        Assert.thatUnchecked(hasCounter(count), ErrorCode.INTERNAL_ERROR,
                "Cannot find metrics associated for requested event: %s", count.title());
        // hasCounter(count) (just asserted true above) already established that timer.getCounter(count) !=
        // null; NullAway can't track that across the Assert.thatUnchecked call since Assert lives in the
        // not-yet-migrated fdb-relational-api module.
        @SuppressWarnings("NullAway")
        final StoreTimer.Counter counter = Objects.requireNonNull(timer.getCounter(count));
        Assert.thatUnchecked(counter.getTimeNanos() == 0, ErrorCode.INTERNAL_ERROR,
                "Event: %s records time and is probably a event timer", count.title());
        return counter.getCount();
    }

    @Override
    public boolean hasCounter(final RelationalMetric.RelationalCount count) {
        @Nullable final StoreTimer timer = timerLookup.apply(count);
        return timer != null && timer.getCounter(count) != null;
    }
}
