/*
 * MetricRegistryStoreTimer.java
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.util.MapUtils;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer} that defers
 * actual metric collection to a {@link com.codahale.metrics.MetricRegistry}.
 *
 * Relational at the upper levels uses MetricRegistry instances to manage its metrics so that metric values
 * can be exposed as psuedo-tables and stored procedures and also to integrate with metrics
 * collection systems. RecordLayer uses a StoreTimer, and so it's necessary that we pass RecordLayer an adapted
 * StoreTimer instance that will pass through the StoreTimer metrics to the MetricRegistry. This class provides
 * that adaptation.
 */
@API(API.Status.EXPERIMENTAL)
public class MetricRegistryStoreTimer extends FDBStoreTimer {
    private final MetricRegistry registry;

    public MetricRegistryStoreTimer(MetricRegistry registry) {
        this.registry = registry;
    }

    @Nullable
    @Override
    protected Counter getCounter(@Nonnull Event event, boolean createIfNotExists) {
        if (event instanceof Aggregate) {
            @Nullable Counter counter = ((Aggregate) event).compute(this);
            if (counter == null && createIfNotExists) {
                //TODO(bfines) this is an awkward way of forcing the use of the private field ZERO_COUNTER in the superclass
                return super.getCounter(event, true);
            }
            return counter;
        } else {
            if (event instanceof Count) {
                return createIfNotExists ?
                        MapUtils.computeIfAbsent(counters, event,
                                ignore -> new RegistryCounter(event.title(), registry.counter(event.title()))) :
                        counters.get(event);
            } else {
                return createIfNotExists ?
                        MapUtils.computeIfAbsent(counters, event,
                                ignore -> new RegistryTimer(event.title(), registry.timer(event.title()))) :
                        counters.get(event);
            }
        }
    }

    @Nullable
    @Override
    protected Counter getTimeoutCounter(@Nonnull Event event, boolean createIfNotExists) {
        if (event instanceof Count) {
            return MapUtils.computeIfAbsent(counters, event,
                    evignore -> new RegistryCounter(event.title(), registry.counter(event.title())));
        } else {
            return MapUtils.computeIfAbsent(counters, event,
                    evignore -> new RegistryTimer(event.title(), registry.timer(event.title())));
        }
    }

    @Override
    public void reset() {
        registry.removeMatching(MetricFilter.ALL);
        super.reset();
    }

    private static class RegistryCounter extends Counter {
        private final com.codahale.metrics.Counter counter;
        private final String name;

        public RegistryCounter(@Nonnull String name, com.codahale.metrics.Counter counter) {
            super(false);
            this.name = name;
            this.counter = counter;
        }

        @Override
        public void record(long timeDifferenceNanos) {
            throw new UnsupportedOperationException("Programmer error: Counter(name = " + name + ") is not a timer");
        }

        @Override
        public void increment(int amount) {
            counter.inc(amount);
            super.increment(amount);
        }

    }

    private static class RegistryTimer extends Counter {
        private final com.codahale.metrics.Timer timer;

        private final String name;

        public RegistryTimer(@Nonnull String name, com.codahale.metrics.Timer timer) {
            super(false);
            this.name = name;
            this.timer = timer;
        }

        @Override
        public void record(long timeDifferenceNanos) {
            timer.update(timeDifferenceNanos, TimeUnit.NANOSECONDS);
            super.record(timeDifferenceNanos);
        }

        @Override
        public void increment(int amount) {
            throw new UnsupportedOperationException("Programmer error: Timer(name = " + name + ") is not a counter");
        }

    }
}
