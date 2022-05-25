/*
 * NoOpMetricRegistry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metrics;

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A No-Op MetricRegistry.
 * <p>
 * NOTE(bfines); This could be replaced by an officially supported one if/when we ever
 * upgrade to the dropwizard version of the library (i.e. 4.x or higher, instead of staying on 3.x).
 */
//the fields are static subclasses and it would make an unreadable hash of the source to have them be first
@SuppressWarnings("PMD.FieldDeclarationsShouldBeAtStartOfClass")
@ExcludeFromJacocoGeneratedReport //it doesn't do anything to be tested
public final class NoOpMetricRegistry extends MetricRegistry {

    public static final MetricRegistry INSTANCE = new NoOpMetricRegistry();

    private NoOpMetricRegistry() {

    }

    @Override
    public Counter counter(String name) {
        return NOOP_COUNTER;
    }

    @Override
    public Histogram histogram(String name) {
        return NOOP_HISTOGRAM;
    }

    @Override
    public Meter meter(String name) {
        return NOOP_METER;
    }

    @Override
    public Timer timer(String name) {
        return NOOP_TIMER;
    }

    @Override
    public boolean remove(String name) {
        return false;
    }

    @Override
    public void removeMatching(MetricFilter filter) {
        //no-op
    }

    @Override
    public void addListener(MetricRegistryListener listener) {
        //no-op
    }

    @Override
    public void removeListener(MetricRegistryListener listener) {
        //no-op
    }

    @Override
    public SortedSet<String> getNames() {
        return Collections.emptySortedSet();
    }

    @Override
    @SuppressWarnings("rawtypes") //part of the interface definition
    public SortedMap<String, Gauge> getGauges() {
        return Collections.emptySortedMap();
    }

    @Override
    @SuppressWarnings("rawtypes") //part of the interface definition
    public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Counter> getCounters() {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Counter> getCounters(MetricFilter filter) {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Histogram> getHistograms() {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Histogram> getHistograms(MetricFilter filter) {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Meter> getMeters() {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Meter> getMeters(MetricFilter filter) {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Timer> getTimers() {
        return Collections.emptySortedMap();
    }

    @Override
    public SortedMap<String, Timer> getTimers(MetricFilter filter) {
        return Collections.emptySortedMap();
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return Collections.emptyMap();
    }

    private static final Counter NOOP_COUNTER = new Counter() {

        @Override
        public void inc(long n) {
        }

        @Override
        public void dec(long n) {
            super.dec(n);
        }

        @Override
        public long getCount() {
            return 0L;
        }
    };

    private static final Reservoir EMPTY_RESERVOIR = new Reservoir() {
        /**
         * {@inheritDoc}
         */
        @Override
        public int size() {
            return 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void update(long value) {
            // NOP
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Snapshot getSnapshot() {
            return EMPTY_SNAPSHOT;
        }
    };

    private static final Histogram NOOP_HISTOGRAM = new Histogram(EMPTY_RESERVOIR) {

        /**
         * {@inheritDoc}
         */
        @Override
        public void update(int value) {
            // NOP
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void update(long value) {
            // NOP
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getCount() {
            return 0L;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Snapshot getSnapshot() {
            return EMPTY_SNAPSHOT;
        }
    };

    private static final long[] EMPTY_LONG_ARRAY = new long[0];
    private static final Snapshot EMPTY_SNAPSHOT = new Snapshot(EMPTY_LONG_ARRAY) {
        /**
         * {@inheritDoc}
         */
        @Override
        public double getValue(double quantile) {
            return 0D;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long[] getValues() {
            return EMPTY_LONG_ARRAY;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int size() {
            return 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getMax() {
            return 0L;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double getMean() {
            return 0D;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getMin() {
            return 0L;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double getStdDev() {
            return 0D;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void dump(OutputStream output) {
            // NOP
        }
    };

    private static final Meter NOOP_METER = new Meter() {
        @Override
        public void mark(long n) {
            // noop
        }

        @Override
        public long getCount() {
            return 0L;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0d;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0d;
        }

        @Override
        public double getMeanRate() {
            return 0d;
        }

        @Override
        public double getOneMinuteRate() {
            return 0d;
        }
    };

    private static final Timer NOOP_TIMER = new Timer(EMPTY_RESERVOIR) {

        @Override
        public void update(long duration, TimeUnit unit) {
            //no op
        }

        @Override
        public <T> T time(Callable<T> event) throws Exception {
            return event.call();
        }

        @Override
        public long getCount() {
            return 0L;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0d;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0d;
        }

        @Override
        public double getMeanRate() {
            return 0d;
        }

        @Override
        public double getOneMinuteRate() {
            return 0d;
        }

        @Override
        public Snapshot getSnapshot() {
            return EMPTY_SNAPSHOT;
        }
    };
}
