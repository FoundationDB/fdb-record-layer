/*
 * Sampling.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.annotation.API;

import java.util.concurrent.TimeUnit;

/**
 * Utility methods and classes to improve the construction of various different sampling methods.
 */
@API(API.Status.EXPERIMENTAL)
public final class Sampling {

    private Sampling() {
    }

    /**
     * Samples events based on a psuedo-fixed long-term throughput of samples per time unit.
     * <p>
     * Over the long term, this will allow roughly {@code maxSamplesPerUnit} samples every {@code timeUnit} unit
     * of time that passes. If the throughput of the events being sampled is lower than this throughput, then all
     * events will be sampled, if it is higher, then some will be rejected. If the event traffic is bursty in time
     * (that is, a bunch of events occur close together in time), they may be sampled at the cost of future
     * traffic being throttled to keep the overall average throughput the same.
     *
     * @param maxSamplesPerUnit the maximum number of samples to allow per unit time
     * @param timeUnit          the time unit to compute within.
     * @return a sampler which will sample events aiming at a long-term rate of {@code maxSamplesPerUnit} samples
     * per unit of {@code timeUnit}
     */
    public static Sampler newSampler(int maxSamplesPerUnit, TimeUnit timeUnit) {
        /*
         * Token bucket sampling adds one new token every <refreshIntervalNanos> nanoseconds, and can keep up
         * to <maxTokenSize> tokens in reserve for bursty traffic. We want that to more-or-less coincide with
         * the specified maxSamplesPerUnit and time Unit.
         *
         * If we are saying that we want to average out at about X sample per minute, then maxSamplesPerUnit = X,
         * and timeUnit = MINUTE, then the refresh interval should be nanos(MINUTE)/X.
         */
        long refreshInterval = timeUnit.toNanos(1) / maxSamplesPerUnit;
        return new TokenBucketSampler(maxSamplesPerUnit, refreshInterval, Clocks.systemClock());
    }

    /**
     * Samples events based on a fixed throughput of samples per events.
     * <p>
     * This should provide maxSamplesPerEvent samples/event, gaining one sample every eventRefreshInterval. However,
     * because this is event sampling, and not time sampling, the returned sampler will not really allow
     * any burst-sampling, it will more-or-less confine itself to a fixed rate. However, if the events are input
     * in a burst, then this sampler will sample in bursts as well.
     *
     * @param maxSamplesPerEvent   the maximum number of samples to allow per event.
     * @param eventRefreshInterval how many events to wait before refreshing the token bucket. This is to amortize
     *                             the cost of refreshing the token counter across many requests.
     */
    public static Sampler eventSampler(int maxSamplesPerEvent, long eventRefreshInterval) {
        return new EventSampler(maxSamplesPerEvent, eventRefreshInterval);
    }
}
