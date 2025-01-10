/*
 * EventSampler.java
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.annotation.API;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Sampler which will aim for a long-term stable throughput at or less than a certain fraction of events
 * (instead of per unit time).
 * <p>
 * This sampler will compute whether an item can be sampled based on a number of events,
 * rather than as a function of time, so the long-term throughput is measured in samples/event.
 *
 * Event sampling is useful when you have a consistent steady-state of incoming events, and you just want to
 * take a limited sample of those events, but don't care about shaping those events at all (e.g. you don't want
 * to allow bursty traffic or anything like that). This is mainly because the way EventSampling tracks time is by
 * the passage of events, so it will only ever allow through samples as a fraction of events, no matter how
 * irregular in time those events may be.
 *
 * If you are interested in a sampling method that captures time-irregular traffic patterns, then you should be
 * sampling according to time-rates, rather than even rates.
 */
@API(API.Status.EXPERIMENTAL)
public class EventSampler implements Sampler {

    private final AtomicLong eventCounter = new AtomicLong(0L);
    private final TokenBucketSampler delegate;

    public EventSampler(int maxSamplesPerEvent, long refreshIntervalEvents) {
        this.delegate = new TokenBucketSampler(maxSamplesPerEvent, refreshIntervalEvents, Clocks.logicalClock(eventCounter));
    }

    @Override
    public boolean canSample() {
        //up the event counter, so that we sample sometimes
        eventCounter.incrementAndGet();
        return delegate.canSample();
    }
}
