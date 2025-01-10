/*
 * TokenBucketSampler.java
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sampling engine which will pass a sample test according to a TokenBucket algorithm.
 * <p>
 * A Token-bucket algorithm allows a constant <em>average</em> throughput, but allows some burstiness by building
 * up tokens over periods of idleness. For more information,
 * see <a href="https://en.wikipedia.org/wiki/Token_bucket">Wikipedia</a>. You can control the burstiness by
 * configuring the maximum number of tokens to allow in the sampler,
 * and the average rate by adjusting the refresh interval. Note, however, that the refresh interval
 * is typically sensitive to the clock used in computing the interval--the refresh interval typically cannot
 * be smaller than the resolution of the system clock used.
 */
@API(API.Status.EXPERIMENTAL)
public class TokenBucketSampler implements Sampler {
    private final int maxTokens;

    private final long refreshIntervalNanos;
    private final Clock clock;

    private final AtomicLong numTokens;

    private final AtomicLong lastRefreshTime;

    public TokenBucketSampler(int maxTokens, long refreshInterval, TimeUnit refreshTime, Clock clock) {
        this(maxTokens, refreshTime.toNanos(refreshInterval), clock);
    }

    public TokenBucketSampler(int maxTokens, long refreshIntervalNanos, Clock clock) {
        Assert.thatUnchecked(maxTokens > 0);
        Assert.thatUnchecked(refreshIntervalNanos > 0);
        this.maxTokens = maxTokens;
        this.refreshIntervalNanos = refreshIntervalNanos;
        this.clock = clock;
        this.numTokens = new AtomicLong(maxTokens); //we start off full
        this.lastRefreshTime = new AtomicLong(clock.readNanos());
    }

    /**
     * Determine if a sample should be taken.
     *
     * @return {@code true} if a sample can be taken, {@code false} if it should be discarded.
     */
    @Override
    public boolean canSample() {
        refreshTokens();

        boolean shouldContinue;
        do {
            long tokensRemaining = numTokens.get();
            if (tokensRemaining == 0) {
                return false;
            } else {
                shouldContinue = !numTokens.compareAndSet(tokensRemaining, tokensRemaining - 1);
            }
        } while (shouldContinue);
        return true;
    }

    private void refreshTokens() {
        //calculate how many tokens need to be added to the bucket since the last token update
        long currTime = clock.readNanos();
        long lastTime = lastRefreshTime.get();
        if ((currTime - lastTime) >= refreshIntervalNanos) {
            //we need to add some tokens
            if (lastRefreshTime.compareAndSet(lastTime, currTime)) {
                //the number of refresh intervals that have passed
                long tokensToAdd = (currTime - lastTime) / refreshIntervalNanos;
                boolean shouldContinue;
                do {
                    long currTokens = numTokens.get();
                    long totalTokens = tokensToAdd + currTokens;
                    if (totalTokens > maxTokens) {
                        totalTokens = maxTokens;
                    }
                    shouldContinue = !numTokens.compareAndSet(currTokens, totalTokens);
                } while (shouldContinue);
            }
        }
    }
}
