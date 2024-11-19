/*
 * ExponentialDelay.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.runners;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This class maintains a delay that should be used when retrying transactional operations that have failed due to
 * something retriable.
 * <p>
 *     The goal here is to avoid making the situation worse when the system is overloaded, by backing off.
 * </p>
 * <p>
 *     While the name is "Exponential", it has a pretty aggressive jitter added so that, if there are two transactions
 *     conflicting, they won't both keep retrying with the same back off on every retry.
 * </p>
 *
 */
@API(API.Status.INTERNAL)
public class ExponentialDelay {

    private static final long MIN_DELAY_MILLIS = 2;
    private final long maxDelayMillis;
    private long currentDelayMillis;
    private long nextDelayMillis;
    @Nonnull
    private ScheduledExecutorService scheduledExecutor;

    public ExponentialDelay(final long initialDelayMillis, final long maxDelayMillis, @Nonnull ScheduledExecutorService scheduledExecutor) {
        currentDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.nextDelayMillis = calculateNextDelayMillis();
        this.scheduledExecutor = scheduledExecutor;
    }

    public CompletableFuture<Void> delay() {
        final long delay = nextDelayMillis;
        currentDelayMillis = Math.max(Math.min(currentDelayMillis * 2, maxDelayMillis), MIN_DELAY_MILLIS);
        nextDelayMillis = calculateNextDelayMillis();
        return delayedFuture(delay);
    }

    @SuppressWarnings("java:S2245") // this source of randomness is not for cryptography/security
    private long calculateNextDelayMillis() {
        return (long)(ThreadLocalRandom.current().nextDouble() * currentDelayMillis);
    }

    @Nonnull
    @VisibleForTesting
    protected CompletableFuture<Void> delayedFuture(final long nextDelayMillis) {
        return MoreAsyncUtil.delayedFuture(nextDelayMillis, TimeUnit.MILLISECONDS, scheduledExecutor);
    }

    public long getNextDelayMillis() {
        return nextDelayMillis;
    }
}
