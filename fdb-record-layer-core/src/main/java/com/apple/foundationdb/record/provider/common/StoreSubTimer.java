/*
 * StoreSubTimer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link StoreTimer} wrapping an underlying {@code StoreTimer} that accumulates the events
 * that are issued to it, as well as sending those events along to the underlying store timer.
 * The {@code StoreSubTimer} provides a mechanism to accumulate local snapshot view of the metrics
 * that comprise a larger operation and is an alternative approach to using
 * {@link com.apple.foundationdb.record.provider.common.StoreTimerSnapshot#from(StoreTimer)}
 * and {@link StoreTimer#getDifference(StoreTimer, StoreTimerSnapshot)}.
 */
@API(API.Status.EXPERIMENTAL)
public class StoreSubTimer extends StoreTimer {
    @Nullable
    private final StoreTimer underlying;

    public StoreSubTimer(@Nullable final StoreTimer underlying) {
        this.underlying = underlying;
    }

    @Override
    public void record(final StoreTimer.Event event, final long timeDifferenceNanos) {
        if (underlying != null) {
            underlying.record(event, timeDifferenceNanos);
        }
        super.record(event, timeDifferenceNanos);
    }

    @Override
    public void increment(@Nonnull final StoreTimer.Count event, int amount) {
        if (underlying != null) {
            underlying.increment(event, amount);
        }
        super.increment(event, amount);
    }
}
