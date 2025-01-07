/*
 * Clocks.java
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

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for creating different clock instances.
 */
@API(API.Status.EXPERIMENTAL)
public class Clocks {

    private static final Clock SYSTEM_CLOCK = System::nanoTime;

    private Clocks() {
    }

    /**
     * Get a clock which reads nano time from the System clock.
     *
     * @return a clock which reads from the system clock.
     */
    public static Clock systemClock() {
        return SYSTEM_CLOCK;
    }

    /**
     * Get a logical clock based off the specified underlying ticker object.
     * <p>
     * When using a logical ticker, it is important that the clock not be set backwards (otherwise, algorithms
     * may have weird results). This is a problem with all clocks, but is much more easily produced using logical clocks
     * like this.
     *
     * @param ticker the underlying counter.
     * @return a clock which uses the underlying ticker to tell logical time.
     */
    public static Clock logicalClock(@Nonnull AtomicLong ticker) {
        return ticker::get;
    }
}
