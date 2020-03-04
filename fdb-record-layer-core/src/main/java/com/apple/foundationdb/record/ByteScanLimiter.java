/*
 * ByteScanLimiter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * Track the number of bytes scanned up to some limit, after which record scans should not be allowed.
 *
 * @see ExecuteState#getByteScanLimiter()
 */
@API(API.Status.INTERNAL)
public interface ByteScanLimiter {
    /**
     * Create a new {@code ByteScanLimiter} with this limiter's original limit, ignoring any calls to
     * {@link #hasBytesRemaining()} and {@link #registerScannedBytes(long)}.
     * @return a new {@code ByteScanLimiter} with this limiter's original limit
     */
    @Nonnull
    ByteScanLimiter reset();

    /**
     * Return whether or not this limiter is actully enforcing the limit (vs one that simply tracks resource consumption).
     *
     * @return {@code true} if the limiter is enforcing a limit.
     */
    boolean isEnforcing();

    /**
     * Atomically check whether the number of remaining bytes is at least 0.
     * @return {@code true} if the remaining count is at least 0 and {@code false} if it is less than 0
     */
    boolean hasBytesRemaining();

    /**
     * Atomically decrement the number of remaining bytes by the given number of bytes.
     * @param bytes the number of bytes to register
     */
    void registerScannedBytes(long bytes);

    /**
     * Get the byte scan limit. In particular, this will return the target
     * number of bytes that this limiter is being used to enforce.
     *
     * @return the byte scan limit being enforced
     */
    long getLimit();

    /**
     * Returns the number of bytes that have been scanned thus far.
     *
     * @return the number of bytes that have been scanned
     */
    long getBytesScanned();
}
