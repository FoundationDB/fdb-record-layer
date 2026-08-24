/*
 * OnReadListener.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.common.OnKeyValueReadListener;
import com.apple.foundationdb.async.common.TimedAsyncIterable;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Callbacks invoked whenever the structure reads from the database. The read-shaped hooks wrap the underlying FDB
 * calls as closely as possible so an implementer can, for example, measure per-read latency: {@link #onAsyncRead}
 * wraps a point {@code get}, {@link #onAsyncReadRange} wraps a range {@code getRange}, and the inherited
 * {@link OnKeyValueReadListener#onKeyValueRead} fires once per key/value actually observed. {@link #onVectorRead} is a
 * higher-level, synchronous bookkeeping hook fired when a stored vector reference is materialized.
 */
public interface OnReadListener extends OnKeyValueReadListener {
    OnReadListener NOOP = new OnReadListener() {
    };

    /**
     * Wraps the {@link CompletableFuture} of a point read ({@code transaction.get}) so an implementer can observe it —
     * typically to time the interval between issuing the read and its completion. The default returns the future
     * unchanged. Instrument as close to the {@code get} call as possible so the measured interval is the read itself.
     *
     * @param <T> the type the read future completes with (the raw value bytes at the {@code get} site)
     * @param future the pending point read
     * @return the future to await; by default the same instance
     */
    @SuppressWarnings("unused")
    default <T> CompletableFuture<T> onAsyncRead(@Nonnull final CompletableFuture<T> future) {
        return future;
    }

    /**
     * Wraps the {@link AsyncIterable} of a range read ({@code transaction.getRange}) so an implementer can observe the
     * scan as it is consumed. Unlike a point read, a range read has no single completion to time; an implementer that
     * wants latency should return a wrapper that accumulates the time spent awaiting each batch — see
     * {@link TimedAsyncIterable#wrap}, which reduces the override to a one-liner. The default returns the iterable
     * unchanged. Instrument as close to the {@code getRange} call as possible so the wrapper spans the actual scan.
     *
     * @param iterable the range scan produced by {@code getRange}
     * @return the iterable to consume; by default the same instance
     */
    @SuppressWarnings("unused")
    @Nonnull
    default AsyncIterable<KeyValue> onAsyncReadRange(@Nonnull final AsyncIterable<KeyValue> iterable) {
        return iterable;
    }

    /**
     * Synchronous bookkeeping hook fired when a stored vector reference is materialized from the database (distinct
     * from the raw {@code onKeyValueRead}, which sees only bytes). The vector's identity is passed as its public
     * components — {@code primaryKey} and {@code vectorUuid} — rather than the package-private {@code VectorId}. The
     * default does nothing.
     *
     * @param clusterId the cluster the reference was read from
     * @param primaryKey the primary key of the vector that was read
     * @param vectorUuid the uuid of the vector reference that was read
     * @param vector the (transformed) vector that was read; call {@link Transformed#getUnderlyingVector()} for the raw
     *        vector
     */
    @SuppressWarnings("unused")
    default void onVectorRead(@Nonnull final UUID clusterId, @Nonnull final Tuple primaryKey,
                              @Nonnull final UUID vectorUuid, @Nonnull final Transformed<RealVector> vector) {
        // nothing
    }
}
