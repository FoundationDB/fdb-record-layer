/*
 * PrefixSkipScanHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Shared skip-scan for index maintainers that partition an index's subspace into one per-prefix substructure
 * (an R-tree, a vector graph, ...), one per distinct leading <em>prefix</em> tuple (the grouping columns of the
 * index key). {@link #prefixSkipScan} walks the index subspace one distinct prefix tuple at a time, forming the
 * outer of a join whose inner scans the substructure for that prefix.
 *
 * @see RTreeIndexHelper
 * @see VectorIndexMaintainer
 */
@API(API.Status.EXPERIMENTAL)
public final class PrefixSkipScanHelper {
    private PrefixSkipScanHelper() {
    }

    /**
     * Returns a function from continuation bytes to a cursor over the distinct prefix tuples within
     * {@code prefixRange}, suitable as the outer of {@link RecordCursor#flatMapPipelined}.
     *
     * @param state the maintainer state
     * @param prefixSize number of leading (prefix/grouping) columns partitioning the index; if {@code 0}, the
     *     returned function yields a single {@code null} prefix tuple without scanning
     * @param timer timer to instrument the skip-scan with, or {@code null} to skip instrumentation
     * @param skipScanEvent the event to instrument the skip-scan with
     * @param prefixRange range constraining the prefix columns
     * @param innerScanProperties scan properties for the per-prefix lookup
     * @return a function from continuation bytes to a cursor over distinct prefix tuples
     */
    @Nonnull
    public static Function<byte[], RecordCursor<Tuple>> prefixSkipScan(@Nonnull final IndexMaintainerState state,
                                                                        final int prefixSize,
                                                                        @Nullable final StoreTimer timer,
                                                                        @Nonnull final StoreTimer.Event skipScanEvent,
                                                                        @Nonnull final TupleRange prefixRange,
                                                                        @Nonnull final ScanProperties innerScanProperties) {
        if (prefixSize <= 0) {
            return outerContinuation -> RecordCursor.fromFuture(CompletableFuture.completedFuture(null));
        }
        return outerContinuation -> {
            final ChainedCursor<Tuple> chainedCursor = new ChainedCursor<>(state.context,
                    lastKeyOptional -> nextPrefixTuple(state, prefixRange, prefixSize,
                            lastKeyOptional.orElse(null), innerScanProperties),
                    Tuple::pack,
                    Tuple::fromBytes,
                    outerContinuation,
                    innerScanProperties);
            return timer == null ? chainedCursor : timer.instrument(skipScanEvent, chainedCursor);
        };
    }

    @SuppressWarnings({"resource", "PMD.CloseResource"})
    @Nonnull
    private static CompletableFuture<Optional<Tuple>> nextPrefixTuple(@Nonnull final IndexMaintainerState state,
                                                                       @Nonnull final TupleRange prefixRange,
                                                                       final int prefixSize,
                                                                       @Nullable final Tuple lastPrefixTuple,
                                                                       @Nonnull final ScanProperties scanProperties) {
        final Subspace indexSubspace = state.indexSubspace;
        final KeyValueCursor cursor;
        if (lastPrefixTuple == null) {
            cursor = KeyValueCursor.Builder.withSubspace(indexSubspace)
                    .setContext(state.context)
                    .setRange(prefixRange)
                    .setContinuation(null)
                    .setScanProperties(scanProperties.setStreamingMode(CursorStreamingMode.ITERATOR)
                            .with(innerExecuteProperties -> innerExecuteProperties.setReturnedRowLimit(1)))
                    .build();
        } else {
            KeyValueCursor.Builder builder = KeyValueCursor.Builder.withSubspace(indexSubspace)
                    .setContext(state.context)
                    .setContinuation(null)
                    .setScanProperties(scanProperties)
                    .setScanProperties(scanProperties.setStreamingMode(CursorStreamingMode.ITERATOR)
                            .with(innerExecuteProperties -> innerExecuteProperties.setReturnedRowLimit(1)));

            cursor = builder.setLow(indexSubspace.pack(lastPrefixTuple), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(prefixRange.getHigh(), prefixRange.getHighEndpoint())
                    .build();
        }

        return cursor.onNext().thenApply(next -> {
            cursor.close();
            if (next.hasNext()) {
                final KeyValue kv = Objects.requireNonNull(next.get());
                return Optional.of(TupleHelpers.subTuple(indexSubspace.unpack(kv.getKey()), 0, prefixSize));
            }
            return Optional.empty();
        });
    }
}
