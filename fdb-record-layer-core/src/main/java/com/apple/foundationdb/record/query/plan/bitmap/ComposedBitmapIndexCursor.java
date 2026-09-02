/*
 * ComposedBitmapIndexCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.bitmap;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.MergeCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.MergeCursorState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.tuple.Tuple;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link RecordCursor} doing a bit-wise merge of bitmaps from two or more {@code BITMAP_VALUE} indexes.
 *
 * The bit operations can correspond to a Boolean expression over those indexes' rightmost grouping keys.
 *
 * @see BitmapValueIndexMaintainer
 */
@API(API.Status.EXPERIMENTAL)
class ComposedBitmapIndexCursor extends MergeCursor<IndexEntry, IndexEntry, MergeCursorState<IndexEntry>> {
    private final Composer composer;

    /**
     * Function for generating a bitmap from several others, all of the same size.
     */
    @FunctionalInterface
    public interface Composer {
        /**
         * Generate a bitmap from several others.
         * @param bitmaps a list of bitmaps or {@code null} if the corresponding input is absent / empty
         * @param size the common size of the bitmaps
         * @return a new bitmap formed from the inputs or {@code null} to represent an empty (all zero) bitmap
         */
        @Nullable
        byte[] compose(List<byte[]> bitmaps, int size);
    }

    protected ComposedBitmapIndexCursor(List<MergeCursorState<IndexEntry>> cursorStates, @Nullable FDBStoreTimer timer, Composer composer) {
        super(cursorStates, timer);
        this.composer = composer;
    }

    @Override
    protected CompletableFuture<List<MergeCursorState<IndexEntry>>> computeNextResultStates() {
        final List<MergeCursorState<IndexEntry>> cursorStates = getCursorStates();
        return whenAll(cursorStates).thenApply(vignore -> {
            boolean anyHasNext = false;
            for (MergeCursorState<IndexEntry> cursorState : cursorStates) {
                // whenAll() guarantees every cursorState's onNext future has completed, so getResult() is non-null here.
                final RecordCursorResult<IndexEntry> result = Objects.requireNonNull(cursorState.getResult());
                if (result.hasNext()) {
                    anyHasNext = true;
                } else if (result.getNoNextReason().isLimitReached()) {
                    // Stop if any has reached limit.
                    return Collections.emptyList();
                }
            }
            if (anyHasNext) {
                // Result states are all those that share the minimum next position,
                // whose bitmaps need to be merged to produce the next stream element.
                final List<MergeCursorState<IndexEntry>> resultStates = new ArrayList<>();
                long nextPosition = Long.MAX_VALUE;
                for (MergeCursorState<IndexEntry> cursorState : cursorStates) {
                    final RecordCursorResult<IndexEntry> result = Objects.requireNonNull(cursorState.getResult());
                    if (result.hasNext()) {
                        // hasNext() being true guarantees get() is non-null here.
                        final IndexEntry indexEntry = Objects.requireNonNull(result.get());
                        final Tuple indexKey = indexEntry.getKey();
                        final long position = indexKey.getLong(indexKey.size() - 1);
                        if (nextPosition > position) {
                            resultStates.clear();
                            nextPosition = position;
                        }
                        if (nextPosition == position) {
                            resultStates.add(cursorState);
                        }
                    }
                }
                return resultStates;
            } else {
                return Collections.emptyList();
            }
        });
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    protected IndexEntry getNextResult(List<MergeCursorState<IndexEntry>> resultStates) {
        final List<MergeCursorState<IndexEntry>> cursorStates = getCursorStates();
        // Every state in resultStates was selected in computeNextResultStates() precisely because its
        // getResult() was non-null and had a next value, so both calls below are safe.
        final IndexEntry firstEntry = Objects.requireNonNull(Objects.requireNonNull(resultStates.get(0).getResult()).get());
        final int size = firstEntry.getValue().getBytes(0).length;
        final List<byte[]> bitmaps = new ArrayList<>(cursorStates.size());
        for (MergeCursorState<IndexEntry> cursorState : cursorStates) {
            if (resultStates.contains(cursorState)) {
                byte[] bitmap = Objects.requireNonNull(Objects.requireNonNull(cursorState.getResult()).get()).getValue().getBytes(0);
                if (bitmap.length != size) {
                    throw new RecordCoreException("Index bitmaps are not all the same size");
                }
                bitmaps.add(bitmap);
            } else {
                bitmaps.add(null);
            }
        }
        @Nullable final byte[] composed = composer.compose(bitmaps, size);
        return new IndexEntry(firstEntry.getIndex(), firstEntry.getKey(), Tuple.fromList(Collections.singletonList(composed)));
    }

    @Override
    protected NoNextReason mergeNoNextReasons() {
        return getStrongestNoNextReason(getCursorStates());
    }

    @Override
    protected RecordCursorContinuation getContinuationObject() {
        return new ComposedBitmapIndexContinuation(getChildContinuations(), null);
    }

    public static ComposedBitmapIndexCursor create(List<Function<byte[], RecordCursor<IndexEntry>>> cursorFunctions,
                                                   Composer composer,
                                                   @Nullable byte[] byteContinuation,
                                                   @Nullable FDBStoreTimer timer) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to ComposedBitmapIndexCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
        final List<MergeCursorState<IndexEntry>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final ComposedBitmapIndexContinuation continuation = ComposedBitmapIndexContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<IndexEntry>> cursorFunction : cursorFunctions) {
            cursorStates.add(MergeCursorState.from(cursorFunction, continuation.getContinuation(i)));
            i++;
        }
        return new ComposedBitmapIndexCursor(cursorStates, timer, composer);
    }

}
