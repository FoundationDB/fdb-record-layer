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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.MergeCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.MergeCursorState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link RecordCursor} doing a bit-wise merge of bitmaps from two or more {@code BITMAP_VALUE} indexes.
 *
 * The bit operations can correspond to a Boolean expression over those indexes' rightmost grouping keys.
 *
 * @see BitmapValueIndexMaintainer
 */
class ComposedBitmapIndexCursor extends MergeCursor<IndexEntry, IndexEntry, MergeCursorState<IndexEntry>> {
    @Nonnull
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
        byte[] compose(@Nonnull List<byte[]> bitmaps, int size);
    }

    protected ComposedBitmapIndexCursor(@Nonnull List<MergeCursorState<IndexEntry>> cursorStates, @Nullable FDBStoreTimer timer, @Nonnull Composer composer) {
        super(cursorStates, timer);
        this.composer = composer;
    }

    @Nonnull
    @Override
    protected CompletableFuture<List<MergeCursorState<IndexEntry>>> computeNextResultStates() {
        final List<MergeCursorState<IndexEntry>> cursorStates = getCursorStates();
        return whenAll(cursorStates).thenApply(vignore -> {
            boolean anyHasNext = false;
            for (MergeCursorState<IndexEntry> cursorState : cursorStates) {
                if (cursorState.getResult().hasNext()) {
                    anyHasNext = true;
                } else if (cursorState.getResult().getNoNextReason().isLimitReached()) {
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
                    if (cursorState.getResult().hasNext()) {
                        final IndexEntry indexEntry = cursorState.getResult().get();
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

    @Nonnull
    @Override
    protected IndexEntry getNextResult(@Nonnull List<MergeCursorState<IndexEntry>> resultStates) {
        final List<MergeCursorState<IndexEntry>> cursorStates = getCursorStates();
        final IndexEntry firstEntry = resultStates.get(0).getResult().get();
        final int size = firstEntry.getValue().getBytes(0).length;
        final List<byte[]> bitmaps = new ArrayList<>(cursorStates.size());
        for (MergeCursorState<IndexEntry> cursorState : cursorStates) {
            if (resultStates.contains(cursorState)) {
                byte[] bitmap = cursorState.getResult().get().getValue().getBytes(0);
                if (bitmap.length != size) {
                    throw new RecordCoreException("Index bitmaps are not all the same size");
                }
                bitmaps.add(bitmap);
            } else {
                bitmaps.add(null);
            }
        }
        final byte[] composed = composer.compose(bitmaps, size);
        return new IndexEntry(firstEntry.getIndex(), firstEntry.getKey(), Tuple.fromList(Collections.singletonList(composed)));
    }

    @Nonnull
    @Override
    protected NoNextReason mergeNoNextReasons() {
        return getStrongestNoNextReason(getCursorStates());
    }

    @Nonnull
    @Override
    protected RecordCursorContinuation getContinuationObject() {
        return new ComposedBitmapIndexContinuation(getChildContinuations(), null);
    }

    @Nonnull
    public static ComposedBitmapIndexCursor create(@Nonnull List<Function<byte[], RecordCursor<IndexEntry>>> cursorFunctions,
                                                   @Nonnull Composer composer,
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
