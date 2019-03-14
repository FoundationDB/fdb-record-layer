/*
 * TextCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.map.BunchedMapMultiIterator;
import com.apple.foundationdb.map.BunchedMapScanEntry;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link com.apple.foundationdb.record.RecordCursor RecordCursor} implementation for iterating over
 * items within an index of type "{@value com.apple.foundationdb.record.metadata.IndexTypes#TEXT}". The
 * index entries returned by this class will contain the token associated with this entry
 * in the column associated with the indexed text field of the index, and the value
 * of each entry will be a {@link Tuple} whose first element is the position list of
 * that token within the associated record's text field.
 *
 * @see TextIndexMaintainer
 * @see com.apple.foundationdb.map.BunchedMap BunchedMap
 */
@API(API.Status.EXPERIMENTAL)
class TextCursor implements BaseCursor<IndexEntry> {
    @Nonnull
    private final BunchedMapMultiIterator<Tuple, List<Integer>, Tuple> underlying;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Index index;
    @Nonnull
    private final CursorLimitManager limitManager;
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private FDBStoreTimer timer;
    private int limitRemaining;
    @Nullable
    private RecordCursorResult<IndexEntry> nextResult;

    TextCursor(@Nonnull BunchedMapMultiIterator<Tuple, List<Integer>, Tuple> underlying,
               @Nonnull Executor executor,
               @Nonnull FDBRecordContext context,
               @Nonnull ScanProperties scanProperties,
               @Nonnull Index index) {
        this.underlying = underlying;
        this.executor = executor;
        this.limitManager = new CursorLimitManager(context, scanProperties);
        this.index = index;

        this.limitRemaining = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
        this.timer = context.getTimer();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            // Like the KeyValueCursor, it is necessary to memoize and return the first result where
            // hasNext is false to avoid the NoNextReason changing.
            return CompletableFuture.completedFuture(nextResult);
        } else if (limitRemaining > 0 && limitManager.tryRecordScan()) {
            return underlying.onHasNext().thenApply(hasNext -> {
                if (hasNext) {
                    BunchedMapScanEntry<Tuple, List<Integer>, Tuple> nextItem = underlying.next();
                    if (timer != null) {
                        timer.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                        timer.increment(FDBStoreTimer.Counts.LOAD_TEXT_ENTRY);
                    }
                    Tuple k = nextItem.getKey();
                    Tuple subspaceTag = nextItem.getSubspaceTag();
                    if (subspaceTag != null) {
                        k = subspaceTag.addAll(k);
                    }
                    if (limitRemaining != Integer.MAX_VALUE) {
                        limitRemaining--;
                    }
                    nextResult = RecordCursorResult.withNextValue(
                            new IndexEntry(index, k, Tuple.from(nextItem.getValue())), continuationHelper());
                } else {
                    // Source iterator is exhausted
                    nextResult = RecordCursorResult.exhausted();
                }
                return nextResult;
            });
        } else { // a limit was exceeded
            if (limitRemaining <= 0) {
                nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), NoNextReason.RETURN_LIMIT_REACHED);
            } else {
                final Optional<NoNextReason> stoppedReason = limitManager.getStoppedReason();
                if (!stoppedReason.isPresent()) {
                    throw new RecordCoreException("limit manager stopped TextCursor but did not report a reason");
                } else {
                    nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), stoppedReason.get());
                }
            }
            return CompletableFuture.completedFuture(nextResult);
        }
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper() {
        return ByteArrayContinuation.fromNullable(underlying.getContinuation());
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public IndexEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextFuture = null;
        return nextResult.get();
    }

    @Nullable
    @Override
    @Deprecated
    public byte[] getContinuation() {
        return nextResult.getContinuation().toBytes();
    }

    @Nonnull
    @Override
    @Deprecated
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        underlying.cancel();
        if (hasNextFuture != null) {
            hasNextFuture.cancel(false);
        }
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
