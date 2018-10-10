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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.map.BunchedMapMultiIterator;
import com.apple.foundationdb.map.BunchedMapScanEntry;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
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
class TextCursor implements BaseCursor<IndexEntry> {
    private final BunchedMapMultiIterator<Tuple, List<Integer>, Tuple> underlying;
    private final Executor executor;
    @Nonnull
    private final CursorLimitManager limitManager;
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    private int limitRemaining;

    TextCursor(@Nonnull BunchedMapMultiIterator<Tuple, List<Integer>, Tuple> underlying,
               @Nonnull Executor executor,
               @Nonnull FDBRecordContext context,
               @Nonnull ScanProperties scanProperties) {
        this.underlying = underlying;
        this.executor = executor;
        this.limitManager = new CursorLimitManager(context, scanProperties);

        this.limitRemaining = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture != null) {
            return hasNextFuture;
        }
        if (limitRemaining != 0 && limitManager.tryRecordScan()) {
            hasNextFuture = underlying.onHasNext();
        } else {
            hasNextFuture = AsyncUtil.READY_FALSE;
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
    public IndexEntry next() {
        BunchedMapScanEntry<Tuple, List<Integer>, Tuple> nextItem = underlying.next();
        Tuple k = nextItem.getKey();
        Tuple subspaceTag = nextItem.getSubspaceTag();
        if (subspaceTag != null) {
            k = subspaceTag.addAll(k);
        }
        hasNextFuture = null;
        if (limitRemaining != Integer.MAX_VALUE) {
            limitRemaining--;
        }
        return new IndexEntry(k, Tuple.from(nextItem.getValue()));
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        return underlying.getContinuation();
    }

    @Nonnull
    @Override
    public NoNextReason getNoNextReason() {
        Optional<NoNextReason> reason = limitManager.getStoppedReason();
        if (reason.isPresent()) {
            return reason.get();
        }
        if (limitRemaining == 0) {
            return NoNextReason.RETURN_LIMIT_REACHED;
        } else {
            return NoNextReason.SOURCE_EXHAUSTED;
        }
    }

    @Override
    public void close() {
        underlying.cancel();
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
