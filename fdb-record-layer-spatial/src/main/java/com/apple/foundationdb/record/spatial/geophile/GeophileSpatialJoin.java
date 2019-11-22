/*
 * GeophileSpatialJoin.java
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

package com.apple.foundationdb.record.spatial.geophile;

import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.IllegalContinuationAccessChecker;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.Space;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.async.IndexAsync;
import com.geophile.z.async.IteratorAsync;
import com.geophile.z.async.SpatialIndexAsync;
import com.geophile.z.async.SpatialJoinAsync;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Generate {@link RecordCursor} from {@link GeophileIndexMaintainer} using Geophile {@link SpatialJoin}.
 */
class GeophileSpatialJoin<L, R> {
    @Nonnull
    private final SpatialJoinAsync<L, R> spatialJoin;
    @Nonnull
    private final FDBRecordStore store;
    @Nonnull
    private final EvaluationContext context;

    GeophileSpatialJoin(@Nonnull SpatialJoinAsync<L, R> spatialJoin, @Nonnull FDBRecordStore store, @Nonnull EvaluationContext context) {
        this.spatialJoin = spatialJoin;
        this.store = store;
        this.context = context;
    }

    @Nonnull
    public SpatialIndexAsync<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName) {
        return getSpatialIndex(indexName, ScanComparisons.EMPTY);
    }

    @Nonnull
    public SpatialIndexAsync<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName,
                                                                 @Nonnull ScanComparisons prefixComparisons) {
        return getSpatialIndex(indexName, prefixComparisons, GeophileRecordImpl::new);
    }

    @Nonnull
    public SpatialIndexAsync<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName,
                                                                 @Nonnull ScanComparisons prefixComparisons,
                                                                 @Nonnull BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction) {
        if (!prefixComparisons.isEquality()) {
            throw new RecordCoreArgumentException("prefix comparisons must only have equality");
        }
        // TODO: Add a FDBRecordStoreBase.getIndexMaintainer String overload to do this.
        final IndexMaintainer indexMaintainer = store.getIndexMaintainer(store.getRecordMetaData().getIndex(indexName));
        final TupleRange prefixRange = prefixComparisons.toTupleRange(store, context);
        final Tuple prefix = prefixRange.getLow();  // Since this is an equality, will match getHigh(), too.
        final IndexAsync<GeophileRecordImpl> index = new GeophileIndexImpl(indexMaintainer, prefix, recordFunction);
        final Space space = ((GeophileIndexMaintainer)indexMaintainer).getSpace();
        return SpatialIndexAsync.newSpatialIndexAsync(space, index);
    }

    @Nonnull
    public RecordCursor<IndexEntry> recordCursor(@Nonnull SpatialObject spatialObject,
                                                 @Nonnull SpatialIndexAsync<GeophileRecordImpl> spatialIndex) {
        // TODO: This is a synchronous implementation using Iterators. A proper RecordCursor implementation needs
        //  Geophile async extensions. Also need to pass down executeProperties.
        final IteratorAsync<GeophileRecordImpl> iterator = spatialJoin.iterator(spatialObject, spatialIndex);
        final RecordCursor<GeophileRecordImpl> recordCursor = cursorFromIteratorAsync(iterator);
        return recordCursor.map(GeophileRecordImpl::getIndexEntry);
    }

    @Nonnull
    public RecordCursor<Pair<IndexEntry, IndexEntry>> recordCursor(@Nonnull SpatialIndexAsync<GeophileRecordImpl> left,
                                                                   @Nonnull SpatialIndexAsync<GeophileRecordImpl> right) {
        // TODO: This is a synchronous implementation using Iterators. A proper RecordCursor implementation needs
        //  Geophile async extensions. Also need to pass down executeProperties.
        final IteratorAsync<com.geophile.z.Pair<GeophileRecordImpl, GeophileRecordImpl>> iterator = spatialJoin.iterator(left, right);
        final RecordCursor<com.geophile.z.Pair<GeophileRecordImpl, GeophileRecordImpl>> recordCursor = cursorFromIteratorAsync(iterator);
        return recordCursor.map(p -> Pair.of(p.left().getIndexEntry(), p.right().getIndexEntry()));
    }

    protected <T> RecordCursor<T> cursorFromIteratorAsync(@Nonnull IteratorAsync<T> iteratorAsync) {
        return new IteratorAsyncCursor<>(store.getExecutor(), iteratorAsync);
    }

    static class IteratorAsyncCursor<T> implements RecordCursor<T> {
        @Nonnull
        private final Executor executor;
        @Nonnull
        private final IteratorAsync<T> iteratorAsync;
        protected int valuesSeen;

        @Nullable
        protected CompletableFuture<Boolean> hasNextFuture;
        @Nullable
        protected RecordCursorResult<T> nextResult;

        // for detecting incorrect cursor usage
        protected boolean mayGetContinuation = false;

        protected IteratorAsyncCursor(@Nonnull Executor executor, @Nonnull IteratorAsync<T> iteratorAsync) {
            this.executor = executor;
            this.iteratorAsync = iteratorAsync;
            this.valuesSeen = 0;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<T>> onNext() {
            return iteratorAsync.nextAsync().thenApply(next -> {
                mayGetContinuation = next == null;
                if (next != null) {
                    // TODO: Need real continuations here.
                    valuesSeen++;
                    nextResult = RecordCursorResult.withNextValue(next, ByteArrayContinuation.fromInt(valuesSeen));
                } else {
                    nextResult = RecordCursorResult.exhausted();
                }
                return nextResult;

            });
        }

        @Override
        public void close() {
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

        @Nonnull
        @Override
        @Deprecated
        public CompletableFuture<Boolean> onHasNext() {
            if (hasNextFuture == null) {
                mayGetContinuation = false;
                hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
            }
            return hasNextFuture;
        }

        @Nullable
        @Override
        @Deprecated
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            mayGetContinuation = true;
            hasNextFuture = null;
            return nextResult.get();
        }

        @Nullable
        @Override
        @Deprecated
        public byte[] getContinuation() {
            IllegalContinuationAccessChecker.check(mayGetContinuation);
            return nextResult.getContinuation().toBytes();
        }

        @Nonnull
        @Override
        @Deprecated
        public NoNextReason getNoNextReason() {
            return nextResult.getNoNextReason();
        }
    }

}
