/*
 * InstrumentedReadTransaction.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.TransactionOptions;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

abstract class InstrumentedReadTransaction<T extends ReadTransaction> implements ReadTransaction {
    @Nonnull
    protected StoreTimer timer;
    @Nonnull
    protected T underlying;

    public InstrumentedReadTransaction(@Nonnull StoreTimer timer, @Nonnull T underlying) {
        this.timer = timer;
        this.underlying = underlying;
    }

    @Override
    public boolean isSnapshot() {
        return underlying.isSnapshot();
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
        return underlying.getReadVersion();
    }

    @Override
    public void setReadVersion(long l) {
        underlying.setReadVersion(l);
    }

    @Override
    public boolean addReadConflictRangeIfNotSnapshot(byte[] keyBegin, byte[] keyEnd) {
        return underlying.addReadConflictRangeIfNotSnapshot(keyBegin, keyEnd);
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
        return underlying.addReadConflictKeyIfNotSnapshot(key);
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return underlying.get(key).thenApply(this::recordRead);
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector keySelector) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return underlying.getKey(keySelector).thenApply(this::recordRead);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
        /* Should this could as one read? */
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode streamingMode) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode streamingMode) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(range));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode streamingMode) {
        timer.increment(FDBStoreTimer.Counts.READS);
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit, reverse, streamingMode));
    }

    @Override
    public TransactionOptions options() {
        return underlying.options();
    }

    @Override
    public <T> T read(Function<? super ReadTransaction, T> function) {
        return function.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<T>> function) {
        return AsyncUtil.applySafely(function, this);
    }

    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }

    @Nullable
    protected byte[] recordRead(@Nullable byte[] value) {
        if (value != null) {
            timer.increment(FDBStoreTimer.Counts.BYTES_READ, value.length);
        }
        return value;
    }

    @Nullable
    protected KeyValue recordRead(@Nonnull KeyValue keyValue) {
        timer.increment(FDBStoreTimer.Counts.BYTES_READ, keyValue.getKey().length + keyValue.getValue().length);
        return keyValue;
    }

    private class ByteCountingAsyncIterable implements AsyncIterable<KeyValue> {
        private AsyncIterable<KeyValue> underlying;

        public ByteCountingAsyncIterable(AsyncIterable<KeyValue> underlying) {
            this.underlying = underlying;
        }

        @Override
        public AsyncIterator<KeyValue> iterator() {
            return new ByteCountingAsyncIterator(underlying.iterator());
        }

        @Override
        public CompletableFuture<List<KeyValue>> asList() {
            final List<KeyValue> result = new ArrayList<>();
            final AsyncIterator<KeyValue> iterator = iterator();
            return AsyncUtil.whileTrue(() -> iterator.onHasNext().thenApply(hasNext -> {
                if (hasNext) {
                    result.add(iterator.next());
                }
                return hasNext;
            })).thenApply(vignore -> result);
        }
    }

    private class ByteCountingAsyncIterator implements AsyncIterator<KeyValue> {
        private AsyncIterator<KeyValue> underlying;

        public ByteCountingAsyncIterator(AsyncIterator<KeyValue> iterator) {
            this.underlying = iterator;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return underlying.onHasNext();
        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public KeyValue next() {
            return recordRead(underlying.next());
        }

        @Override
        public void cancel() {
            underlying.cancel();
        }
    }
}
