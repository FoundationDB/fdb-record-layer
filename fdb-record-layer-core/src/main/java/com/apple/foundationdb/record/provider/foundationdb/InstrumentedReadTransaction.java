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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Wrapper around a {@code Transaction} or {@code ReadTransaction} responsible for instrumenting
 * read operations (e.g. tracking reads, bytes read, etc.).
 *
 * @param <T> the type of transaction to be instrumented
 */
@API(API.Status.INTERNAL)
abstract class InstrumentedReadTransaction<T extends ReadTransaction> implements ReadTransaction {

    protected static final int MAX_KEY_LENGTH = 10_000;
    protected static final int MAX_VALUE_LENGTH = 100_000;

    private static final int MAX_LOGGED_BYTES = 400;

    @Nullable
    protected StoreTimer timer;

    @Nonnull
    protected T underlying;

    protected final boolean enableAssertions;

    public InstrumentedReadTransaction(@Nullable StoreTimer timer, @Nonnull T underlying, boolean enableAssertions) {
        this.timer = timer;
        this.underlying = underlying;
        this.enableAssertions = enableAssertions;
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
        return underlying.addReadConflictRangeIfNotSnapshot(checkKey(keyBegin), checkKey(keyEnd));
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
        return underlying.addReadConflictKeyIfNotSnapshot(checkKey(key));
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        increment(FDBStoreTimer.Counts.READS);
        return underlying.get(checkKey(key)).thenApply(this::recordRead);
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector keySelector) {
        increment(FDBStoreTimer.Counts.READS);
        return underlying.getKey(checkKey(keySelector)).thenApply(this::recordRead);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
        /* Should this could as one read? */
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode streamingMode) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode streamingMode) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(range)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(range), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(range), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode streamingMode) {
        increment(FDBStoreTimer.Counts.READS);
        increment(FDBStoreTimer.Counts.RANGE_READS);
        return new ByteCountingAsyncIterable(underlying.getRange(checkKey(range), limit, reverse, streamingMode));
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(final byte[] begin, final byte[] end) {
        return underlying.getEstimatedRangeSizeBytes(begin, end);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(final Range range) {
        return underlying.getEstimatedRangeSizeBytes(range);
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
            increment(FDBStoreTimer.Counts.BYTES_READ, value.length);
        }
        return value;
    }

    @Nullable
    protected KeyValue recordRead(@Nonnull KeyValue keyValue) {
        final int bytes = keyValue.getKey().length + keyValue.getValue().length;
        increment(FDBStoreTimer.Counts.BYTES_READ, bytes);
        return keyValue;
    }

    protected void increment(StoreTimer.Count count) {
        if (timer != null) {
            timer.increment(count);
        }
    }

    protected void increment(StoreTimer.Count count, int amount) {
        if (timer != null) {
            timer.increment(count, amount);
        }
    }

    protected void recordSinceNanoTime(StoreTimer.Event event, long nanoTime) {
        if (timer != null) {
            timer.recordSinceNanoTime(event, nanoTime);
        }
    }

    @Nonnull
    protected KeySelector checkKey(@Nonnull KeySelector keySelector) {
        checkKey(keySelector.getKey());
        return keySelector;
    }

    @Nonnull
    protected Range checkKey(@Nonnull Range range) {
        checkKey(range.begin);
        checkKey(range.end);
        return range;
    }

    @Nonnull
    protected byte[] checkKey(@Nonnull byte[] key) {
        if (enableAssertions && key.length > MAX_KEY_LENGTH) {
            throw new FDBExceptions.FDBStoreKeySizeException("Key length exceeds limit",
                    LogMessageKeys.KEY_SIZE, key.length,
                    LogMessageKeys.KEY, loggable(key));
        }
        return key;
    }

    @Nonnull
    protected byte[] checkValue(@Nonnull byte[] key, @Nonnull byte[] value) {
        if (enableAssertions && value.length > MAX_VALUE_LENGTH) {
            throw new FDBExceptions.FDBStoreValueSizeException("Value length exceeds limit",
                    LogMessageKeys.VALUE_SIZE, value.length,
                    LogMessageKeys.KEY, loggable(key),
                    LogMessageKeys.VALUE, loggable(value));
        }
        return value;
    }

    @Nonnull
    protected String loggable(@Nonnull byte[] value) {
        if (value.length <= MAX_LOGGED_BYTES + 20) {
            return ByteArrayUtil2.loggable(value);
        }

        byte[] portion = Arrays.copyOfRange(value, 0, MAX_LOGGED_BYTES);
        return ByteArrayUtil2.loggable(portion)
                + "+"
                + (value.length - MAX_LOGGED_BYTES)
                + " bytes";
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
            return underlying.asList().thenApply(keyValues -> {
                int bytes = 0;
                for (KeyValue kv : keyValues) {
                    bytes += kv.getKey().length + kv.getValue().length;
                }
                increment(FDBStoreTimer.Counts.BYTES_READ, bytes);
                return keyValues;
            });
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
