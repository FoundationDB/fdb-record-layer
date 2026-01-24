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

import com.apple.foundationdb.KeyArrayResult;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MappedKeyValue;
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
import java.util.Objects;
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
    @Nullable
    protected StoreTimer delayedTimer;

    @Nonnull
    protected T underlying;

    protected final boolean enableAssertions;

    /**
     * Create a new transaction wrapping an existing {@link ReadTransaction}. This will then use the
     * given timers to instrument the transaction's operations. Note that the main {@code timer} may be
     * shared between multiple transactions, but the {@code delayedTimer} should be solely used by this
     * transaction.
     *
     * @param timer the main timer to use for most events
     * @param delayedTimer the timer to use for events that are marked as {@linkplain StoreTimer.Event#isDelayedUntilCommit() delayed until commit}
     * @param underlying the underlying {@link ReadTransaction} to wrap
     * @param enableAssertions whether operations should validate their inputs and throw {@link com.apple.foundationdb.record.RecordCoreException}s
     *     if constaints like maximum key or value size are exceeded
     */
    public InstrumentedReadTransaction(@Nullable StoreTimer timer, @Nullable StoreTimer delayedTimer, @Nonnull T underlying, boolean enableAssertions) {
        this.timer = timer;
        this.delayedTimer = delayedTimer;
        this.underlying = underlying;
        this.enableAssertions = enableAssertions;
    }

    @Nullable
    protected StoreTimer getTimerForEvent(StoreTimer.Event event) {
        return event.isDelayedUntilCommit() ? delayedTimer : timer;
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
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode streamingMode) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode streamingMode) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(begin), checkKey(end), limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(range)));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(range), limit));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(range), limit, reverse));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode streamingMode) {
        return new ByteCountingAsyncIterable<>(underlying.getRange(checkKey(range), limit, reverse, streamingMode));
    }

    @Override
    public AsyncIterable<MappedKeyValue> getMappedRange(final KeySelector begin, final KeySelector end, final byte[] mapper, final int limit, final boolean reverse, final StreamingMode mode) {
        increment(FDBStoreTimer.Counts.REMOTE_FETCH);
        return new ByteCountingAsyncIterable<>(underlying.getMappedRange(begin, end, mapper, limit, reverse, mode),
                InstrumentedReadTransaction::countMappedKeyValueBytes);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(final Range range, final long chunkSize) {
        return underlying.getRangeSplitPoints(range, chunkSize);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(final byte[] begin, final byte[] end, final long chunkSize) {
        return underlying.getRangeSplitPoints(begin, end, chunkSize);
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
    public <V> V read(Function<? super ReadTransaction, V> function) {
        return function.apply(this);
    }

    @Override
    public <V> CompletableFuture<V> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<V>> function) {
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

    protected void increment(StoreTimer.Count count) {
        StoreTimer eventTimer = getTimerForEvent(count);
        if (eventTimer != null) {
            eventTimer.increment(count);
        }
    }

    protected void increment(StoreTimer.Count count, int amount) {
        StoreTimer eventTimer = getTimerForEvent(count);
        if (eventTimer != null) {
            eventTimer.increment(count, amount);
        }
    }

    protected void recordSinceNanoTime(StoreTimer.Event event, long nanoTime) {
        StoreTimer eventTimer = getTimerForEvent(event);
        if (eventTimer != null) {
            eventTimer.recordSinceNanoTime(event, nanoTime);
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
            return Objects.requireNonNull(ByteArrayUtil2.loggable(value));
        }

        byte[] portion = Arrays.copyOfRange(value, 0, MAX_LOGGED_BYTES);
        return ByteArrayUtil2.loggable(portion)
                + "+"
                + (value.length - MAX_LOGGED_BYTES)
                + " bytes";
    }

    private static int countKeyValueBytes(KeyValue kv) {
        return kv.getKey().length + kv.getValue().length;
    }

    private static int countMappedKeyValueBytes(MappedKeyValue mkv) {
        return countKeyValueBytes(mkv) + mkv.getRangeResult().stream().mapToInt(InstrumentedReadTransaction::countKeyValueBytes).sum();
    }

    private class ByteCountingAsyncIterable<K extends KeyValue> implements AsyncIterable<K> {
        private final AsyncIterable<K> underlying;

        private final Function<K, Integer> counterOp;

        public ByteCountingAsyncIterable(AsyncIterable<K> underlying) {
            this(underlying, InstrumentedReadTransaction::countKeyValueBytes);
        }

        public ByteCountingAsyncIterable(AsyncIterable<K> underlying, Function<K, Integer> counterOp) {
            this.underlying = underlying;
            this.counterOp = counterOp;
        }

        @Override
        @Nonnull
        public AsyncIterator<K> iterator() {
            increment(FDBStoreTimer.Counts.READS);
            increment(FDBStoreTimer.Counts.RANGE_READS);
            return new ByteCountingAsyncIterator<>(underlying.iterator(), counterOp);
        }

        @Override
        public CompletableFuture<List<K>> asList() {
            increment(FDBStoreTimer.Counts.READS);
            increment(FDBStoreTimer.Counts.RANGE_READS);
            return underlying.asList().thenApply(keyValues -> {
                if (keyValues.isEmpty()) {
                    increment(FDBStoreTimer.Counts.EMPTY_SCANS);
                } else {
                    int bytes = 0;
                    for (K kv : keyValues) {
                        bytes += counterOp.apply(kv);
                    }
                    increment(FDBStoreTimer.Counts.BYTES_READ, bytes);
                }
                return keyValues;
            });
        }
    }

    private class ByteCountingAsyncIterator<K extends KeyValue> implements AsyncIterator<K> {
        private final AsyncIterator<K> underlying;
        private final Function<K, Integer> counterOp;

        private volatile boolean hasAnyOrRecordedEmpty;

        public ByteCountingAsyncIterator(AsyncIterator<K> iterator, Function<K, Integer> counterOp) {
            this.underlying = iterator;
            this.counterOp = counterOp;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return underlying.onHasNext().whenComplete((doesHaveNext, err) -> {
                if (err == null) {
                    handleHasNext(doesHaveNext);
                }
            });
        }

        @Override
        public boolean hasNext() {
            boolean doesHaveNext = underlying.hasNext();
            handleHasNext(doesHaveNext);
            return doesHaveNext;
        }

        private void handleHasNext(boolean doesHaveNext) {
            if (doesHaveNext) {
                hasAnyOrRecordedEmpty = true;
            } else if (!hasAnyOrRecordedEmpty) {
                synchronized (this) {
                    if (!hasAnyOrRecordedEmpty) {
                        increment(FDBStoreTimer.Counts.EMPTY_SCANS);
                        hasAnyOrRecordedEmpty = true;
                    }
                }
            }
        }

        @Override
        public K next() {
            K next = underlying.next();
            increment(FDBStoreTimer.Counts.BYTES_READ, counterOp.apply(next));
            return next;
        }

        @Override
        public void cancel() {
            underlying.cancel();
        }
    }
}
