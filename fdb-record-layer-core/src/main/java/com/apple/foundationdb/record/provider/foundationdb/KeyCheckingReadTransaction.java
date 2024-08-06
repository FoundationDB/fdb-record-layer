/*
 * KeyCheckingReadTransaction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Wrapper around {@link Transaction} that performs checks on keys passed to various operations.
 * @param <T> type of transaction
 */
@API(API.Status.EXPERIMENTAL)
public class KeyCheckingReadTransaction<T extends ReadTransaction> implements ReadTransaction {
    @Nonnull
    protected final T underlying;
    @Nonnull
    protected final KeyChecker keyChecker;

    protected KeyCheckingReadTransaction(@Nonnull final T underlying, @Nonnull KeyChecker keyChecker) {
        this.underlying = underlying;
        this.keyChecker = keyChecker;
    }

    protected void checkKeyRange(final Range range, boolean mutation) {
        checkKeyRange(range.begin, range.end, mutation);
    }

    protected void checkKeyRange(final byte[] keyBegin, final byte[] keyEnd, boolean mutation) {
        keyChecker.checkKeyRange(keyBegin, keyEnd, mutation);
    }

    protected void checkKey(final byte[] key, boolean mutation) {
        keyChecker.checkKey(key, mutation);
    }

    @Override
    public boolean isSnapshot() {
        return underlying.isSnapshot();
    }

    @Override
    public ReadTransaction snapshot() {
        return new KeyCheckingReadTransaction<>(underlying.snapshot(), keyChecker);
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
        return underlying.getReadVersion();
    }

    @Override
    public void setReadVersion(final long version) {
        underlying.setReadVersion(version);
    }

    @Override
    public boolean addReadConflictRangeIfNotSnapshot(final byte[] keyBegin, final byte[] keyEnd) {
        checkKeyRange(keyBegin, keyEnd, false);
        return underlying.addReadConflictRangeIfNotSnapshot(keyBegin, keyEnd);
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(final byte[] key) {
        checkKey(key, false);
        return underlying.addReadConflictKeyIfNotSnapshot(key);
    }

    @Override
    public CompletableFuture<byte[]> get(final byte[] key) {
        checkKey(key, false);
        return underlying.get(key);
    }

    @Override
    public CompletableFuture<byte[]> getKey(final KeySelector selector) {
        checkKey(selector.getKey(), false);
        return underlying.getKey(selector);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final KeySelector begin, final KeySelector end) {
        checkKeyRange(begin.getKey(), end.getKey(), false);
        return underlying.getRange(begin, end);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final KeySelector begin, final KeySelector end, final int limit) {
        checkKeyRange(begin.getKey(), end.getKey(), false);
        return underlying.getRange(begin, end, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final KeySelector begin, final KeySelector end, final int limit, final boolean reverse) {
        checkKeyRange(begin.getKey(), end.getKey(), false);
        return underlying.getRange(begin, end, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final KeySelector begin, final KeySelector end, final int limit, final boolean reverse, final StreamingMode mode) {
        checkKeyRange(begin.getKey(), end.getKey(), false);
        return underlying.getRange(begin, end, limit, reverse, mode);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final byte[] begin, final byte[] end) {
        checkKeyRange(begin, end, false);
        return underlying.getRange(begin, end);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final byte[] begin, final byte[] end, final int limit) {
        checkKeyRange(begin, end, false);
        return underlying.getRange(begin, end, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final byte[] begin, final byte[] end, final int limit, final boolean reverse) {
        checkKeyRange(begin, end, false);
        return underlying.getRange(begin, end, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final byte[] begin, final byte[] end, final int limit, final boolean reverse, final StreamingMode mode) {
        checkKeyRange(begin, end, false);
        return underlying.getRange(begin, end, limit, reverse, mode);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final Range range) {
        checkKeyRange(range, false);
        return underlying.getRange(range);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final Range range, final int limit) {
        checkKeyRange(range, false);
        return underlying.getRange(range, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final Range range, final int limit, final boolean reverse) {
        checkKeyRange(range, false);
        return underlying.getRange(range, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(final Range range, final int limit, final boolean reverse, final StreamingMode mode) {
        checkKeyRange(range, false);
        return underlying.getRange(range, limit, reverse, mode);
    }

    @Override
    public AsyncIterable<MappedKeyValue> getMappedRange(final KeySelector begin, final KeySelector end, final byte[] mapper, final int limit, final boolean reverse, final StreamingMode mode) {
        checkKeyRange(begin.getKey(), end.getKey(), false);
        return underlying.getMappedRange(begin, end, mapper, limit, reverse, mode);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(final byte[] begin, final byte[] end) {
        checkKeyRange(begin, end, false);
        return underlying.getEstimatedRangeSizeBytes(begin, end);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(final Range range) {
        checkKeyRange(range, false);
        return underlying.getEstimatedRangeSizeBytes(range);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(final byte[] begin, final byte[] end, final long chunkSize) {
        checkKeyRange(begin, end, false);
        return underlying.getRangeSplitPoints(begin, end, chunkSize);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(final Range range, final long chunkSize) {
        checkKeyRange(range, false);
        return underlying.getRangeSplitPoints(range, chunkSize);
    }

    @Override
    public TransactionOptions options() {
        return underlying.options();
    }

    @Override
    public <T> T read(final Function<? super ReadTransaction, T> retryable) {
        return retryable.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> readAsync(final Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
        return retryable.apply(this);
    }

    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }
}
