/*
 * KeyCheckingTransaction.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Wrapper around {@link Transaction} that performs checks on keys passed to various operations.
 * @see KeyChecker
 */
@API(API.Status.EXPERIMENTAL)
public class KeyCheckingTransaction extends KeyCheckingReadTransaction<Transaction> implements Transaction {

    public KeyCheckingTransaction(@Nonnull final Transaction underlying, @Nonnull KeyChecker keyChecker) {
        super(underlying, keyChecker);
    }

    @Override
    public void addReadConflictRange(final byte[] keyBegin, final byte[] keyEnd) {
        checkKeyRange(keyBegin, keyEnd, false);
        underlying.addReadConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addReadConflictKey(final byte[] key) {
        checkKey(key, false);
        underlying.addReadConflictKey(key);
    }

    @Override
    public void addWriteConflictRange(final byte[] keyBegin, final byte[] keyEnd) {
        checkKeyRange(keyBegin, keyEnd, true);
        underlying.addWriteConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addWriteConflictKey(final byte[] key) {
        checkKey(key, true);
        underlying.addWriteConflictKey(key);
    }

    @Override
    public void set(final byte[] key, final byte[] value) {
        checkKey(key, true);
        underlying.set(key, value);
    }

    @Override
    public void clear(final byte[] key) {
        checkKey(key, true);
        underlying.clear(key);
    }

    @Override
    public void clear(final byte[] beginKey, final byte[] endKey) {
        checkKeyRange(beginKey, endKey, true);
        underlying.clear(beginKey, endKey);
    }

    @Override
    public void clear(final Range range) {
        checkKeyRange(range, true);
        underlying.clear(range);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void clearRangeStartsWith(final byte[] prefix) {
        checkKey(prefix, true);
        underlying.clearRangeStartsWith(prefix);
    }

    @Override
    public void mutate(final MutationType optype, final byte[] key, final byte[] param) {
        checkKey(key, true);
        underlying.mutate(optype, key, param);
    }

    @Override
    public CompletableFuture<Void> commit() {
        return underlying.commit();
    }

    @Override
    public Long getCommittedVersion() {
        return underlying.getCommittedVersion();
    }

    @Override
    public CompletableFuture<byte[]> getVersionstamp() {
        return underlying.getVersionstamp();
    }

    @Override
    public CompletableFuture<Long> getApproximateSize() {
        return underlying.getApproximateSize();
    }

    @Override
    public CompletableFuture<Transaction> onError(final Throwable e) {
        return underlying.onError(e);
    }

    @Override
    public void cancel() {
        underlying.cancel();
    }

    @Override
    public CompletableFuture<Void> watch(final byte[] key) throws FDBException {
        checkKey(key, false);
        return underlying.watch(key);
    }

    @Override
    public Database getDatabase() {
        return underlying.getDatabase();
    }

    @Override
    public <T> T run(final Function<? super Transaction, T> retryable) {
        return retryable.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> runAsync(final Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
        return retryable.apply(this);
    }

    @Override
    public void close() {
        try (underlying) {
            keyChecker.close();
        }
    }
}
