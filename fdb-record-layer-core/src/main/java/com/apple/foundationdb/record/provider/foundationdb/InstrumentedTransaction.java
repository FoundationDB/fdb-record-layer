/*
 * InstrumentedTransaction.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Wrapper around {@link Transaction} that instruments certain calls to expose their behavior with
 * {@link FDBStoreTimer} metrics.
 */
public class InstrumentedTransaction extends InstrumentedReadTransaction<Transaction> implements Transaction {

    @Nullable
    protected ReadTransaction snapshot; // lazily cached snapshot wrapper

    public InstrumentedTransaction(@Nonnull StoreTimer timer, @Nonnull Transaction underlying) {
        super(timer, underlying);
    }

    @Override
    public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
        underlying.addReadConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addReadConflictKey(byte[] key) {
        underlying.addReadConflictKey(key);
    }

    @Override
    public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
        underlying.addWriteConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addWriteConflictKey(byte[] key) {
        underlying.addWriteConflictKey(key);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        underlying.set(key, value);
        timer.increment(FDBStoreTimer.Counts.WRITES);
        timer.increment(FDBStoreTimer.Counts.BYTES_WRITTEN, key.length + value.length);
    }

    @Override
    public void clear(byte[] key) {
        underlying.clear(key);
        timer.increment(FDBStoreTimer.Counts.DELETES);
    }

    @Override
    public void clear(byte[] keyBegin, byte[] keyEnd) {
        underlying.clear(keyBegin, keyEnd);
        timer.increment(FDBStoreTimer.Counts.DELETES);
    }

    @Override
    public void clear(Range range) {
        underlying.clear(range);
        timer.increment(FDBStoreTimer.Counts.DELETES);
    }

    @Override
    @Deprecated
    public void clearRangeStartsWith(byte[] prefix) {
        underlying.clearRangeStartsWith(prefix);
        timer.increment(FDBStoreTimer.Counts.DELETES);
    }

    @Override
    public void mutate(MutationType opType, byte[] key, byte[] param) {
        underlying.mutate(opType, key, param);
        /* Do we want to track each mutation type separately as well? */
        timer.increment(FDBStoreTimer.Counts.MUTATIONS);
    }

    @Override
    public CompletableFuture<Void> commit() {
        long startTimeNanos = System.nanoTime();
        return underlying.commit().whenComplete((v, ex) ->
                timer.recordSinceNanoTime(FDBStoreTimer.Events.COMMITS, startTimeNanos));
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
    public CompletableFuture<Transaction> onError(Throwable throwable) {
        return underlying.onError(throwable);
    }

    @Override
    public void cancel() {
        underlying.cancel();
    }

    @Override
    public CompletableFuture<Void> watch(byte[] bytes) throws FDBException {
        return underlying.watch(bytes);
    }

    @Override
    public Database getDatabase() {
        return underlying.getDatabase();
    }

    @Override
    public <T> T run(Function<? super Transaction, T> function) {
        return function.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> function) {
        return AsyncUtil.applySafely(function, this);
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public boolean isSnapshot() {
        return underlying.isSnapshot();
    }

    @Override
    public ReadTransaction snapshot() {
        if (snapshot == null) {
            snapshot = new Snapshot(timer, underlying.snapshot());
        }
        return snapshot;
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

    private static class Snapshot extends InstrumentedReadTransaction<ReadTransaction> implements ReadTransaction {
        public Snapshot(@Nonnull StoreTimer timer, @Nonnull ReadTransaction underlying) {
            super(timer, underlying);
        }

        @Override
        public ReadTransaction snapshot() {
            return this;
        }
    }
}
