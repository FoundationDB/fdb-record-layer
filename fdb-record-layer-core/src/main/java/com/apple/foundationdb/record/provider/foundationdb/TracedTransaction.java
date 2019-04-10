/*
 * TracedTransaction.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A {@link Transaction} implementation that tracks its lifetime by saving the stack trace when it is created and logging that if it is not closed.
 */
@API(API.Status.INTERNAL)
public class TracedTransaction implements Transaction {
    private static final Logger LOGGER = LoggerFactory.getLogger(TracedTransaction.class);

    private Throwable stack = new Throwable();

    protected Transaction transaction;

    @Nullable
    private final Map<String, String> mdcContext;

    TracedTransaction(Transaction transaction, @Nullable Map<String, String> mdcContext) {
        this.transaction = transaction;
        this.mdcContext = mdcContext;
    }

    @SuppressWarnings({"NoFinalizer", "squid:ObjectFinalizeOverridenCheck", "PMD.FinalizeDoesNotCallSuperFinalize", "deprecation"})
    @Override
    protected void finalize() {
        if (transaction != null) {
            if (mdcContext != null) {
                FDBRecordContext.restoreMdc(mdcContext);
            }
            LOGGER.warn("did not close context", stack);
            if (mdcContext != null) {
                FDBRecordContext.clearMdc(mdcContext);
            }
        }
    }

    @Override
    public void close() {
        transaction.close();
        transaction = null;
    }

    // The following methods act as a facade of Transaction.

    @Override
    public ReadTransaction snapshot() {
        return transaction.snapshot();
    }

    @Override
    public void setReadVersion(long version) {
        transaction.setReadVersion(version);
    }

    @Override
    public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
        transaction.addReadConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addReadConflictKey(byte[] key) {
        transaction.addReadConflictKey(key);
    }

    @Override
    public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
        transaction.addWriteConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addWriteConflictKey(byte[] key) {
        transaction.addWriteConflictKey(key);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        transaction.set(key, value);
    }

    @Override
    public void clear(byte[] key) {
        transaction.clear(key);
    }

    @Override
    public void clear(byte[] beginKey, byte[] endKey) {
        transaction.clear(beginKey, endKey);
    }

    @Override
    public void clear(Range range) {
        transaction.clear(range);
    }

    @Override
    @Deprecated
    public void clearRangeStartsWith(byte[] prefix) {
        transaction.clearRangeStartsWith(prefix);
    }

    @Override
    public void mutate(MutationType optype, byte[] key, byte[] param) {
        transaction.mutate(optype, key, param);
    }

    @Override
    public CompletableFuture<Void> commit() {
        return transaction.commit();
    }

    @Override
    public Long getCommittedVersion() {
        return transaction.getCommittedVersion();
    }

    @Override
    public CompletableFuture<byte[]> getVersionstamp() {
        return transaction.getVersionstamp();
    }

    @Override
    public CompletableFuture<Transaction> onError(Throwable e) {
        return transaction.onError(e);
    }

    @Override
    public void cancel() {
        transaction.cancel();
    }

    @Override
    public CompletableFuture<Void> watch(byte[] key) throws FDBException {
        return transaction.watch(key);
    }

    @Override
    public Database getDatabase() {
        return transaction.getDatabase();
    }

    @Override
    public <T> T run(Function<? super Transaction, T> retryable) {
        return transaction.run(retryable);
    }

    @Override
    public <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
        return transaction.runAsync(retryable);
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
        return transaction.getReadVersion();
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        return transaction.get(key);
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector selector) {
        return transaction.getKey(selector);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
        return transaction.getRange(begin, end);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        return transaction.getRange(begin, end, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
        return transaction.getRange(begin, end, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode mode) {
        return transaction.getRange(begin, end, limit, reverse, mode);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
        return transaction.getRange(begin, end);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
        return transaction.getRange(begin, end, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
        return transaction.getRange(begin, end, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode mode) {
        return transaction.getRange(begin, end, limit, reverse, mode);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
        return transaction.getRange(range);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
        return transaction.getRange(range, limit);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
        return transaction.getRange(range, limit, reverse);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode mode) {
        return transaction.getRange(range, limit, reverse, mode);
    }

    @Override
    public TransactionOptions options() {
        return transaction.options();
    }

    @Override
    public <T> T read(Function<? super ReadTransaction, T> retryable) {
        return transaction.read(retryable);
    }

    @Override
    public <T> CompletableFuture<T> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
        return transaction.readAsync(retryable);
    }

    @Override
    public Executor getExecutor() {
        return transaction.getExecutor();
    }

}

