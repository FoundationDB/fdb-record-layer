/*
 * FDBRecordStoreConcurrencyManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.concurrency;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Interface for managing the concurrency within a given {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore}.
 * That class is created with an instance of this interface as a member, and it should route operations through
 * appropriate methods.
 *
 * <p>
 * This framework is a bit of a work in progress. It currently only offers basic protection for single record
 * operations. That is, it prevents multiple operations from hitting issues when interleaving reads or writes
 * to the same record, but it does not take any locks to prevent a concurrent read and a range operation like
 * a {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#deleteRecordsWhere(QueryComponent) deleteRecordsWhere()}.
 * Nor will it prevent concurrent writes to the database during a query. As the framework gets refined, this
 * may change. As such, the adopter is still somewhat responsible for managing their concurrent accesses to
 * the record store until some of these shortcomings are addressed.
 * </p>
 *
 * <p>
 * If the lock management causes problems (e.g., if managing the locks takes too many resources or if the
 * introduction of the lock manager results in deadlocks), this can be disabled by invoking
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setDisableConcurrencyManagement(boolean) setDisableConcurrencyManagement(true)}
 * on the store's builder. This will switch the implementation to the {@link NoOpConcurrencyManager}, which
 * runs all operations immediately without waiting for any lock.
 * </p>
 *
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setDisableConcurrencyManagement(boolean)
 * @see FDBRecordStoreConcurrencyManager for the default implementation
 * @see NoOpConcurrencyManager for an implementation that does nothing, allowing the user to opt-out if problems arise
 */
@API(API.Status.INTERNAL)
public sealed interface StoreConcurrencyManager permits NoOpConcurrencyManager, FDBRecordStoreConcurrencyManager {
    /**
     * Perform an operation with a shared lock covering a single record.
     * This is applied on operations like {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#loadRecordAsync(Tuple) loadRecordAsync()}
     * to ensure that the read does not see partial updates, e.g., one split point overwritten by a
     * concurrent {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#saveRecordAsync(Message) saveRecordAsync()}.
     * This will wait for any previously started writes to the record to finish before beginning the operation, and it will
     * block any future writes to the record from beginning until this read has completed.
     *
     * @param primaryKey the primary key of the record being read
     * @param operation an operation to execute
     * @return a future that will complete when the operation has finished
     * @param <T> the type returned by the operation
     */
    <T> CompletableFuture<T> doWithRecordReadLock(@Nonnull Tuple primaryKey, @Nonnull Supplier<CompletableFuture<T>> operation);

    /**
     * Perform an operation with an exclusive lock covering a single record.
     * This is applied on operations like {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#saveRecordAsync(Message) saveRecordAsync()}
     * and {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#deleteRecordAsync(Tuple) deleteRecordAsync()}
     * to ensure that the writes do not interfere with each other or with any concurrent reads.
     * This will wait for any previously started operations to the record to finish before beginning, and it will
     * block any future operations to the record from beginning until this write has completed.
     *
     * @param primaryKey the primary key of the record being written
     * @param operation an operation to execute
     * @return a future that will complete when the operation has finished
     * @param <T> the type returned by the operation
     */
    <T> CompletableFuture<T> doWithRecordWriteLock(@Nonnull Tuple primaryKey, @Nonnull Supplier<CompletableFuture<T>> operation);
}
