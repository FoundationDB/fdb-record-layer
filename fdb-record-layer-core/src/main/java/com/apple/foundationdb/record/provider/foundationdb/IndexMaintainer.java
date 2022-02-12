/*
 * IndexMaintainer.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Object responsible for translating record saves and deletes into updates to a secondary index.
 *
 * This class is essentially an interface, implementing only the storage of an {@link IndexMaintainerState}.
 * A functional base class is provided by {@link com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer}.
 *
 * An {@code IndexMaintainer} instance is associated with a particular record store and index meta-data.
 * Implementers should assume that the same maintainer might be used to perform multiple record updates,
 * even though this never happens today.
 *
 * @see IndexMaintainerFactory
 */
@API(API.Status.MAINTAINED)
public abstract class IndexMaintainer {
    protected final IndexMaintainerState state;

    protected IndexMaintainer(IndexMaintainerState state) {
        this.state = state;
    }

    /**
     * Returns the subspace in which the index data is stored.
     * @return subspace for index data
     */
    @Nonnull
    public Subspace getIndexSubspace() {
        return state.indexSubspace;
    }

    /**
     * Returns the secondary subspace in which the index data is stored.
     * @return secondary subspace for index data
     */
    @Nonnull
    public Subspace getSecondarySubspace() {
        return state.store.indexSecondarySubspace(state.index);
    }

    /**
     * Scan entries in the index.
     * @param scanType the {@link IndexScanType type} of scan to perform
     * @param range the range to scan
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return a cursor over index entries in the given range
     */
    @Nonnull
    public abstract RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                                  @Nonnull TupleRange range,
                                                  @Nullable byte[] continuation,
                                                  @Nonnull ScanProperties scanProperties);

    /**
     * Scan entries in the index.
     * @param scanBounds the {@link IndexScanBounds bounds} of the scan to perform
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return a cursor over index entries in the given range
     */
    @Nonnull
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanBounds scanBounds,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        return scan(scanBounds.getScanType(), ((IndexScanRange)scanBounds).getScanRange(), continuation, scanProperties);
    }

    /**
     * Update associated index for a changed record.
     * @param oldRecord the previous stored record or <code>null</code> if a new record is being created
     * @param newRecord the new record or <code>null</code> if an old record is being deleted
     * @param <M> type of message
     * @return a future that is complete when the record update is done
     */
    @Nonnull
    public abstract <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                                       @Nullable FDBIndexableRecord<M> newRecord);


    /**
     * Scans through the list of uniqueness violations within the database.
     * It will return a cursor of {@link IndexEntry} instances where the
     * {@link IndexEntry#getKey() getKey()} will return the primary key
     * of the record causing a problem and {@link IndexEntry#getValue() getValue()}
     * will return the index value that is being duplicated.
     *
     * @param range range of tuples to read
     * @param continuation any continuation from a previous invocation
     * @param scanProperties row limit and other scan properties
     * @return a cursor that will return primary key-index key pairs indicating uniqueness violations
     */
    @Nonnull
    public abstract RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties);

    /**
     * Validates the integrity of the index entries. The definition of exactly what validations are performed is up to
     * the implementation of the index.
     *
     * It is not responsible for metadata validation, which is defined in
     * {@link com.apple.foundationdb.record.metadata.IndexValidator}.
     *
     * @param continuation any continuation from a previous validation invocation
     * @param scanProperties skip, limit and other properties of the validation (use default values if <code>null</code>)
     * @return a cursor over invalid index entries including reasons
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public abstract RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                                    @Nullable ScanProperties scanProperties);

    /**
     * Return <code>true</code> if this index be used to evaluate the given record function.
     * @param function requested function
     * @return {@code true} if this index can be used to evaluate the given function
     */
    public abstract boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function);

    /**
     * Apply the key and value expressions to a <code>record</code>.
     * @param <M> the message type of the record
     * @param record the record from which the index will extract its key and value
     * @return a list of index keys and values
     */
    @Nullable
    public abstract <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record);

    /**
     * Similar to {@link #evaluateIndex(FDBRecord)}, but returns null if the record should be filtered out.
     * @param <M> the message type of the record
     * @param savedRecord the indexable record from which the index will extract its key and value
     * @return a list of index keys and values
     */
    @Nullable
    public abstract  <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord);

    /**
     * Evaluate a record function on the given record.
     * @param <T> the result type of the function
     * @param <M> the message type of the record
     * @param context context for evaluation
     * @param function the record function to apply to the given record
     * @param record record against which to evaluate
     * @return a future that completes with the result of evaluation
     */
    @Nonnull
    public abstract <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                                       @Nonnull IndexRecordFunction<T> function,
                                                                                       @Nonnull FDBRecord<M> record);

    @Nonnull
    protected <T> CompletableFuture<T> unsupportedRecordFunction(@Nonnull IndexRecordFunction<T> function) {
        throw new RecordCoreException("Index " + state.index.getName() + " does not support " + function);
    }

    /**
     * Get whether this index can be used to evaluate the given aggregate function.
     * @param function the requested aggregate function
     * @return <code>true</code> if this index be used to evaluate the given aggregate function
     */
    public abstract boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function);

    /**
     * Evaluate an aggregate function over the given range using this index.
     * @param function the aggregate function to evaluate
     * @param range the range over which to accumulate the aggregate
     * @param isolationLevel the isolation level at which to perform the scan
     * @return a future that completes with the aggregate result
     */
    @Nonnull
    public abstract CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                                       @Nonnull TupleRange range,
                                                                       @Nonnull final IsolationLevel isolationLevel);

    @Nonnull
    protected CompletableFuture<Tuple> unsupportedAggregateFunction(@Nonnull IndexAggregateFunction function) {
        throw new RecordCoreException("Index " + state.index.getName() + " does not support " + function);
    }

    /**
     * Whether updating or removing a record on this index is idempotent. In principle, all
     * index updates in the normal case are idempotent as long as the index update and the
     * record insertion or deletion is atomic. However, certain indexes (mostly aggregate
     * indexes) have the property that the index update on its own are not idempotent.
     * @return whether updating this index is idempotent
     */
    public abstract boolean isIdempotent();

    /**
     * Whether this key has been added to some range within the {@link com.apple.foundationdb.async.RangeSet RangeSet}
     * associated with this index. This is used within the context of seeing if one should update a non-idempotent
     * write-only index with a new key. If the key is in some range, then it means that one should update the index
     * as it is based off of stale data. If the key is not in some range, then it means that one should not update
     * the index, as the rebuild job will handle it later.
     * @param primaryKey the record key of the record to check
     * @return a future that will be <code>true</code> if some range contains the record and <code>false</code> otherwise
     */
    @Nonnull
    public abstract CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey);

    /**
     * Get whether this index scan delete records matching a particular key query.
     * @param matcher the key query
     * @param evaluated parameters to the key query
     * @return <code>true</code> if this index accommodate a <code>whereRecordsWhere</code>
     */
    public abstract boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated);

    /**
     * Clear index storage associated with the given key prefix.
     * @param tr transaction in which to access the database
     * @param prefix prefix of primary key to clear
     * @return a future that is complete when the given prefix has been cleared from this index
     */
    public abstract CompletableFuture<Void> deleteWhere(@Nonnull Transaction tr, @Nonnull Tuple prefix);

    /**
     * Perform a type-specific operation on index. 
     * Allowed operations will vary by index type.
     * @param operation the requested operation
     * @return a future that completes with the result of the operation
     */
    public abstract CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation);
}
