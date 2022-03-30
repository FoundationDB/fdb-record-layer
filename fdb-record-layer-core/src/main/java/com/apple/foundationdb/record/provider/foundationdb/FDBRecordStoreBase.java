/*
 * FDBRecordStoreBase.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Base interface for typed and untyped record stores.
 *
 * This interface is the main front-end for most operations inserting, modifying, or querying data through
 * the Record Layer. A record store combines:
 *
 * <ul>
 *     <li>A {@link Subspace} (often specified via a {@link KeySpacePath})</li>
 *     <li>The {@link com.apple.foundationdb.record.RecordMetaData RecordMetaData} associated with the data stored with the data in that subspace</li>
 *     <li>An {@link FDBRecordContext} which wraps a FoundationDB {@link com.apple.foundationdb.Transaction Transaction}</li>
 * </ul>
 *
 * <p>
 * All of the record store's data&mdash;including index data&mdash;are stored durably within the given subspace. Note that
 * the meta-data is <em>not</em> stored by the record store directly. However, information about the store's current meta-data
 * version is persisted with the store to detect when the meta-data have changed and to know if any action needs to be taken
 * to begin using the new meta-data. (For example, new indexes might need to be built and removed indexes deleted.) The same
 * meta-data may be used for multiple record stores, and separating the meta-data from the data makes updating the shared
 * meta-data simpler as it only needs to be updated in one place. The {@link FDBMetaDataStore} may be used if one wishes
 * to persist the meta-data into a FoundationDB cluster.
 * </p>
 *
 * <p>
 * All operations conducted by a record store are conducted within the lifetime single transaction, and no data is persisted
 * to the database until the transaction is committed by calling {@link FDBRecordContext#commit()} or
 * {@link FDBRecordContext#commitAsync()}. Record Layer transactions inherit all of the guarantees and limitations of
 * the transactions exposed by FoundationDB, including their durability and consistency guarantees as well as size and
 * duration limits. See the FoundationDB <a href="https://apple.github.io/foundationdb/known-limitations.html">known limitations</a>
 * for more details.
 * </p>
 *
 * <p>
 * The record store also allows the user to tweak additional parameters such as what the parallelism of pipelined operations
 * should be (through the {@link PipelineSizer}) and what serializer should be used to read and write data to the database.
 * See the {@link BaseBuilder} interface for more details.
 * </p>
 *
 * @param <M> type used to represent stored records
 * @see FDBRecordStore
 * @see FDBTypedRecordStore
 */
@API(API.Status.MAINTAINED)
public interface FDBRecordStoreBase<M extends Message> extends RecordMetaDataProvider {

    /**
     * Get the untyped record store associated with this possibly typed store.
     * @return an untyped record store
     */
    FDBRecordStore getUntypedRecordStore();

    /**
     * Get a typed record store using the given typed serializer.
     * @param <N> the type for the new record store
     * @param serializer typed serializer to use
     * @return a new typed record store
     */
    default <N extends Message> FDBTypedRecordStore<N> getTypedRecordStore(@Nonnull RecordSerializer<N> serializer) {
        return new FDBTypedRecordStore<>(getUntypedRecordStore(), serializer);
    }

    /**
     * Get the record context (transaction) to use for the record store.
     * @return context the record context / transaction to use
     */
    @Nonnull
    FDBRecordContext getContext();

    @Nonnull
    default Executor getExecutor() {
        return getContext().getExecutor();
    }

    @Nullable
    default FDBStoreTimer getTimer() {
        return getContext().getTimer();
    }

    /**
     * Get the subspace provider.
     * @return the subspace provider
     */
    @Nullable
    SubspaceProvider getSubspaceProvider();

    /**
     * Get the serializer used to convert records into byte arrays.
     * @return the serializer to use
     */
    @Nonnull
    RecordSerializer<M> getSerializer();

    /**
     * Hook for checking if store state for client changes.
     */
    interface UserVersionChecker {
        /**
         * Check the user version.
         * @param oldUserVersion the old user version or <code>-1</code> if this is a new record store
         * @param oldMetaDataVersion the old meta-data version
         * @param metaData the meta-data provider that will be used to get meta-data
         * @return the user version to store in the record info header
         */
        CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion,
                                                    RecordMetaDataProvider metaData);

        /**
         * Determine what to do about an index needing to be built. When a {@link FDBRecordStore} is opened,
         * this method will be called on any index that has been added to the
         * {@link com.apple.foundationdb.record.RecordMetaData RecordMetaData} since the last time the record store was
         * opened. The index will then be initialized with the {@link IndexState} returned, which in turn determines
         * whether the index must be maintained during record inserts and deletes and also whether the index
         * can be read (for queries, for example). In general, an index is only really useful if it is
         * {@link IndexState#READABLE}, but if this method returns {@link IndexState#READABLE}, then the
         * index must be built in the same transaction that opens the record store, which can lead to errors
         * on large stores if the index cannot be built in the
         * <a href="https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions">five second
         * FoundationDB transaction time limit</a>.
         *
         * <p>
         * By default, this will return {@link IndexState#READABLE} for any indexes on new types (which
         * can be used right away without doing any I/O) or if the number of records in the store is small (below
         * {@link FDBRecordStore#MAX_RECORDS_FOR_REBUILD}). However, if the record store is large, this will return
         * {@link IndexState#DISABLED}, which indicates that the index should not be maintained and that it cannot be
         * used until the index is built by the {@link OnlineIndexer}.
         * </p>
         *
         * <p>
         * For adopters, two utility methods are provided that can be used to make implementing this method easier.
         * The first is {@link FDBRecordStore#disabledIfTooManyRecordsForRebuild(long, boolean)}, which replicates
         * the default behavior. The second is
         * {@link FDBRecordStore#writeOnlyIfTooManyRecordsForRebuild(long, boolean)}, which is similar to the default
         * except that it returns {@link IndexState#WRITE_ONLY} instead of {@link IndexState#DISABLED} and was the
         * default prior to Record Layer 3.0. Note that all indexes must be made {@link IndexState#WRITE_ONLY} before
         * they can be built, but the {@link OnlineIndexer} should generally handle that index state transition, and so
         * most adopters should return {@link IndexState#DISABLED} on indexes that cannot be built in-line.
         * </p>
         *
         * @param index the index that has not been built for this store
         * @param recordCount the number of records already in the store
         * @param indexOnNewRecordTypes <code>true</code> if all record types for the index are new (the number of
         *                              records related to this index is 0), in which case the index is able to be
         *                              "rebuilt" instantly with no cost.
         * @return the desired state of the new index. If this is {@link IndexState#READABLE}, the index will be built right away
         * @see FDBRecordStore#disabledIfTooManyRecordsForRebuild(long, boolean)
         * @see FDBRecordStore#writeOnlyIfTooManyRecordsForRebuild(long, boolean)
         */
        default IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
            return FDBRecordStore.disabledIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes);
        }

        /**
         * Determine what to do about an index needing to be rebuilt. For more information about when this method
         * is called and what the return value is used for, see {@link #needRebuildIndex(Index, long, boolean)}.
         *
         * <p>
         * This method takes the record count and {@linkplain FDBRecordStore#estimateRecordsSizeAsync() estimated size}
         * as parameters. Implementors can choose to use either value (or neither) when determining whether an index
         * should be built in-line (i.e., whether to return {@link IndexState#READABLE}) when the meta-data on a store
         * is upgraded. For record types on which an appropriate
         * {@linkplain com.apple.foundationdb.record.metadata.IndexTypes#COUNT count} index is either not defined or
         * for which scanning the count index would be too much work due to grouping keys (see
         * <a href="https://github.com/foundationDB/fdb-record-layer/issues/7">Issue #7</a>), it may be more efficient
         * to base the indexing decision on the size estimate alone.
         * </p>
         *
         * <p>
         * Both the record count and size estimate parameter are specified via suppliers that will not evaluate the
         * count or size until requested. If a value is requested from either supplier, the returned future should
         * complete after the future returned by the supplier.
         * </p>
         *
         * <p>
         * By default, this will call {@link #needRebuildIndex(Index, long, boolean)} with the record count returned
         * by {@code lazyRecordCount}, so adopters who want to use the record count but not the records size estimate
         * only need to implement that function.
         * </p>
         *
         * @param index the index that has not been built for this store
         * @param lazyRecordCount a supplier that will return a future with the number of records already in the store
         * @param lazyEstimatedSize a supplier that will return a future that will resolve to an estimate of the size of
         *                          the store in bytes
         * @param indexOnNewRecordTypes <code>true</code> if all record types for the index are new (the number of
         *                              records related to this index is 0), in which case the index is able to be
         *                              "rebuilt" instantly with no cost.
         * @return a future that will complete to the desired state of the new index
         * @see #needRebuildIndex(Index, long, boolean)
         */
        @API(API.Status.EXPERIMENTAL)
        @Nonnull
        default CompletableFuture<IndexState> needRebuildIndex(Index index,
                                                               Supplier<CompletableFuture<Long>> lazyRecordCount,
                                                               Supplier<CompletableFuture<Long>> lazyEstimatedSize,
                                                               boolean indexOnNewRecordTypes) {
            return lazyRecordCount.get()
                    .thenApply(recordCount -> needRebuildIndex(index, recordCount, indexOnNewRecordTypes));
        }
    }

    /**
     * Action to take if the record store does / does not already exist.
     * @see FDBRecordStore.Builder#createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck)
     */
    enum StoreExistenceCheck {
        /**
         * No special action.
         *
         * This should be used with care, since if the record store already has records, there is
         * no guarantee that they were written at the current versions (meta-data and format).
         * It is really only appropriate in development when switching from {@code uncheckedOpen}
         * or {@code build} to a checked open.
         */
        NONE,

        /**
         * Throw if the record store does not have an info header but does have have at least one
         * record. This differs from {@link #ERROR_IF_NO_INFO_AND_NOT_EMPTY} in that there is
         * data stored in the record store other than just the records and the indexes, including
         * meta-data about which indexes have been built. A record store that is missing a header
         * but has this other data is in a corrupt state, but as there are no records, it can be
         * recovered when creating the store in a straightforward way.
         */
        ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES,

        /**
         * Throw if the record store does not have an info header but is not empty. Unlike with
         * {@link #ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES}, this existence check will throw an
         * error even if there are no records in the store, only data stored internally by the
         * Record Layer.
         *
         * This corresponds to {@link FDBRecordStore.Builder#createOrOpen}
         */
        ERROR_IF_NO_INFO_AND_NOT_EMPTY,

        /**
         * Throw if the record store already exists.
         *
         * This corresponds to {@link FDBRecordStore.Builder#create}
         * @see RecordStoreAlreadyExistsException
         */
        ERROR_IF_EXISTS,

        /**
         * Throw if the record store does not already exist.
         *
         * This corresponds to {@link FDBRecordStore.Builder#open}
         * @see RecordStoreDoesNotExistException
         */
        ERROR_IF_NOT_EXISTS
    }

    /**
     * Action to take if the record being saved does / does not already exist.
     * @see FDBRecordStoreBase#saveRecordAsync(Message, RecordExistenceCheck)
     */
    enum RecordExistenceCheck {
        /**
         * No special action.
         *
         * This corresponds to {@link FDBRecordStoreBase#saveRecord}
         */
        NONE,

        /**
         * Throw if the record already exists.
         *
         * This corresponds to {@link FDBRecordStoreBase#insertRecord}
         * @see RecordAlreadyExistsException
         */
        ERROR_IF_EXISTS,

        /**
         * Throw if the record does not already exist.
         *
         * @see RecordDoesNotExistException
         */
        ERROR_IF_NOT_EXISTS,

        /**
         * Throw if an existing record has a different record type.
         *
         * @see RecordTypeChangedException
         */
        ERROR_IF_RECORD_TYPE_CHANGED,

        /**
         * Throw if the record does not already exist or has a different record type.
         *
         * This corresponds to {@link FDBRecordStoreBase#updateRecord}
         * @see RecordDoesNotExistException
         * @see RecordTypeChangedException
         */
        ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED;

        public boolean errorIfExists() {
            return this == ERROR_IF_EXISTS;
        }

        public boolean errorIfNotExists() {
            return this == ERROR_IF_NOT_EXISTS || this == ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED;
        }

        public boolean errorIfTypeChanged() {
            return this == ERROR_IF_RECORD_TYPE_CHANGED || this == ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED;
        }
    }

    /**
     * Provided during record save (via {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)}),
     * directs the behavior of the save w.r.t. the record's version.
     * In the presence of a version, either {@code DEFAULT} or {@code WITH_VERSION} can be used.
     * For safety, <code>NO_VERSION</code> should only be used with a null version.
     */
    enum VersionstampSaveBehavior {
        /**
         * Match the behavior dictated by the meta-data. If {@link com.apple.foundationdb.record.RecordMetaData#isStoreRecordVersions()}
         * returns {@code true}, this will always store the record with a version (like {@link #WITH_VERSION}). Otherwise,
         * it will store the record with the provided version if given or with no version if not.
         */
        DEFAULT,
        /**
         * Do not save the record with a version. If a non-null version is provided to {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)},
         * then an error will be thrown.
         */
        NO_VERSION,
        /**
         * Always save the record with a version. If a null version is provided, then the record store will chose
         * a new version.
         *
         * <p>
         * Note: due to <a href="https://github.com/FoundationDB/fdb-record-layer/issues/964">Issue #964</a>, on some
         * older record stores, namely those that were originally created with a {@linkplain FDBRecordStore#getFormatVersion()
         * format version} below {@link FDBRecordStore#SAVE_VERSION_WITH_RECORD_FORMAT_VERSION}, records written with a
         * version on stores where {@link com.apple.foundationdb.record.RecordMetaData#isStoreRecordVersions()} is
         * {@code false} will not return the version with a record when read, even though the version will be stored.
         * Users can avoid this by either migrating data to a new store or by setting {@code isStoreRecordVersions()}
         * to {@code true} in the meta-data and then supplying the {@link #NO_VERSION} when saving any records that
         * do not need an associated version.
         * </p>
         */
        WITH_VERSION,
        /**
         * Save a record with a version if and only if a non-null version is passed to {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)}.
         * In this mode, the record store will never assign a version to the record, but it will always use the
         * version provided (or store the record with no version if {@code null}). This is useful if one is copying
         * data from one record store to another and one wants to preserve the versions (including non-versions) for each
         * record.
         */
        IF_PRESENT,
    }

    /**
     * Async version of {@link #saveRecord(Message)}.
     * @param rec the record to save
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M rec) {
        return saveRecordAsync(rec, (FDBRecordVersion)null);
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck)}.
     * @param rec the record to save
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M rec, @Nonnull RecordExistenceCheck existenceCheck) {
        return saveRecordAsync(rec, existenceCheck, null, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Async version of {@link #saveRecord(Message, FDBRecordVersion)}.
     * @param rec the record to save
     * @param version the associated record version
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M rec, @Nullable FDBRecordVersion version) {
        return saveRecordAsync(rec, version, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Async version of {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param rec the record to save
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M rec, @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveRecordAsync(rec, RecordExistenceCheck.NONE, version, behavior);
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param rec the record to save
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull M rec, @Nonnull RecordExistenceCheck existenceCheck,
                                                          @Nullable FDBRecordVersion version, @Nonnull VersionstampSaveBehavior behavior);

    /**
     * Save the given record.
     * @param rec the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> saveRecord(@Nonnull final M rec) {
        return saveRecord(rec, (FDBRecordVersion)null);
    }

    /**
     * Save the given record.
     * @param rec the record to be saved
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> saveRecord(@Nonnull final M rec, @Nonnull RecordExistenceCheck existenceCheck) {
        return saveRecord(rec, existenceCheck, null, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Save the given record with a specific version. If <code>null</code>
     * is passed for <code>version</code>, then a new version is
     * created that will be unique for this record.
     * @param rec the record to be saved
     * @param version the version to associate with the record when saving
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> saveRecord(@Nonnull final M rec, @Nullable final FDBRecordVersion version) {
        return saveRecord(rec, version, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Save the given record with a specific version.
     * The version is handled according to the behavior value. If behavior is <code>DEFAULT</code> then
     * the method acts as {@link #saveRecord(Message, FDBRecordVersion)}. If behavior is <code>NO_VERSION</code> then
     * <code>version</code> is ignored and no version is saved. If behavior is <code>WITH_VERSION</code> then the value
     * of <code>version</code>  is stored as given by the caller.
     * @param rec the record to be saved
     * @param version the version to associate with the record when saving
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> saveRecord(@Nonnull final M rec, @Nullable final FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveRecord(rec, RecordExistenceCheck.NONE, version, behavior);
    }

    /**
     * Save the given record with a specific version.
     * The version is handled according to the behavior value. If behavior is <code>DEFAULT</code> then
     * the method acts as {@link #saveRecord(Message, FDBRecordVersion)}. If behavior is <code>NO_VERSION</code> then
     * <code>version</code> is ignored and no version is saved. If behavior is <code>WITH_VERSION</code> then the value
     * of <code>version</code>  is stored as given by the caller.
     * @param rec the record to be saved
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @param version the version to associate with the record when saving
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> saveRecord(@Nonnull final M rec, @Nonnull RecordExistenceCheck existenceCheck,
                                         @Nullable final FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD, saveRecordAsync(rec, existenceCheck, version, behavior));
    }

    /**
     * Save the given record and throw an exception if a record already exists with the same primary key.
     * @param rec the record to be saved
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> insertRecordAsync(@Nonnull final M rec) {
        return saveRecordAsync(rec, RecordExistenceCheck.ERROR_IF_EXISTS);
    }

    /**
     * Save the given record and throw an exception if a record already exists with the same primary key.
     * @param rec the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> insertRecord(@Nonnull final M rec) {
        return saveRecord(rec, RecordExistenceCheck.ERROR_IF_EXISTS);
    }

    /**
     * Save the given record and throw an exception if the record does not already exist in the database.
     * @param rec the record to be saved
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> updateRecordAsync(@Nonnull final M rec) {
        return saveRecordAsync(rec, RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
    }

    /**
     * Save the given record and throw an exception if the record does not already exist in the database.
     * @param rec the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    default FDBStoredRecord<M> updateRecord(@Nonnull final M rec) {
        return saveRecord(rec, RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
    }

    /**
     * Load the record with the given primary key.
     * @param primaryKey the primary key for the record
     * @return a {@link FDBStoredRecord} for the record or <code>null</code>.
     */
    @Nullable
    default FDBStoredRecord<M> loadRecord(@Nonnull final Tuple primaryKey) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, loadRecordAsync(primaryKey));
    }

    /**
     * Load the record with the given primary key.
     * @param primaryKey the primary key for the record
     * @param snapshot whether to load at snapshot isolation
     * @return a {@link FDBStoredRecord} for the record or <code>null</code>.
     */
    @Nullable
    default FDBStoredRecord<M> loadRecord(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, loadRecordAsync(primaryKey, snapshot));
    }

    /**
     * Asynchronously load a record.
     * @param primaryKey the key for the record to be loaded
     * @return a CompletableFuture that will return a message or null if there was no record with that key
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> loadRecordAsync(@Nonnull final Tuple primaryKey) {
        return loadRecordAsync(primaryKey, false);
    }

    /**
     * Asynchronously load a record.
     * @param primaryKey the key for the record to be loaded
     * @param snapshot whether to load at snapshot isolation
     * @return a CompletableFuture that will return a message or null if there was no record with that key
     */
    @Nonnull
    default CompletableFuture<FDBStoredRecord<M>> loadRecordAsync(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        return loadRecordInternal(primaryKey, ExecuteState.NO_LIMITS, snapshot);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    CompletableFuture<FDBStoredRecord<M>> loadRecordInternal(@Nonnull Tuple primaryKey, @Nonnull ExecuteState executeState, boolean snapshot);

    /**
     * Get record into FDB RYW cache.
     * Caller needs to hold on to result until ready or else there is a chance it will get
     * GC'ed and cancelled before then.
     * @param primaryKey the primary key for the record to retrieve
     * @return a future that will return {@code null} when the record is preloaded
     */
    @Nonnull
    CompletableFuture<Void> preloadRecordAsync(@Nonnull Tuple primaryKey);

    /**
     * Check if a record exists in the record store with the given primary key.
     * This performs its reads at the {@link IsolationLevel#SERIALIZABLE} isolation level.
     *
     * @param primaryKey the primary key of the record
     * @return a future that will complete to <code>true</code> if some record in record store has that primary key and
     *     <code>false</code> otherwise
     * @see #recordExistsAsync(Tuple, IsolationLevel)
     */
    @Nonnull
    default CompletableFuture<Boolean> recordExistsAsync(@Nonnull final Tuple primaryKey) {
        return recordExistsAsync(primaryKey, IsolationLevel.SERIALIZABLE);
    }

    /**
     * Check if a record exists in the record store with the given primary key.
     * This is slightly more efficient than loading the record and checking if that record is <code>null</code>
     * as it does not have to deserialize the record, though the record's contents are still read from the
     * database and sent over the network.
     *
     * @param primaryKey the primary key of the record
     * @param isolationLevel the isolation level to use when reading
     * @return a future that will complete to <code>true</code> if some record in record store has that primary key and
     *     <code>false</code> otherwise
     */
    @Nonnull
    CompletableFuture<Boolean> recordExistsAsync(@Nonnull final Tuple primaryKey, @Nonnull final IsolationLevel isolationLevel);

    /**
     * Check if a record exists in the record store with the given primary key.
     * This method is blocking. For the non-blocking version of this method, see {@link #recordExistsAsync(Tuple)}.
     *
     * @param primaryKey the primary key of the record
     * @return <code>true</code> if some record in record store has that primary key and <code>false</code> otherwise
     * @see #recordExistsAsync(Tuple)
     */
    default boolean recordExists(@Nonnull final Tuple primaryKey) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_RECORD_EXISTS, recordExistsAsync(primaryKey));
    }

    /**
     * Check if a record exists in the record store with the given primary key.
     * This method is blocking. For the non-blocking version of this method, see {@link #recordExistsAsync(Tuple, IsolationLevel)}.
     *
     * @param primaryKey the primary key of the record
     * @param isolationLevel the isolation level to use when reading
     * @return <code>true</code> if some record in record store has that primary key and <code>false</code> otherwise
     * @see #recordExistsAsync(Tuple)
     */
    default boolean recordExists(@Nonnull final Tuple primaryKey, @Nonnull final IsolationLevel isolationLevel) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_RECORD_EXISTS, recordExistsAsync(primaryKey, isolationLevel));
    }

    /**
     * Add a read conflict as if one had read the record with the given primary key. This will cause this transaction
     * to fail (with a {@link com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionConflictException})
     * if a concurrent transaction modifies the record with the provided primary key. This call however does not require
     * performing any reads against the database, so it is faster and cheaper to perform than a real read. Note also that
     * read-only operations are not checked for conflicts, so if this method is called, but the transaction performs
     * no mutations, the transaction will never be failed with the above exception. Note also that this does not
     * check that a record with this primary key actually exists in the database.
     *
     * <p>
     * One use case is that this can be used to promote a read from {@link IsolationLevel#SNAPSHOT} to
     * {@link IsolationLevel#SERIALIZABLE}. For example, if one performs a query at {@link IsolationLevel#SNAPSHOT} and
     * then uses a subset of the records to determine a few other writes, then one can add conflicts to <em>only</em>
     * the records actually used.
     * </p>
     *
     * <p>
     * This method should be used with care and is advised only for those users who need extra control over conflict
     * ranges.
     * </p>
     *
     * @param primaryKey the primary key of the record to add a read conflict on
     * @see com.apple.foundationdb.Transaction#addReadConflictRange(byte[], byte[])
     */
    void addRecordReadConflict(@Nonnull final Tuple primaryKey);

    /**
     * Add a write conflict as if one had modified the record with the given primary key. This will cause any concurrent
     * transactions to fail (with a {@link com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionConflictException})
     * if they read the record with the provided primary key. This call however does not require performing any writes
     * against the database, so it is faster and cheaper to perform than a real write. Note that this does not check
     * if a record with this primary key actually exists in the database, and it does not update any indexes associated
     * with the record. In this way, it is identical (in terms of conflicts) with overwriting the given record with itself,
     * though it will not induce any disk I/O or cause any {@linkplain com.apple.foundationdb.Transaction#watch(byte[]) watches}
     * on the modified keys to fire.
     *
     * <p>
     * This method should be used with care and is advised only for those users who need extra control over conflict
     * ranges.
     * </p>
     *
     * @param primaryKey the primary key of the record to add a write conflict on
     * @see com.apple.foundationdb.Transaction#addWriteConflictRange(byte[], byte[])
     */
    void addRecordWriteConflict(@Nonnull final Tuple primaryKey);

    /**
     * Scan the records in the database.
     *
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     *
     * @return a cursor that will scan everything in the range, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    default RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, scanProperties);
    }

    /**
     * Scan the records in the database in a range.
     *
     * @param range the range to scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     *
     * @return a cursor that will scan everything in the range, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    default RecordCursor<FDBStoredRecord<M>> scanRecords(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return scanRecords(range.getLow(), range.getHigh(), range.getLowEndpoint(), range.getHighEndpoint(), continuation, scanProperties);
    }

    /**
     * Scan the records in the database in a range.
     *
     * @param low low point of scan range
     * @param high high point of scan point
     * @param lowEndpoint whether low point is inclusive or exclusive
     * @param highEndpoint whether high point is inclusive or exclusive
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     *
     * @return a cursor that will scan everything in the range, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable Tuple low, @Nullable Tuple high,
                                                 @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint,
                                                 @Nullable byte[] continuation,
                                                 @Nonnull ScanProperties scanProperties);

    /**
     * Count the number of records in the database in a range.
     *
     * @param low low point of scan range
     * @param high high point of scan point
     * @param lowEndpoint whether low point is inclusive or exclusive
     * @param highEndpoint whether high point is inclusive or exclusive
     *
     * @return a future that will complete with the number of records in the range
     */
    @Nonnull
    default CompletableFuture<Integer> countRecords(@Nullable Tuple low, @Nullable Tuple high,
                                                    @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        return countRecords(low, high, lowEndpoint, highEndpoint, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Count the number of records in the database in a range.
     *
     * @param low low point of scan range
     * @param high high point of scan point
     * @param lowEndpoint whether low point is inclusive or exclusive
     * @param highEndpoint whether high point is inclusive or exclusive
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     *
     * @return a future that will complete with the number of records in the range
     */
    @Nonnull
    CompletableFuture<Integer> countRecords(@Nullable Tuple low, @Nullable Tuple high,
                                            @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint,
                                            @Nullable byte[] continuation,
                                            @Nonnull ScanProperties scanProperties);

    /**
     * Scan the entries in an index.
     * @param index the index to scan
     * @param scanBounds the scan to preform
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will scan the index, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanBounds scanBounds, @Nullable byte[] continuation,
                                       @Nonnull ScanProperties scanProperties);

    /**
     * Scan the entries in an index.
     * @param index the index to scan
     * @param scanType the type of scan to perform
     * @param range range to scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will scan the index, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    default RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanType scanType,
                                               @Nonnull TupleRange range, @Nullable byte[] continuation,
                                               @Nonnull ScanProperties scanProperties) {
        return scanIndex(index, new IndexScanRange(scanType, range), continuation, scanProperties);
    }

    /**
     * Scan the records pointed to by an index.
     * @param indexName the name of the index
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName) {
        return scanIndexRecords(indexName, IsolationLevel.SERIALIZABLE);
    }

    /**
     * Scan the records pointed to by an index.
     * @param indexName the name of the index
     * @param isolationLevel the isolation level to use when reading
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName, IsolationLevel isolationLevel) {
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationLevel).build()));
    }

    /**
     * Scan the records pointed to by an index.
     * @param indexName the name of the index
     * @param scanType the type of scan to perform
     * @param range the range of the index to scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName,
                                                               @Nonnull final IndexScanType scanType,
                                                               @Nonnull final TupleRange range,
                                                               @Nullable byte[] continuation,
                                                               @Nonnull ScanProperties scanProperties) {
        return scanIndexRecords(indexName, scanType, range, continuation, IndexOrphanBehavior.ERROR, scanProperties);
    }

    /**
     * Scan the records pointed to by an index.
     * @param indexName the name of the index
     * @param scanType the type of scan to perform
     * @param range the range of the index to scan
     * @param continuation any continuation from a previous scan
     * @param orphanBehavior how the iteration process should respond in the face of entries in the index for which
     *    there is no associated record
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName,
                                                               @Nonnull final IndexScanType scanType,
                                                               @Nonnull final TupleRange range,
                                                               @Nullable byte[] continuation,
                                                               @Nonnull IndexOrphanBehavior orphanBehavior,
                                                               @Nonnull ScanProperties scanProperties) {
        final Index index = getRecordMetaData().getIndex(indexName);
        return fetchIndexRecords(scanIndex(index, scanType, range, continuation, scanProperties), orphanBehavior,
                scanProperties.getExecuteProperties().getState());
    }

    /**
     * Given a cursor that iterates over entries in an index, attempts to fetch the associated records for those entries.
     *
     * @param indexCursor a cursor iterating over entries in the index
     * @param orphanBehavior how the iteration process should respond in the face of entries in the index for which
     *    there is no associated record
     * @return a cursor returning indexed record entries
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> fetchIndexRecords(@Nonnull RecordCursor<IndexEntry> indexCursor,
                                                                @Nonnull IndexOrphanBehavior orphanBehavior) {
        return fetchIndexRecords(indexCursor, orphanBehavior, ExecuteState.NO_LIMITS);
    }

    /**
     * Given a cursor that iterates over entries in an index, attempts to fetch the associated records for those entries.
     *
     * @param indexCursor A cursor iterating over entries in the index.
     * @param orphanBehavior How the iteration process should respond in the face of entries in the index for which
     *    there is no associated record.
     * @param executeState the {@link ExecuteState} associated with this query execution
     * @return A cursor returning indexed record entries.
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> fetchIndexRecords(@Nonnull RecordCursor<IndexEntry> indexCursor,
                                                                @Nonnull IndexOrphanBehavior orphanBehavior,
                                                                @Nonnull ExecuteState executeState) {
        RecordCursor<FDBIndexedRecord<M>> recordCursor = indexCursor.mapPipelined(entry ->
                loadIndexEntryRecord(entry, orphanBehavior, executeState), getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
        if (orphanBehavior == IndexOrphanBehavior.SKIP) {
            recordCursor = recordCursor.filter(Objects::nonNull);
        }
        return recordCursor;
    }

    /**
     * Scan the records pointed to by an index equal to indexed values.
     * @param indexName the name of the index
     * @param values a left-subset of values of indexed fields
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecordsEqual(@Nonnull final String indexName, @Nonnull final Object... values) {
        final Tuple tuple = Tuple.from(values);
        final TupleRange range = TupleRange.allOf(tuple);
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Scan the records pointed to by an index between two indexed values.
     * @param indexName the name of the index
     * @param low the low value for the first indexed field
     * @param high the high value for the first indexed field
     * @return a cursor that return records pointed to by the index
     */
    @Nonnull
    default RecordCursor<FDBIndexedRecord<M>> scanIndexRecordsBetween(@Nonnull final String indexName,
                                                                      @Nullable final Object low, @Nullable final Object high) {
        final Tuple lowTuple = Tuple.from(low);
        final Tuple highTuple = Tuple.from(high);
        final TupleRange range = new TupleRange(lowTuple, highTuple,
                EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Determine if a given index entry points to a record.
     * @param entry the index entry to check
     * @param isolationLevel whether to use snapshot read
     * @return a future that completes with {@code true} if the given index entry still points to a record
     */
    @Nonnull
    default CompletableFuture<Boolean> hasIndexEntryRecord(@Nonnull final IndexEntry entry,
                                                           @Nonnull final IsolationLevel isolationLevel) {
        return recordExistsAsync(entry.getPrimaryKey(), isolationLevel);
    }

    /**
     * Using the given index entry, resolve the primary key and asynchronously return the referenced record.
     * @param entry the index entry to be resolved
     * @param orphanBehavior the {@link IndexOrphanBehavior} to apply if the record is not found
     * @return the record referred to by the given index entry
     */
    @Nonnull
    default CompletableFuture<FDBIndexedRecord<M>> loadIndexEntryRecord(@Nonnull final IndexEntry entry,
                                                                        @Nonnull final IndexOrphanBehavior orphanBehavior) {
        return loadIndexEntryRecord(entry, orphanBehavior, ExecuteState.NO_LIMITS);
    }

    /**
     * Using the given index entry, resolve the primary key and asynchronously return the referenced record.
     * @param entry the index entry to be resolved
     * @param orphanBehavior the {@link IndexOrphanBehavior} to apply if the record is not found
     * @param executeState an execution state object to be used to enforce limits on query execution
     * @return the record referred to by the given index entry
     */
    @Nonnull
    default CompletableFuture<FDBIndexedRecord<M>> loadIndexEntryRecord(@Nonnull final IndexEntry entry,
                                                                        @Nonnull final IndexOrphanBehavior orphanBehavior,
                                                                        @Nonnull final ExecuteState executeState) {
        final Tuple primaryKey = entry.getPrimaryKey();
        return loadRecordInternal(primaryKey, executeState,false).thenApply(rec -> {
            if (rec == null) {
                switch (orphanBehavior) {
                    case SKIP:
                        return null;
                    case RETURN:
                        break;
                    case ERROR:
                        if (getTimer() != null) {
                            getTimer().increment(FDBStoreTimer.Counts.BAD_INDEX_ENTRY);
                        }
                        throw new RecordCoreStorageException("record not found from index entry").addLogInfo(
                                LogMessageKeys.INDEX_NAME, entry.getIndex().getName(),
                                LogMessageKeys.PRIMARY_KEY, primaryKey,
                                LogMessageKeys.INDEX_KEY, entry.getKey(),
                                getSubspaceProvider().logKey(), getSubspaceProvider().toString(getContext()));
                    default:
                        throw new RecordCoreException("Unexpected index orphan behavior: " + orphanBehavior);
                }
            }
            return new FDBIndexedRecord<>(entry, rec);
        });
    }

    /**
     * Return a tuple to be used as the key for an index entry for the given value and primary key.
     * @param index the index for which this will be an entry
     * @param valueKey the indexed value(s) for the entry
     * @param primaryKey the primary key for the record
     * @return the key to use for an index entry, the two tuples appended with redundant parts of the primary key removed
     */
    @Nonnull
    static Tuple indexEntryKey(@Nonnull Index index, @Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        List<Object> primaryKeys = primaryKey.getItems();
        index.trimPrimaryKey(primaryKeys);
        if (primaryKeys.isEmpty()) {
            return valueKey;
        } else {
            return valueKey.addAll(primaryKeys);
        }
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version only selects violations that have the
     * given key as the uniqueness violation key.
     *
     * @param index the index to scan the uniqueness violations of
     * @param valueKey the key (as a tuple) of the index whose violations to scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        TupleRange range = TupleRange.allOf(valueKey);
        return scanUniquenessViolations(index, range, continuation, scanProperties);
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version only selects violations that have the
     * given key as the uniqueness violation key.
     *
     * @param index the index to scan the uniqueness violations of
     * @param indexKey the key of the index whose violations to scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Key.Evaluated indexKey, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return scanUniquenessViolations(index, indexKey.toTuple(), continuation, scanProperties);
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version only selects violations that have the
     * given key as the uniqueness violation key. It does not limit the number of responses it returns.
     *
     * @param index the index to scan the uniqueness violations of
     * @param valueKey the key (as a tuple) of the index whose violations to scan
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Tuple valueKey) {
        return scanUniquenessViolations(index, valueKey, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version only selects violations that have the
     * given key as the uniqueness violation key. It does not limit the number of responses it
     * returns.
     *
     * @param index the index to scan the uniqueness violations of
     * @param indexKey the key of the index whose violations to scan
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Key.Evaluated indexKey) {
        return scanUniquenessViolations(index, indexKey, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version tries to retrieve all of the violations it can
     * subject to the limit specified.
     *
     * @param index the index to scan the uniqueness violations of
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return scanUniquenessViolations(index, TupleRange.ALL, continuation, scanProperties);
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version tries to retrieve all of the violations it can
     * subject to the limit specified.
     *
     * @param index the index to scan the uniqueness violations of
     * @param limit the maximum number of uniqueness violations to report
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, int limit) {
        return scanUniquenessViolations(index, null, new ScanProperties(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(limit)
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .build()));
    }

    /**
     * Scan the list of uniqueness violations for an index for violations with a specific value.
     * This is similar to the version of {@link FDBRecordStoreBase#scanUniquenessViolations(Index, TupleRange, byte[], ScanProperties) scanUniquenessViolations()}
     * that takes a {@link TupleRange}, but this version tries to retrieve all of the violations it can. It
     * does not try to limit its results.
     *
     * @param index the index to scan the uniqueness violations of
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    default RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index) {
        return scanUniquenessViolations(index, Integer.MAX_VALUE);
    }

    /**
     * Scan the list of uniqueness violations identified for an index. It looks only for violations
     * within the given range subject to the given limit and (possibly) will go in reverse.
     * They will be returned in an order that is grouped by the index value keys that they have in common
     * and will be ordered within the grouping by the primary key.
     *
     * <p>
     * Because of how the data are stored, each primary key that is part of a uniqueness violation
     * will appear at most once for each index key that is causing a violation. The associated
     * existing key is going to be one of the other keys, but it might not be the only one.
     * This means that the total number of violations per index key is capped at the number of records in the
     * store (rather than the square), but it also means that the existing key data is of limited help.
     *
     * @param index the index to scan the uniqueness violations of
     * @param range the range of tuples to include in the scan
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     * @return a cursor that will return uniqueness violations stored for the given index in the given store
     */
    @Nonnull
    RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull TupleRange range,
                                                                          @Nullable byte[] continuation,
                                                                          @Nonnull ScanProperties scanProperties);

    /**
     * Removes all of the records that have the given value set as their index index value (are thus causing
     * a uniqueness violation) except for the one that has the given primary key (if the key is not <code>null</code>).
     * This is like the version of {@link FDBRecordStoreBase#resolveUniquenessViolation(Index, Tuple, Tuple) resolveUniquenessViolation()}
     * that takes a {@link Tuple}, but this takes the index value as a {@link Key.Evaluated} instead.
     * @param index the index to resolve uniqueness violations for
     * @param indexKey the value of the index that is being removed
     * @param primaryKey the primary key of the record that should remain (or <code>null</code> to remove all of them)
     * @return a future that will complete when all of the records have been removed
     */
    @Nonnull
    default CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Key.Evaluated indexKey, @Nullable Tuple primaryKey) {
        return resolveUniquenessViolation(index, indexKey.toTuple(), primaryKey);
    }

    /**
     * Removes all of the records that have the given value set as their index value (and are thus causing a
     * uniqueness violation) except for the one that has the given primary key (if the key is not <code>null</code>).
     * It also cleans up the set of uniqueness violations so that none of the remaining entries will
     * be associated with the given value key.
     * @param index the index to resolve uniqueness violations for
     * @param valueKey the value of the index that is being removed
     * @param primaryKey the primary key of the record that should remain (or <code>null</code> to remove all of them)
     * @return a future that will complete when all of the records have been removed
     */
    @Nonnull
    CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable Tuple primaryKey);

    /**
     * Return the key portion of <code>entry</code>, which should be the key with the index value
     * as a tuple. This is used to store the index uniqueness violations when building a
     * unique index.
     * @param valueKey the value of the index for a record
     * @param primaryKey the primary key for a record
     * @return a tuple that is the two keys appended together
     */
    @Nonnull
    static Tuple uniquenessViolationKey(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        return valueKey.addAll(primaryKey);
    }

    /**
     * Async version of {@link #deleteRecord}.
     * @param primaryKey the primary key of the record to delete
     * @return a future that completes {@code true} if the record was present to be deleted
     */
    @Nonnull
    CompletableFuture<Boolean> deleteRecordAsync(@Nonnull final Tuple primaryKey);

    /**
     * Delete the record with the given primary key.
     *
     * @param primaryKey the primary key for the record to be deleted
     *
     * @return true if something was there to delete, false if the record didn't exist
     */
    default boolean deleteRecord(@Nonnull Tuple primaryKey) {
        return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordAsync(primaryKey));
    }

    /**
     * Delete all the data in the record store.
     * <p>
     * Everything except the store header and index state information is cleared from the database.
     * This is is an efficient operation as all data are contiguous.
     * This means that any {@linkplain IndexState#DISABLED disabled} or {@linkplain IndexState#WRITE_ONLY write-only}
     * index will remain in its disabled or write-only state after all of the data are cleared. If one also wants
     * to reset all index states, one can call {@link FDBRecordStore#rebuildAllIndexes()}, which should complete
     * quickly on an empty record store. If one wants to remove the record store entirely (including the store
     * header and all index states), one should call {@link FDBRecordStore#deleteStore(FDBRecordContext, KeySpacePath)}
     * instead of this method.
     *
     * <p>
     * Note that, at the moment, this operation also has the side effect of resetting
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#MAX_EVER_TUPLE MAX_EVER} and
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#MIN_EVER_TUPLE MIN_EVER} indexes.
     * See: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/398">Issue #398</a>.
     * </p>
     *
     * @see FDBRecordStore#deleteStore(FDBRecordContext, KeySpacePath)
     * @see FDBRecordStore#deleteStore(FDBRecordContext, Subspace)
     */
    void deleteAllRecords();

    /**
     * Delete records and associated index entries matching a query filter.
     * <p>
     * Throws an exception if the operation cannot be done efficiently in a small number of contiguous range
     * clears. In practice, this means that the query filter must constrain a prefix of all record types' primary keys
     * and of all indexes' root expressions.
     *
     * @param component the query filter for records to delete efficiently
     */
    default void deleteRecordsWhere(@Nonnull QueryComponent component) {
        getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordsWhereAsync(component));
    }

    /**
     * Delete records and associated index entries matching a query filter.
     * <p>
     * Throws an exception if the operation cannot be done efficiently in a small number of contiguous range
     * clears. In practice, this means both that all record types must have a record type key prefix and
     * that the query filter must constrain a prefix of all record types' primary keys and of all indexes' root
     * expressions.
     *
     * @param recordType the type of records to delete
     * @param component the query filter for records to delete efficiently or {@code null} to delete all records of the given type
     */
    default void deleteRecordsWhere(@Nonnull String recordType, @Nullable QueryComponent component) {
        getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordsWhereAsync(recordType, component));
    }

    /**
     * Async version of {@link #deleteRecordsWhereAsync}.
     *
     * @param component the query filter for records to delete efficiently
     * @return a future that will be complete when the delete is done
     */
    @Nonnull
    CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull QueryComponent component);

    /**
     * Async version of {@link #deleteRecordsWhere(String, QueryComponent)}.
     * @param recordType the type of records to delete
     * @param component the query filter for records to delete efficiently or {@code null} to delete all records of the given type
     * @return a future that will be complete when the delete is done
     */
    @Nonnull
    default CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull String recordType, @Nullable QueryComponent component) {
        return deleteRecordsWhereAsync(FDBRecordStore.mergeRecordTypeAndComponent(recordType, component));
    }

    /**
     * Function for computing the number of elements to allow in the asynchronous pipeline for an operation of the given
     * type.
     */
    interface PipelineSizer {
        int getPipelineSize(@Nonnull PipelineOperation pipelineOperation);
    }

    /**
     * Get the function for computing the number of elements to allow in the asynchronous pipeline for an operation of the given
     * type.
     * @return the pipeline sizer
     */
    @Nonnull
    PipelineSizer getPipelineSizer();

    /**
     * Get the number of elements to allow in the asynchronous pipeline for an operation of the given type.
     * @param pipelineOperation the operation
     * @return the number of elements to pipeline
     */
    default int getPipelineSize(@Nonnull PipelineOperation pipelineOperation) {
        return getPipelineSizer().getPipelineSize(pipelineOperation);
    }

    /**
     * Compute an estimated size of the store in bytes. The estimate will include all data in the store, including
     * all records and indexes.
     *
     * <p>
     * This uses a sample maintained by the database to efficiently compute an estimate for the size of the store
     * without needing to scan all data. Because keys in this data structure are sampled, the value will not be
     * exact. If an exact size is needed, the
     * {@link com.apple.foundationdb.record.provider.foundationdb.cursors.SizeStatisticsCollectorCursor} can
     * be used, though note that that cursor must read the entire store to produce its statistics.
     * </p>
     *
     * @return a future that will contain an estimate for the size of the store
     */
    @Nonnull
    CompletableFuture<Long> estimateStoreSizeAsync();

    /**
     * Compute an estimated size of all records in the store in bytes. The estimate will only include the space used
     * by the records and excludes all other data maintained by the store. (For example, index data is <em>not</em>
     * included in the returned estimate.)
     *
     * <p>
     * This uses the same method for computing the estimate as {@link #estimateStoreSizeAsync()}.
     * </p>
     *
     * @return a future that will contain an estimate for the size of all records in the store
     * @see #estimateStoreSizeAsync()
     */
    @Nonnull
    default CompletableFuture<Long> estimateRecordsSizeAsync() {
        return estimateRecordsSizeAsync(TupleRange.ALL);
    }

    /**
     * Compute an estimated size in bytes of all records in the store within the given primary key range. The estimate
     * will only include the space used by the records and excludes all other data maintained by the store. (For
     * example, index data is <em>not</em> included in the returned estimate.)
     *
     * <p>
     * This uses the same method for computing the estimate as {@link #estimateStoreSizeAsync()}.
     * </p>
     *
     * @param range range of records to estimate the size of
     * @return a future that will contain an estimate for the size of all records in the store
     * @see #estimateStoreSizeAsync()
     */
    @Nonnull
    CompletableFuture<Long> estimateRecordsSizeAsync(@Nonnull TupleRange range);

    /**
     * Get the number of records in the record store.
     *
     * There must be a suitable {@code COUNT} type index defined.
     * @return a future that will complete to the number of records in the store
     */
    @Nonnull
    default CompletableFuture<Long> getSnapshotRecordCount() {
        return getSnapshotRecordCount(EmptyKeyExpression.EMPTY, Key.Evaluated.EMPTY);
    }

    /**
     * Get the number of records in a portion of the record store determined by a group key expression.
     *
     * There must be a suitably grouped {@code COUNT} type index defined.
     * @param key the grouping key expression
     * @param value the value of {@code key} to match
     * @return a future that will complete to the number of records
     */
    @Nonnull
    default CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value) {
        // Using IndexQueryabilityFilter.TRUE probably isn't ideal here, but is used to preserve backwards
        // compatibility
        return getSnapshotRecordCount(key, value, IndexQueryabilityFilter.TRUE);
    }

    /**
     * Get the number of records in a portion of the record store determined by a group key expression.
     *
     * There must be a suitably grouped, readable {@code COUNT} type index defined, that is not filtered by
     * {@code indexQueryabilityFilter}, or a suitable record count key on the metadata.
     * @param key the grouping key expression
     * @param value the value of {@code key} to match
     * @param indexQueryabilityFilter a filter to restrict which indexes can be used when planning. If there is a
     * record count key, that may be used, and will not be checked against this filter.
     * @return a future that will complete to the number of records
     */
    @Nonnull
    CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value,
                                                   @Nonnull IndexQueryabilityFilter indexQueryabilityFilter);

    /**
     * Get the number of records in the record store of the given record type.
     *
     * The record type must have a {@code COUNT} index defined for it.
     * @param recordTypeName record type for which to count records
     * @return a future that will complete to the number of records
     */
    @Nonnull
    default CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName) {
        // Using IndexQueryabilityFilter.TRUE probably isn't ideal here, but is used to preserve backwards
        // compatibility
        return getSnapshotRecordCountForRecordType(recordTypeName, IndexQueryabilityFilter.TRUE);
    }

    /**
     * Get the number of records in the record store of the given record type.
     *
     * The record type must have a readable {@code COUNT} index defined for it, that is not excluded by the
     * {@code indexQueryabilityFilter}.
     * @param recordTypeName record type for which to count records
     * @param indexQueryabilityFilter a filter to restrict which indexes can be used when planning.
     * @return a future that will complete to the number of records
     */
    @Nonnull
    CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName,
                                                                @Nonnull IndexQueryabilityFilter indexQueryabilityFilter);

    default CompletableFuture<Long> getSnapshotRecordUpdateCount() {
        return getSnapshotRecordUpdateCount(EmptyKeyExpression.EMPTY, Key.Evaluated.EMPTY);
    }

    default CompletableFuture<Long> getSnapshotRecordUpdateCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value) {
        // Using IndexQueryabilityFilter.TRUE probably isn't ideal here, but is used to preserve backwards
        // compatibility
        return getSnapshotRecordUpdateCount(key, value, IndexQueryabilityFilter.TRUE);
    }

    default CompletableFuture<Long> getSnapshotRecordUpdateCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value,
                                                                 @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        return evaluateAggregateFunction(
                Collections.emptyList(), IndexFunctionHelper.countUpdates(key), TupleRange.allOf(value.toTuple()),
                IsolationLevel.SNAPSHOT, indexQueryabilityFilter)
                .thenApply(tuple -> tuple.getLong(0));
    }

    /**
     * Evaluate a {@link RecordFunction} against a record.
     * @param function the function to evaluate
     * @param rec the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    default <T> CompletableFuture<T> evaluateRecordFunction(@Nonnull RecordFunction<T> function,
                                                            @Nonnull FDBRecord<M> rec) {
        return evaluateRecordFunction(EvaluationContext.EMPTY, function, rec);
    }

    /**
     * Evaluate a {@link RecordFunction} against a record.
     * @param evaluationContext evaluation context containing parameter bindings
     * @param function the function to evaluate
     * @param rec the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    default <T> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                            @Nonnull RecordFunction<T> function,
                                                            @Nonnull FDBRecord<M> rec) {
        if (function instanceof IndexRecordFunction<?>) {
            IndexRecordFunction<T> indexRecordFunction = (IndexRecordFunction<T>)function;
            return evaluateIndexRecordFunction(evaluationContext, indexRecordFunction, rec);
        } else if (function instanceof StoreRecordFunction<?>) {
            StoreRecordFunction<T> storeRecordFunction = (StoreRecordFunction<T>)function;
            return evaluateStoreFunction(evaluationContext, storeRecordFunction, rec);
        }
        throw new RecordCoreException("Cannot evaluate record function " + function);
    }

    /**
     * Evaluate a {@link IndexRecordFunction} against a record.
     * @param <T> the type of the result
     * @param evaluationContext evaluation context containing parameter bindings
     * @param function the function to evaluate
     * @param rec the record to evaluate against
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    <T> CompletableFuture<T> evaluateIndexRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                         @Nonnull IndexRecordFunction<T> function,
                                                         @Nonnull FDBRecord<M> rec);

    /**
     * Evaluate a {@link StoreRecordFunction} against a record.
     * @param <T> the type of the result
     * @param function the function to evaluate
     * @param rec the record to evaluate against
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    default <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull StoreRecordFunction<T> function,
                                                           @Nonnull FDBRecord<M> rec) {
        return evaluateStoreFunction(EvaluationContext.EMPTY, function, rec);
    }

    /**
     * Evaluate a {@link StoreRecordFunction} against a record.
     * @param <T> the type of the result
     * @param evaluationContext evaluation context containing parameter bindings
     * @param function the function to evaluate
     * @param rec the record to evaluate against
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull EvaluationContext evaluationContext,
                                                   @Nonnull StoreRecordFunction<T> function,
                                                   @Nonnull FDBRecord<M> rec);

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     *
     * Before calling {@link #evaluateAggregateFunction(List, IndexAggregateFunction, TupleRange, IsolationLevel)},
     * this overload adjusts the given range to include any prefix in the function itself.
     * @param evaluationContext evaluation context containing parameter bindings
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @param isolationLevel whether to use snapshot reads
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    default CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull EvaluationContext evaluationContext,
                                                               @Nonnull List<String> recordTypeNames,
                                                               @Nonnull IndexAggregateFunction aggregateFunction,
                                                               @Nonnull TupleRange range,
                                                               @Nonnull IsolationLevel isolationLevel) {
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction,
                aggregateFunction.adjustRange(evaluationContext, range), isolationLevel);
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a group value.
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param value the value for the group key(s)
     * @param isolationLevel whether to use snapshot reads
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    default CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                               @Nonnull IndexAggregateFunction aggregateFunction,
                                                               @Nonnull Key.Evaluated value,
                                                               @Nonnull IsolationLevel isolationLevel) {
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction, TupleRange.allOf(value.toTuple()), isolationLevel);
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @param isolationLevel whether to use snapshot reads
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    default CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                               @Nonnull IndexAggregateFunction aggregateFunction,
                                                               @Nonnull TupleRange range,
                                                               @Nonnull IsolationLevel isolationLevel) {
        // Using IndexQueryabilityFilter.TRUE probably isn't ideal here, but is used to preserve backwards
        // compatibility
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction, range, isolationLevel,
                IndexQueryabilityFilter.TRUE);
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @param isolationLevel whether to use snapshot reads
     * @param indexQueryabilityFilter a filter to restrict which indexes can be used when planning. This will not be
     * consulted if the aggregateFunction already has a readable index
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                       @Nonnull IndexAggregateFunction aggregateFunction,
                                                       @Nonnull TupleRange range,
                                                       @Nonnull IsolationLevel isolationLevel,
                                                       @Nonnull IndexQueryabilityFilter indexQueryabilityFilter);

    /**
     * Get a query result record from a stored record.
     * This is from a direct record scan / lookup without an associated index.
     * @param storedRecord the stored record to convert to a queried record
     * @return a {@link FDBQueriedRecord} corresponding to {@code storedRecord}
     */
    @Nonnull
    default FDBQueriedRecord<M> queriedRecord(@Nonnull FDBStoredRecord<M> storedRecord) {
        return FDBQueriedRecord.stored(storedRecord);
    }

    /**
     * Get a query result record from an indexed record.
     * This is from an index scan and permits access to the underlying index entry.
     * @param indexedRecord the indexed record to convert to a queried record
     * @return a {@link FDBQueriedRecord} corresponding to {@code indexedRecord}
     */
    @Nonnull
    default FDBQueriedRecord<M> queriedRecord(@Nonnull FDBIndexedRecord<M> indexedRecord) {
        return FDBQueriedRecord.indexed(indexedRecord);
    }

    /**
     * Get a query result from a covering index entry.
     * The entire <code>StoredRecord</code> is not available, and the record only has fields from the index entry.
     * Normal indexes have a primary key in their entries, but aggregate indexes do not.
     * @param index the index from which the entry came
     * @param indexEntry the index entry
     * @param recordType the record type of the indexed record
     * @param partialRecord the partially populated Protobuf record
     * @param hasPrimaryKey whether the index entry has a primary key
     * @return a {@link FDBQueriedRecord} corresponding to {@code indexEntry}
     */
    @Nonnull
    default FDBQueriedRecord<M> coveredIndexQueriedRecord(@Nonnull Index index, @Nonnull IndexEntry indexEntry, @Nonnull RecordType recordType,
                                                          @Nonnull M partialRecord, boolean hasPrimaryKey) {
        return FDBQueriedRecord.covered(index, indexEntry,
                hasPrimaryKey ? index.getEntryPrimaryKey(indexEntry.getKey()) : TupleHelpers.EMPTY,
                recordType, partialRecord);
    }

    /**
     * Plan and execute a query.
     * @param query the query to plan and execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    default RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQuery query) {
        return executeQuery(planQuery(query));
    }

    /**
     * Plan and execute a query.
     * @param query the query to plan and execute
     * @param continuation continuation from a previous execution of this same query
     * @param executeProperties limits on execution
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    default RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQuery query,
                                                           @Nullable byte[] continuation,
                                                           @Nonnull ExecuteProperties executeProperties) {
        return executeQuery(planQuery(query), continuation, executeProperties);
    }

    /**
     * Execute a query.
     * @param plan the plan to execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    default RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQueryPlan plan) {
        return plan.execute(this, EvaluationContext.EMPTY);
    }

    /**
     * Execute a query.
     * @param plan the plan to execute
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    default RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQueryPlan plan,
                                                           @Nullable byte[] continuation,
                                                           @Nonnull ExecuteProperties executeProperties) {
        return plan.execute(this, EvaluationContext.EMPTY, continuation, executeProperties);
    }

    /**
     * Execute a query.
     * @param plan the plan to execute
     * @param continuation continuation from a previous execution of this same plan
     * @param evaluationContext a context for the execution of this plan
     * @param executeProperties limits on execution
     * @return a cursor for query results of type {@link QueryResult}
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    default RecordCursor<QueryResult> executePlan(@Nonnull RecordQueryPlan plan,
                                                  @Nullable byte[] continuation,
                                                  @Nonnull EvaluationContext evaluationContext,
                                                  @Nonnull ExecuteProperties executeProperties) {
        return plan.executePlan(this, evaluationContext, continuation, executeProperties);
    }

    /**
     * Plan a query.
     * @param query the query to plan
     * @param parameterRelationshipGraph a set of bindings and their relationships that provide additional information
     *        to the planner that may improve plan quality but may also tighten requirements imposed on the parameter
     *        bindings that are used to execute the query
     * @return a query plan
     * @see RecordQueryPlanner#plan
     */
    @Nonnull
    RecordQueryPlan planQuery(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph);

    /**
     * Plan a query.
     * @param query the query to plan
     * @return a query plan
     * @see RecordQueryPlanner#plan
     */
    @Nonnull
    default RecordQueryPlan planQuery(@Nonnull RecordQuery query) {
        return planQuery(query, ParameterRelationshipGraph.empty());
    }

    /**
     * Builder for {@link FDBRecordStoreBase}.
     * @param <M> type used to represent stored records
     * @param <R> type of built record store
     */
    interface BaseBuilder<M extends Message, R extends FDBRecordStoreBase<M>> {

        /**
         * Get the serializer used to convert records into byte arrays.
         * @return the serializer to use
         */
        @Nullable
        RecordSerializer<M> getSerializer();

        /**
         * Set the serializer used to convert records into byte arrays.
         * @param serializer the serializer to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setSerializer(@Nonnull RecordSerializer<M> serializer);

        /**
         * Get the storage format version for this store.
         * @return the format version to use
         */
        int getFormatVersion();

        /**
         * Set the storage format version for this store.
         *
         * Normally, this should be set to the highest format version supported by all code that may access the record
         * store. {@link #open} will set the store's format version to <code>max(max_supported_version, current_version)</code>.
         * This is to support cases where the target cannot be changed everywhere at once and some instances write the new version before others
         * know that they are licensed to do so. It is still <em>critically</em> important that <em>all</em> instances know how to handle
         * the new version before <em>any</em> instance allows it.
         *
         * When installing a new version of the record layer library that includes a format change, first install everywhere having arranged for
         * {@link #setFormatVersion} to be called with the <em>old</em> format version. Then, after that install is complete, change to the newer version.
         * @param formatVersion the format version to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setFormatVersion(int formatVersion);

        /**
         * Get the provider for the record store's meta-data.
         * @return the meta-data source to use
         */
        @Nullable
        RecordMetaDataProvider getMetaDataProvider();

        /**
         * Set the provider for the record store's meta-data.
         * If {@link #setMetaDataStore} is also called, the provider will only be used to initialize the meta-data store when it is empty. The record store will be built using the store as its provider.
         * @param metaDataProvider the meta-data source to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider);

        /**
         * Get the {@link FDBMetaDataStore} to use as the source of meta-data.
         * @return the meta-data store to use
         */
        @Nullable
        FDBMetaDataStore getMetaDataStore();

        /**
         * Set the {@link FDBMetaDataStore} to use as the source of meta-data.
         * If {@link #setMetaDataProvider} is also called, it will be used to seed the store.
         * @param metaDataStore the meta-data store to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore);

        /**
         * Get the record context (transaction) to use for the record store.
         * @return context the record context / transaction to use
         */
        @Nullable
        FDBRecordContext getContext();

        /**
         * Set the record context (transaction) to use for the record store.
         * @param context the record context / transaction to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setContext(@Nullable FDBRecordContext context);

        /**
         * Get the subspace provider.
         * @return the subspace provider
         */
        @Nullable
        SubspaceProvider getSubspaceProvider();

        /**
         * Set the subspace provider from a subspace provider.
         * @param subspaceProvider the subspace provider
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider);

        /**
         * Set the subspace to use for the record store.
         * The record store is allowed to use the entire subspace, so it should not overlap any other record store's subspace.
         * It is preferred to {@link #setKeySpacePath} rather than this because key space path provides more meaningful logs.
         * @param subspace the subspace to use
         * @return this builder
         */
        @Nonnull
        @API(API.Status.UNSTABLE)
        BaseBuilder<M, R> setSubspace(@Nullable Subspace subspace);

        /**
         * Set the key space path to use for the record store.
         * The record store is allowed to use the entire subspace, so it should not overlap any other record store's subspace.
         * Note: The context should be set before setting the key space path.
         * @param keySpacePath the key space path to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setKeySpacePath(@Nullable KeySpacePath keySpacePath);

        /**
         * Get the {@link FDBRecordStore.UserVersionChecker function} to be used to check the meta-data version of the record store.
         * @return the checker function to use
         */
        @Nullable
        UserVersionChecker getUserVersionChecker();

        /**
         * Set the {@link FDBRecordStore.UserVersionChecker function} to be used to check the meta-data version of the record store.
         * @param userVersionChecker the checker function to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setUserVersionChecker(@Nullable UserVersionChecker userVersionChecker);

        /**
         * Get the registry of index maintainers to be used by the record store.
         * @return the index registry to use
         */
        @Nonnull
        IndexMaintainerRegistry getIndexMaintainerRegistry();

        /**
         * Set the registry of index maintainers to be used by the record store.
         * @param indexMaintainerRegistry the index registry to use
         * @return this builder
         * @see FDBRecordStore#getIndexMaintainer
         * @see RecordMetaDataBuilder#setIndexMaintainerRegistry
         */
        @Nonnull
        BaseBuilder<M, R> setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry);

        /**
         * Get the {@link IndexMaintenanceFilter index filter} to be used by the record store.
         * @return the index filter to use
         */
        @Nonnull
        IndexMaintenanceFilter getIndexMaintenanceFilter();

        /**
         * Set the {@link IndexMaintenanceFilter index filter} to be used by the record store.
         * @param indexMaintenanceFilter the index filter to use
         * @return this builder
         */
        @Nonnull
        BaseBuilder<M, R> setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter);

        /**
         * Get the {@link FDBRecordStoreBase.PipelineSizer object} to be used to determine the depth of pipelines run by the record store.
         * @return the sizer to use
         */
        @Nonnull
        PipelineSizer getPipelineSizer();

        /**
         * Set the {@link PipelineSizer object} to be used to determine the depth of pipelines run by the record store.
         * @param pipelineSizer the sizer to use
         * @return this builder
         * @see FDBRecordStoreBase#getPipelineSize
         */
        @Nonnull
        BaseBuilder<M, R> setPipelineSizer(@Nonnull PipelineSizer pipelineSizer);

        /**
         * Get the store state cache to be used by the record store. If the builder returns {@code null}, the produced
         * record store will use the default store state cache provided by the {@link FDBDatabase} when initializing
         * the record store state.
         *
         * @return the store state cached used by this record store of {@code null} if it uses the database default
         */
        @API(API.Status.EXPERIMENTAL)
        @Nullable
        FDBRecordStoreStateCache getStoreStateCache();

        /**
         * Set the store state cache to be used by the record store. If {@code null} is provided or if this method
         * is never called, the produced record store will use the default store state cache provided by the
         * {@link FDBDatabase}.
         *
         * @param storeStateCache the store state cache to used by this record store or {@code null} to specify that this should use the database default
         * @return this builder
         */
        @API(API.Status.EXPERIMENTAL)
        @Nonnull
        BaseBuilder<M, R> setStoreStateCache(@Nonnull FDBRecordStoreStateCache storeStateCache);

        /**
         * Make a copy of this builder.
         * This can be used to share enough of the state to connect to the same record store several times in different transactions.
         * <pre>
         *     builder = FDBRecordStore.newBuilder().setMetaDataProvider(metadata).setSubspace(subspace)
         *     store1 = builder.copyBuilder().setContext(context1).build()
         *     store2 = builder.copyBuilder().setContext(context2).build()
         * </pre>
         * @return a new builder with the same state as this builder
         */
        @Nonnull
        BaseBuilder<M, R> copyBuilder();

        /**
         * Build the record store.
         * @return a new record store with the desired state.
         */
        @Nonnull
        R build();

        /**
         * Synchronous version of {@link #uncheckedOpenAsync}.
         * @return a store with the appropriate parameters set
         */
        @Nonnull
        default R uncheckedOpen() {
            return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_STORE_STATE, uncheckedOpenAsync());
        }

        /**
         * Synchronous version of {@link #createAsync}.
         * @return a store with the appropriate parameters set
         */
        @Nonnull
        default R create() {
            return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createAsync());
        }

        /**
         * Synchronous version of {@link #openAsync}.
         * @return a store with the appropriate parameters set
         */
        @Nonnull
        default R open() {
            return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, openAsync());
        }

        /**
         * Synchronous version of {@link #createOrOpenAsync}.
         * @return a store with the appropriate parameters set
         */
        @Nonnull
        default R createOrOpen() {
            return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createOrOpenAsync());
        }

        /**
         * Synchronous version of {@link #createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck)}.
         * @param existenceCheck whether the store must already exist
         * @return an open record store
         */
        @Nonnull
        default R createOrOpen(@Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
            return getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createOrOpenAsync(existenceCheck));
        }

        /**
         * Opens a <code>FDBRecordStore</code> instance without calling {@link FDBRecordStore#checkVersion}.
         * @return a future that will contain a store with the appropriate parameters set when ready
         */
        @Nonnull
        CompletableFuture<R> uncheckedOpenAsync();

        /**
         * Opens a new <code>FDBRecordStore</code> instance in the given path with the given meta-data.
         * The store must not have already been written to the specified subspace.
         * @return a future that will contain a store with the appropriate parameters set when ready
         */
        @Nonnull
        default CompletableFuture<R> createAsync() {
            return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS);
        }

        /**
         * Opens an existing <code>FDBRecordStore</code> instance in the given path with the given meta-data.
         * The store must have already been written to the specified subspace.
         * @return a future that will contain a store with the appropriate parameters set when ready
         */
        @Nonnull
        default CompletableFuture<R> openAsync() {
            return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS);
        }

        /**
         * Opens a <code>FDBRecordStore</code> instance in the given path with the given meta-data.
         * @return a future that will contain a store with the appropriate parameters set when ready
         */
        @Nonnull
        default CompletableFuture<R> createOrOpenAsync() {
            return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);
        }

        /**
         * Opens a <code>FDBRecordStore</code> instance in the given path with the given meta-data.
         * @param existenceCheck whether the store must already exist
         * @return a future that will contain a store with the appropriate parameters set when ready
         */
        @Nonnull
        CompletableFuture<R> createOrOpenAsync(@Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck);

    }

}
