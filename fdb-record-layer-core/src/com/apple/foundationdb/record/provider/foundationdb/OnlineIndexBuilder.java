/*
 * OnlineIndexBuilder.java
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

import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Builds an index online, i.e., concurrently with other database operations. In order to minimize
 * the impact that these operations have with other operations, this attempts to minimize the
 * priorities of its transactions. Additionally, it attempts to limit the amount of work it will
 * done in a fashion that will decrease as the number of failures for a given build attempt increases.
 *
 * <p>
 * As ranges of elements are rebuilt, the fact that the range has rebuilt is added to a {@link com.apple.foundationdb.async.RangeSet}
 * associated with the index being built. This {@link com.apple.foundationdb.async.RangeSet} is used to (a) coordinate work between
 * different builders that might be running on different machines to ensure that the same work isn't
 * duplicated and to (b) make sure that non-idempotent indexes (like <code>COUNT</code> or <code>SUM_LONG</code>)
 * don't update themselves (or fail to update themselves) incorrectly.
 * </p>
 *
 * <p>
 * Unlike many other features in the Record Layer core, this has a retry loop.
 * </p>
 */
public class OnlineIndexBuilder extends OnlineIndexBuilderBase<Message> {
    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within a record store built from the given store builder.
     * This constructor also lets the user set a few parameters that affect rate-limiting and error handling when the index is built.
     * Setting these parameters to {@link OnlineIndexBuilderBase#UNLIMITED OnlineIndexBuilder.UNLIMITED} will cause these limits to be ignored.
     *
     * @param fdb database that contains the record store
     * @param recordStoreBuilder builder to use to open the record store
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index
     * @param limit maximum number of records to process in one transaction
     * @param maxRetries maximum number of times to retry a single range rebuild
     * @param recordsPerSecond maximum number of records to process in a single second
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                              @Nonnull Index index, @Nullable Collection<RecordType> recordTypes,
                              int limit, int maxRetries, int recordsPerSecond) {
        super(fdb, recordStoreBuilder, index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within a record store built from the given store builder.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param fdb database that contains the record store
     * @param recordStoreBuilder builder to use to open the record store
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                              @Nonnull Index index, @Nullable Collection<RecordType> recordTypes) {
        this(fdb, recordStoreBuilder, index, recordTypes, DEFAULT_LIMIT, DEFAULT_MAX_RETRIES, DEFAULT_RECORDS_PER_SECOND);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within a record store built from the given store builder.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param fdb database that contains the record store
     * @param recordStoreBuilder builder to use to open the record store
     * @param index the index to build
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder recordStoreBuilder, @Nonnull Index index) {
        this(fdb, recordStoreBuilder, index, null);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given record store.
     * This constructor also lets the user set a few parameters that affect rate-limiting and error handling when the index is built.
     * Setting these parameters to {@link OnlineIndexBuilderBase#UNLIMITED OnlineIndexBuilder.UNLIMITED} will cause these limits to be ignored.
     *
     * @param recordStore the record store to use as a prototype
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index
     * @param limit maximum number of records to process in one transaction
     * @param maxRetries maximum number of times to retry a single range rebuild
     * @param recordsPerSecond maximum number of records to process in a single second
     */
    public OnlineIndexBuilder(@Nonnull FDBRecordStore recordStore,
                              @Nonnull Index index, @Nullable Collection<RecordType> recordTypes,
                              int limit, int maxRetries, int recordsPerSecond) {
        super(recordStore, index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given record store.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param recordStore the record store to use as a prototype
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index
     */
    public OnlineIndexBuilder(@Nonnull FDBRecordStore recordStore,
                              @Nonnull Index index, @Nullable Collection<RecordType> recordTypes) {
        this(recordStore, index, recordTypes, DEFAULT_LIMIT, DEFAULT_MAX_RETRIES, DEFAULT_RECORDS_PER_SECOND);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given record store.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param recordStore the record store to use as a prototype
     * @param index the index to build
     */
    public OnlineIndexBuilder(@Nonnull FDBRecordStore recordStore, @Nonnull Index index) {
        this(recordStore, index, null);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index of the given name within the given record store.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param recordStore the record store to use as a prototype
     * @param indexName the name of the index to build
     */
    public OnlineIndexBuilder(@Nonnull FDBRecordStore recordStore, @Nonnull String indexName) {
        this(recordStore, recordStore.getRecordMetaData().getIndex(indexName));
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given meta-data that is
     * used by a record store within the given subspace.
     * This constructor also lets the user set a few parameters that affect rate-limiting and error handling when the index is built.
     * Setting these parameters to {@link OnlineIndexBuilderBase#UNLIMITED OnlineIndexBuilder.UNLIMITED} will cause these limits to be ignored.
     *
     * @param fdb database that contains the record store
     * @param metaData the meta-data of the store containing the index being built
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index or {@code null} for all to which it applies
     * @param subspaceProvider the subspace provider showing where the record store is located
     * @param limit maximum number of records to process in one transaction
     * @param maxRetries maximum number of times to retry a single range rebuild
     * @param recordsPerSecond maximum number of records to process in a single second
     */
    @SuppressWarnings("squid:S00107")   // Many parameters, but there are more convenient ones below.
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb,
                              @Nonnull RecordMetaDataProvider metaData, @Nonnull Index index, @Nullable Collection<RecordType> recordTypes,
                              @Nonnull SubspaceProvider subspaceProvider,
                              int limit, int maxRetries, int recordsPerSecond) {
        this(fdb, FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setSubspaceProvider(subspaceProvider),
                index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given meta-data that is
     * used by a record store within the given subspace.
     * This constructor also lets the user set a few parameters that affect rate-limiting and error handling when the index is built.
     * Setting these parameters to {@link OnlineIndexBuilderBase#UNLIMITED OnlineIndexBuilder.UNLIMITED} will cause these limits to be ignored.
     *
     * @param fdb database that contains the record store
     * @param metaData the meta-data of the store containing the index being built
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index or {@code null} for all to which it applies
     * @param subspace subspace in which the record store is location
     * @param limit maximum number of records to process in one transaction
     * @param maxRetries maximum number of times to retry a single range rebuild
     * @param recordsPerSecond maximum number of records to process in a single second
     */
    @SuppressWarnings("squid:S00107")   // Many parameters, but there are more convenient ones below.
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb,
                              @Nonnull RecordMetaDataProvider metaData, @Nonnull Index index, @Nullable Collection<RecordType> recordTypes,
                              @Nonnull Subspace subspace,
                              int limit, int maxRetries, int recordsPerSecond) {
        this(fdb, FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setSubspace(subspace),
                index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given meta-data that is
     * used by a record store within the given subspace.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param fdb database that contains the record store
     * @param metaData the meta-data of the store containing the index being built
     * @param index the index to build
     * @param recordTypes the record types for which to rebuild the index or {@code null} for all to which it applies
     * @param subspace subspace in which the record store is location
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb,
                              @Nonnull RecordMetaDataProvider metaData, @Nonnull Index index, @Nullable Collection<RecordType> recordTypes,
                              @Nonnull Subspace subspace) {
        this(fdb, metaData, index, recordTypes, subspace, DEFAULT_LIMIT, DEFAULT_MAX_RETRIES, DEFAULT_RECORDS_PER_SECOND);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index within the given meta-data that is
     * used by a record store within the given subspace.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param fdb database that contains the record store
     * @param metaData the meta-data of the store containing the index being built
     * @param index the index to build
     * @param subspace subspace in which the record store is location
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb,
                              @Nonnull RecordMetaDataProvider metaData, @Nonnull Index index,
                              @Nonnull Subspace subspace) {
        this(fdb, metaData, index, null, subspace);
    }

    /**
     * Creates an <code>OnlineIndexBuilder</code> to construct an index of the given name within the given meta-data that is
     * used by a record store within the given subspace.
     * Default values are used for the parameters to tune rate-limiting and error handling within the index builder.
     * These values are:
     * <ul>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_LIMIT DEFAULT_LIMIT}: {@value DEFAULT_LIMIT}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_MAX_RETRIES DEFAULT_MAX_RETRIES}: {@value DEFAULT_MAX_RETRIES}</li>
     *     <li>{@link OnlineIndexBuilderBase#DEFAULT_RECORDS_PER_SECOND DEFAULT_RECORDS_PER_SECOND}: {@value DEFAULT_RECORDS_PER_SECOND}</li>
     * </ul>
     *
     * @param fdb database that contains the record store
     * @param metaData the meta-data of the store containing the index being built
     * @param indexName the name of the index to build
     * @param subspace subspace in which the record store is location
     */
    public OnlineIndexBuilder(@Nonnull FDBDatabase fdb,
                              @Nonnull RecordMetaDataProvider metaData, @Nonnull String indexName,
                              @Nonnull Subspace subspace) {
        this(fdb, metaData, metaData.getRecordMetaData().getIndex(indexName), subspace);
    }

}
