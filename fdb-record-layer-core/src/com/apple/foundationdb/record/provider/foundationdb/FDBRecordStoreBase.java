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

import com.apple.foundationdb.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.MutableRecordStoreState;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeOrBuilder;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.LoggableException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Base class for typed and untyped record stores.
 * @param <M> type used to represent stored records
 * @see FDBRecordStore
 * @see FDBTypedRecordStore
 */
@API(API.Status.MAINTAINED)
public abstract class FDBRecordStoreBase<M extends Message> extends FDBStoreBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreBase.class);

    private static final int MIN_FORMAT_VERSION = 1;
    // 1 - initial implementation
    public static final int INFO_ADDED_FORMAT_VERSION = 1;
    // 2 - added record counting
    public static final int RECORD_COUNT_ADDED_FORMAT_VERSION = 2;
    // 3 - added support for a key in record count
    public static final int RECORD_COUNT_KEY_ADDED_FORMAT_VERSION = 3;
    // 4 - tightened up format version migration (version mostly for testing)
    public static final int FORMAT_CONTROL_FORMAT_VERSION = 4;
    // 5 - started writing unsplit records with a suffix
    public static final int SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION = 5;
    // 6 - store record version at a split point within the record
    public static final int SAVE_VERSION_WITH_RECORD_FORMAT_VERSION = 6;

    // The current code can read and write up to the format version below
    public static final int MAX_SUPPORTED_FORMAT_VERSION = SAVE_VERSION_WITH_RECORD_FORMAT_VERSION;

    // Record stores attempt to upgrade to this version
    public static final int DEFAULT_FORMAT_VERSION = MAX_SUPPORTED_FORMAT_VERSION;

    // These agree with the client's values. They could be tunable and even increased with knobs.
    public static final int KEY_SIZE_LIMIT = 10_000;
    public static final int VALUE_SIZE_LIMIT = 100_000;

    // The maximum number of records to allow before triggering online index builds
    // instead of a transactional rebuild.
    public static final int MAX_RECORDS_FOR_REBUILD = 200;

    // The size of preload cache
    private static final int PRELOAD_CACHE_SIZE = 100;

    /**
     * Provided during record save (via {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)}),
     * directs the behavior of the save w.r.t. the record's version.
     * In the presence of a version, either <code>DEFAULT</code> or <code>WITH_VERSION</code> can be used.
     * For safety, <code>NO_VERSION</code> should only be used with a null version.
     */
    public enum VersionstampSaveBehavior {
        DEFAULT,        // Follow rules dictated by the metadata
        NO_VERSION,     // Explicitly do NOT save a version
        WITH_VERSION,   // Explicitly save a version even if meta-data says not to
    }

    protected int formatVersion;
    protected int userVersion;

    private boolean omitUnsplitRecordSuffix;

    @Nonnull
    protected final RecordMetaDataProvider metaDataProvider;

    @Nullable
    protected MutableRecordStoreState recordStoreState;

    @Nonnull
    protected final RecordSerializer<M> serializer;

    @Nonnull
    protected final IndexMaintainerRegistry indexMaintainerRegistry;

    @Nonnull
    protected final IndexMaintenanceFilter indexMaintenanceFilter;

    @Nonnull
    protected final PipelineSizer pipelineSizer;

    @Nullable
    private Subspace cachedRecordsSubspace;

    private final Cache<Tuple, FDBRawRecord> preloadCache;

    @SuppressWarnings("squid:S00107")
    protected FDBRecordStoreBase(@Nonnull FDBRecordContext context,
                                 @Nonnull SubspaceProvider subspaceProvider,
                                 int formatVersion,
                                 @Nonnull RecordMetaDataProvider metaDataProvider,
                                 @Nonnull RecordSerializer<M> serializer,
                                 @Nonnull IndexMaintainerRegistry indexMaintainerRegistry,
                                 @Nonnull IndexMaintenanceFilter indexMaintenanceFilter,
                                 @Nonnull PipelineSizer pipelineSizer) {
        super(context, subspaceProvider);
        this.formatVersion = formatVersion;
        this.metaDataProvider = metaDataProvider;
        this.serializer = serializer;
        this.indexMaintainerRegistry = indexMaintainerRegistry;
        this.indexMaintenanceFilter = indexMaintenanceFilter;
        this.pipelineSizer = pipelineSizer;
        this.omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
        this.preloadCache = CacheBuilder.<Tuple,FDBRawRecord>newBuilder().maximumSize(PRELOAD_CACHE_SIZE).build();
    }

    /**
     * Return a {@link FDBRecordStoreBuilder} with the same state as this record store.
     *
     * For example, to connect to the same record store from another transaction:
     * <code>store.asBuilder().setContext(newContext).build()</code>
     * @return a builder initialized with the same state as this store
     */
    public abstract FDBRecordStoreBuilder<M, ? extends FDBRecordStoreBase<M>> asBuilder();

    /**
     * Get the {@link RecordMetaData} used by this store.
     * @return the associated meta-data
     */
    @Nonnull
    public RecordMetaData getRecordMetaData() {
        return metaDataProvider.getRecordMetaData();
    }

    /**
     * Get the {@link RecordStoreState} for this store.
     * This represents the indexes that are disabled or in the process of being rebuilt.
     * If the state is not already loaded, it is loaded synchronously.
     * @return the store state for this store
     */
    @Nonnull
    public RecordStoreState getRecordStoreState() {
        if (recordStoreState == null) {
            recordStoreState = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_STORE_STATE, loadRecordStoreStateAsync(context, getSubspace()));
        }
        return recordStoreState;
    }

    /**
     * Get the storage format version currently in use for this record store.
     *
     * After calling {@link FDBRecordStoreBuilder#open} or {@link #checkVersion} directly, this will be the format stored in the store's info header.
     *
     * Index maintainers can use this to determine what format to expect / produce.
     * @return the storage format version
     */
    public int getFormatVersion() {
        return formatVersion;
    }

    /**
     * Get the user version currently in use for this record store.
     *
     * After calling {@link FDBRecordStoreBuilder#open} or {@link #checkVersion} directly, this will be the value stored in the store's info header.
     *
     * This version is returned from {@link UserVersionChecker#checkUserVersion} and does not have any meaning within the Record Layer core.
     * @return the user format version
     */
    public int getUserVersion() {
        return userVersion;
    }

    private boolean useOldVersionFormat() {
        // If the store is either explicitly using the older format version or if
        // it is using a newer one, but because of how the data were originally stored
        // in this record store, then use the older location for record versions.
        return getFormatVersion() < SAVE_VERSION_WITH_RECORD_FORMAT_VERSION || omitUnsplitRecordSuffix;
    }

    protected static final Object STORE_INFO_KEY = FDBRecordStoreKeyspace.STORE_INFO.key();
    protected static final Object RECORD_KEY = FDBRecordStoreKeyspace.RECORD.key();
    protected static final Object INDEX_KEY = FDBRecordStoreKeyspace.INDEX.key();
    protected static final Object INDEX_SECONDARY_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE.key();
    protected static final Object RECORD_COUNT_KEY = FDBRecordStoreKeyspace.RECORD_COUNT.key();
    protected static final Object INDEX_STATE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_STATE_SPACE.key();
    protected static final Object INDEX_RANGE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_RANGE_SPACE.key();
    protected static final Object INDEX_UNIQUENESS_VIOLATIONS_KEY = FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE.key();
    protected static final Object RECORD_VERSION_KEY = FDBRecordStoreKeyspace.RECORD_VERSION_SPACE.key();

    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_ONE = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_MINUS_ONE = new byte[] { -1, -1, -1, -1, -1, -1, -1, -1 };

    /**
     * Action to take if the record being saved does / does not already exist.
     * @see FDBRecordStoreBase#saveRecordAsync(Message, RecordExistenceCheck)
     */
    public enum RecordExistenceCheck {
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
     * Async version of {@link #saveRecord(Message)}.
     * @param record the record to save
     * @return a future that completes with the stored record form of the saved record
     */
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M record) {
        return saveRecordAsync(record, (FDBRecordVersion)null);
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck)}.
     * @param record the record to save
     * @param existenceCheck whether the record must already exist
     * @return a future that completes with the stored record form of the saved record
     */
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M record, @Nonnull RecordExistenceCheck existenceCheck) {
        return saveRecordAsync(record, existenceCheck, null, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Async version of {@link #saveRecord(Message, FDBRecordVersion)}.
     * @param record the record to save
     * @param version the associated record version
     * @return a future that completes with the stored record form of the saved record
     */
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M record, @Nullable FDBRecordVersion version) {
        return saveRecordAsync(record, version, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Async version of {@link #saveRecord(Message, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param record the record to save
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M record, @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveRecordAsync(record, RecordExistenceCheck.NONE, version, behavior);
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param record the record to save
     * @param existenceCheck whether the record must already exist
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final M record, @Nonnull RecordExistenceCheck existenceCheck,
                                                                 @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Descriptors.Descriptor recordDescriptor = record.getDescriptorForType();
        final RecordType recordType = metaData.getRecordTypeForDescriptor(recordDescriptor);
        final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();

        final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(record).setRecordType(recordType);
        final FDBRecordVersion recordVersion = recordVersionForSave(metaData, version, behavior);
        recordBuilder.setVersion(recordVersion);
        final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(emptyEvaluationContext(), recordBuilder).toTuple();
        recordBuilder.setPrimaryKey(primaryKey);

        CompletableFuture<FDBStoredRecord<M>> result = loadRecordAsync(primaryKey).thenCompose(oldRecord -> {
            final FDBStoredRecord<M> newRecord = saveSerializedRecord(metaData, recordBuilder, oldRecord);
            if (oldRecord == null) {
                if (existenceCheck.errorIfNotExists()) {
                    throw new RecordDoesNotExistException("record does not exist",
                            LogMessageKeys.PRIMARY_KEY, primaryKey);
                }
                addRecordCount(metaData, newRecord, LITTLE_ENDIAN_INT64_ONE);
            } else {
                if (existenceCheck.errorIfExists()) {
                    throw new RecordAlreadyExistsException("record already exists",
                            LogMessageKeys.PRIMARY_KEY, primaryKey);
                }
                if (existenceCheck.errorIfTypeChanged() && oldRecord.getRecordType() != recordType) {
                    throw new RecordTypeChangedException("record type changed",
                            LogMessageKeys.PRIMARY_KEY, primaryKey,
                            LogMessageKeys.ACTUAL_TYPE, oldRecord.getRecordType().getName(),
                            LogMessageKeys.EXPECTED_TYPE, recordType.getName());
                }
                if (getTimer() != null) {
                    getTimer().increment(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES, oldRecord.getValueSize());
                }
            }
            return updateSecondaryIndexes(oldRecord, newRecord).thenApply(v -> newRecord);
        });
        return context.instrument(FDBStoreTimer.Events.SAVE_RECORD, result);
    }

    @Nullable
    protected FDBRecordVersion recordVersionForSave(@Nonnull RecordMetaData metaData, @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        if (behavior.equals(VersionstampSaveBehavior.NO_VERSION)) {
            if (version != null) {
                throw new RecordCoreException("Nonnull version supplied with a NO_VERSION behavior: " + version);
            }
            return null;
        }
        if (version == null && (behavior.equals(VersionstampSaveBehavior.WITH_VERSION) || metaData.isStoreRecordVersions())) {
            return FDBRecordVersion.incomplete(context.claimLocalVersion());
        }
        return version;
    }

    protected FDBStoredRecord<M> saveSerializedRecord(@Nonnull final RecordMetaData metaData, @Nonnull final FDBStoredRecordBuilder<M> recordBuilder, @Nullable FDBStoredSizes oldSizeInfo) {
        final Tuple primaryKey = recordBuilder.getPrimaryKey();
        final FDBRecordVersion version = recordBuilder.getVersion();
        final byte[] serialized = serializer.serialize(metaData, recordBuilder.getRecordType(), recordBuilder.getRecord(), getTimer());
        final FDBRecordVersion splitVersion = useOldVersionFormat() ? null : version;
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        SplitHelper.saveWithSplit(context, recordsSubspace(), recordBuilder.getPrimaryKey(), serialized, splitVersion, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix, true, oldSizeInfo, sizeInfo);
        countKeysAndValues(FDBStoreTimer.Counts.SAVE_RECORD_KEY, FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES, FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES, sizeInfo);
        recordBuilder.setSize(sizeInfo);

        if (version != null && useOldVersionFormat()) {
            saveVersionWithOldFormat(primaryKey, version);
        }
        return recordBuilder.build();
    }

    private void saveVersionWithOldFormat(@Nonnull Tuple primaryKey, @Nonnull FDBRecordVersion version) {
        byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
        if (version.isComplete()) {
            context.ensureActive().set(versionKey, version.toBytes());
        } else {
            context.addToLocalVersionCache(primaryKey, version.getLocalVersion());
            final byte[] valueBytes = version.writeTo(ByteBuffer.allocate(FDBRecordVersion.VERSION_LENGTH + Integer.BYTES).order(ByteOrder.BIG_ENDIAN))
                    .putInt(0)
                    .array();
            context.addVersionMutation(MutationType.SET_VERSIONSTAMPED_VALUE, versionKey, valueBytes);
        }
    }

    protected CompletableFuture<Void> updateSecondaryIndexes(@Nullable final FDBStoredRecord<M> oldRecord,
                                                             @Nullable final FDBStoredRecord<M> newRecord) {
        if (oldRecord == null && newRecord == null) {
            return AsyncUtil.DONE;
        }
        if (recordStoreState == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> updateSecondaryIndexes(oldRecord, newRecord));
        }

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        final RecordType sameRecordType;
        if (oldRecord == null) {
            sameRecordType = newRecord.getRecordType();
        } else if (newRecord == null) {
            sameRecordType = oldRecord.getRecordType();
        } else if (oldRecord.getRecordType() == newRecord.getRecordType()) {
            sameRecordType = newRecord.getRecordType();
        } else {
            sameRecordType = null;
        }
        recordStoreState.beginRead();
        boolean haveFuture = false;
        try {
            if (sameRecordType != null) {
                updateSecondaryIndexes(oldRecord, newRecord, futures, getEnabledIndexes(sameRecordType));
                updateSecondaryIndexes(oldRecord, newRecord, futures, getEnabledUniversalIndexes());
                updateSecondaryIndexes(oldRecord, newRecord, futures, getEnabledMultiTypeIndexes(sameRecordType));
            } else {
                final List<Index> oldIndexes = new ArrayList<>();
                if (oldRecord != null) {
                    final RecordType oldRecordType = oldRecord.getRecordType();
                    oldIndexes.addAll(getEnabledIndexes(oldRecordType));
                    oldIndexes.addAll(getEnabledUniversalIndexes());
                    oldIndexes.addAll(getEnabledMultiTypeIndexes(oldRecordType));
                }
                List<Index> newIndexes = new ArrayList<>();
                if (newRecord != null) {
                    final RecordType newRecordType = newRecord.getRecordType();
                    newIndexes.addAll(getEnabledIndexes(newRecordType));
                    newIndexes.addAll(getEnabledUniversalIndexes());
                    newIndexes.addAll(getEnabledMultiTypeIndexes(newRecordType));
                }
                List<Index> commonIndexes = new ArrayList<>(oldIndexes);
                commonIndexes.retainAll(newIndexes);
                oldIndexes.removeAll(commonIndexes);
                newIndexes.removeAll(commonIndexes);
                updateSecondaryIndexes(oldRecord, null, futures, oldIndexes);
                updateSecondaryIndexes(null, newRecord, futures, newIndexes);
                updateSecondaryIndexes(oldRecord, newRecord, futures, commonIndexes);
            }
            haveFuture = true;
        } finally {
            if (!haveFuture) {
                recordStoreState.endRead();
            }
        }
        if (futures.isEmpty()) {
            recordStoreState.endRead();
            return AsyncUtil.DONE;
        } else if (futures.size() == 1) {
            return futures.get(0).whenComplete((v, t) -> recordStoreState.endRead());
        } else {
            return AsyncUtil.whenAll(futures).whenComplete((v, t) -> recordStoreState.endRead());
        }
    }

    protected void updateSecondaryIndexes(@Nullable final FDBStoredRecord<M> oldRecord,
                                          @Nullable final FDBStoredRecord<M> newRecord,
                                          @Nonnull final List<CompletableFuture<Void>> futures,
                                          @Nonnull final List<Index> indexes) {
        if (oldRecord == null && newRecord == null) {
            return;
        }
        for (Index index : indexes) {
            final IndexMaintainer<M> maintainer = getIndexMaintainer(index);
            final CompletableFuture<Void> future;
            if (!maintainer.isIdempotent() && isIndexWriteOnly(index)) {
                // In this case, the index is still being built, so we are not
                // going to update the record unless the rebuild job has already
                // gotten to this range.
                final Tuple primaryKey = newRecord == null ? oldRecord.getPrimaryKey() : newRecord.getPrimaryKey();
                future = maintainer.addedRangeWithKey(primaryKey)
                        .thenCompose(present -> {
                            if (present) {
                                return maintainer.update(oldRecord, newRecord);
                            } else {
                                return AsyncUtil.DONE;
                            }
                        });
                if (!MoreAsyncUtil.isCompletedNormally(future)) {
                    futures.add(future);
                }
            } else {
                future = maintainer.update(oldRecord, newRecord);
            }
            if (!MoreAsyncUtil.isCompletedNormally(future)) {
                futures.add(future);
            }
        }
    }

    @Nonnull
    public Subspace recordsSubspace() {
        if (cachedRecordsSubspace == null) {
            cachedRecordsSubspace = getSubspace().subspace(Tuple.from(RECORD_KEY));
        }
        return cachedRecordsSubspace;
    }

    @Nonnull
    public Subspace indexSubspace(@Nonnull Index index) {
        return getSubspace().subspace(Tuple.from(INDEX_KEY, index.getSubspaceKey()));
    }

    @Nonnull
    public Subspace indexSubspaceFromMaintainer(@Nonnull Index index) {
        return getIndexMaintainer(index).getIndexSubspace();
    }

    @Nonnull
    public Subspace indexStateSubspace() {
        return getSubspace().subspace(Tuple.from(INDEX_STATE_SPACE_KEY));
    }

    @Nonnull
    public Subspace indexSecondarySubspace(@Nonnull Index index) {
        return getSubspace().subspace(Tuple.from(INDEX_SECONDARY_SPACE_KEY, index.getSubspaceKey()));
    }

    /**
     * Subspace for index in which to place a {@link com.apple.foundationdb.async.RangeSet RangeSet}.
     * This is used for determining how much progress has been made on building the index in the
     * case that one is building the index offline.
     * @param index the index to retrieve the range subspace for
     * @return the subspace for the {@link com.apple.foundationdb.async.RangeSet RangeSet} for the given index
     */
    @Nonnull
    public Subspace indexRangeSubspace(@Nonnull Index index) {
        return getSubspace().subspace(Tuple.from(INDEX_RANGE_SPACE_KEY, index.getSubspaceKey()));
    }

    /**
     * Subspace for index in which to place a record in uniqueness violations. This
     * is used while the index is being built to keep track of what values are duplicated and
     * thus have to be addressed later.
     * @param index the index to retrieve the uniqueness violation subspace for
     * @return the subspace for the uniqueness violations for the given index
     */
    @Nonnull
    public Subspace indexUniquenessViolationsSubspace(@Nonnull Index index) {
        return getSubspace().subspace(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY, index.getSubspaceKey()));
    }

    /**
     * Get the maintainer for a given index.
     * @param index the required index
     * @return the maintainer for the given index
     */
    public IndexMaintainer<M> getIndexMaintainer(@Nonnull Index index) {
        return indexMaintainerRegistry.getIndexMaintainer(new IndexMaintainerState<>(this, index, indexMaintenanceFilter));
    }

    public int getKeySizeLimit() {
        return KEY_SIZE_LIMIT;
    }

    public int getValueSizeLimit() {
        return VALUE_SIZE_LIMIT;
    }

    public void addUniquenessCheck(@Nonnull AsyncIterable<KeyValue> kvs,
                                   @Nonnull Index index,
                                   @Nonnull IndexEntry indexEntry,
                                   @Nonnull Tuple primaryKey) {
        getRecordContext().addCommitCheck(new IndexUniquenessCheck(kvs, index, indexEntry, primaryKey));
    }

    class IndexUniquenessCheck implements FDBRecordContext.CommitCheck {
        @Nonnull
        private final AsyncIterator<KeyValue> iter;
        @Nonnull
        private final Index index;
        @Nonnull
        private final IndexEntry indexEntry;
        @Nonnull
        private final Tuple primaryKey;
        @Nonnull
        private CompletableFuture<Boolean> onHasNext;
        @Nonnull
        private IndexMaintainer<M> indexMaintainer;

        public IndexUniquenessCheck(@Nonnull AsyncIterable<KeyValue> iter,
                                    @Nonnull Index index,
                                    @Nonnull IndexEntry indexEntry,
                                    @Nonnull Tuple primaryKey) {
            this.iter = iter.iterator();
            this.index = index;
            this.indexEntry = indexEntry;
            this.primaryKey = primaryKey;
            this.indexMaintainer = getIndexMaintainer(index);

            onHasNext = this.iter.onHasNext();
            onHasNext = context.instrument(FDBStoreTimer.Events.CHECK_INDEX_UNIQUENESS, onHasNext);
        }

        @Override
        public boolean isReady() {
            return onHasNext.isDone();
        }

        @Override
        public void check() {
            Tuple valueKey = null;
            while (iter.hasNext()) {
                Tuple existingEntry = SplitHelper.unpackKey(indexMaintainer.getIndexSubspace(), iter.next());
                Tuple existingKey = indexEntryPrimaryKey(index, existingEntry);
                if (!primaryKey.equals(existingKey)) {
                    if (isIndexWriteOnly(index)) {
                        if (valueKey == null) {
                            valueKey = indexEntry.getKey();
                        }
                        indexMaintainer.updateUniquenessViolations(valueKey, primaryKey, existingKey, false);
                        indexMaintainer.updateUniquenessViolations(valueKey, existingKey, primaryKey, false);
                    } else {
                        throw new RecordIndexUniquenessViolation(index, indexEntry, primaryKey, existingKey);
                    }
                }
            }
        }
    }

    public CompletableFuture<IndexOperationResult> performIndexOperationAsync(@Nonnull String indexName,
                                                                              @Nonnull IndexOperation operation) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        return getIndexMaintainer(index).performOperation(operation);
    }

    public IndexOperationResult performIndexOperation(@Nonnull String indexName, @Nonnull IndexOperation operation) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_OPERATION, performIndexOperationAsync(indexName, operation));
    }

    /**
     * Save the given record.
     * @param record the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> saveRecord(@Nonnull final M record) {
        return saveRecord(record, (FDBRecordVersion)null);
    }

    /**
     * Save the given record.
     * @param record the record to be saved
     * @param existenceCheck whether the record must already exist
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> saveRecord(@Nonnull final M record, @Nonnull RecordExistenceCheck existenceCheck) {
        return saveRecord(record, existenceCheck, null, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Save the given record with a specific version. If <code>null</code>
     * is passed for <code>version</code>, then a new version is
     * created that will be unique for this record.
     * @param record the record to be saved
     * @param version the version to associate with the record when saving
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> saveRecord(@Nonnull final M record, @Nullable final FDBRecordVersion version) {
        return saveRecord(record, version, VersionstampSaveBehavior.DEFAULT);
    }

    /**
     * Save the given record with a specific version.
     * The version is handled according to the behavior value. If behavior is <code>DEFAULT</code> then
     * the method acts as {@link #saveRecord(Message, FDBRecordVersion)}. If behavior is <code>NO_VERSION</code> then
     * <code>version</code> is ignored and no version is saved. If behavior is <code>WITH_VERSION</code> then the value
     * of <code>version</code>  is stored as given by the caller.
     * @param record the record to be saved
     * @param version the version to associate with the record when saving
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> saveRecord(@Nonnull final M record, @Nullable final FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveRecord(record, RecordExistenceCheck.NONE, version, behavior);
    }

    /**
     * Save the given record with a specific version.
     * The version is handled according to the behavior value. If behavior is <code>DEFAULT</code> then
     * the method acts as {@link #saveRecord(Message, FDBRecordVersion)}. If behavior is <code>NO_VERSION</code> then
     * <code>version</code> is ignored and no version is saved. If behavior is <code>WITH_VERSION</code> then the value
     * of <code>version</code>  is stored as given by the caller.
     * @param record the record to be saved
     * @param existenceCheck whether the record must already exist
     * @param version the version to associate with the record when saving
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> saveRecord(@Nonnull final M record, @Nonnull RecordExistenceCheck existenceCheck,
                                         @Nullable final FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD, saveRecordAsync(record, existenceCheck, version, behavior));
    }

    /**
     * Save the given record and throw an exception if a record already exists with the same primary key.
     * @param record the record to be saved
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    public CompletableFuture<FDBStoredRecord<M>> insertRecordAsync(@Nonnull final M record) {
        return saveRecordAsync(record, RecordExistenceCheck.ERROR_IF_EXISTS);
    }

    /**
     * Save the given record and throw an exception if a record already exists with the same primary key.
     * @param record the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> insertRecord(@Nonnull final M record) {
        return saveRecord(record, RecordExistenceCheck.ERROR_IF_EXISTS);
    }

    /**
     * Save the given record and throw an exception if the record does not already exist in the database.
     * @param record the record to be saved
     * @return a future that completes with the stored record form of the saved record
     */
    @Nonnull
    public CompletableFuture<FDBStoredRecord<M>> updateRecordAsync(@Nonnull final M record) {
        return saveRecordAsync(record, RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
    }

    /**
     * Save the given record and throw an exception if the record does not already exist in the database.
     * @param record the record to be saved
     * @return wrapping object containing saved record and metadata
     */
    @Nonnull
    public FDBStoredRecord<M> updateRecord(@Nonnull final M record) {
        return saveRecord(record, RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
    }

    /**
     * Load the record with the given primary key.
     * @param primaryKey the primary key for the record
     * @return a {@link FDBStoredRecord} for the record or <code>null</code>.
     */
    @Nullable
    public FDBStoredRecord<M> loadRecord(@Nonnull final Tuple primaryKey) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, loadRecordAsync(primaryKey));
    }

    @Nullable
    public FDBStoredRecord<M> loadRecord(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, loadRecordAsync(primaryKey, snapshot));
    }

    /**
     * Asynchronously load a record.
     * @param primaryKey the key for the record to be loaded
     * @return a CompletableFuture that will return a message or null if there was no record with that key
     */
    @Nonnull
    public CompletableFuture<FDBStoredRecord<M>> loadRecordAsync(@Nonnull final Tuple primaryKey) {
        return loadRecordAsync(primaryKey, false);
    }

    @Nonnull
    public CompletableFuture<FDBStoredRecord<M>> loadRecordAsync(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        return loadRecordInternal(primaryKey, snapshot);
    }

    protected CompletableFuture<FDBStoredRecord<M>> loadRecordInternal(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();

        final Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional;
        if (useOldVersionFormat()) {
            versionFutureOptional = loadRecordVersionAsync(primaryKey);
        } else {
            versionFutureOptional = Optional.empty();
        }

        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        final long startTime = System.nanoTime();
        CompletableFuture<FDBStoredRecord<M>> result = loadRawRecordAsync(primaryKey, sizeInfo, snapshot)
                .thenCompose(rawRecord -> {
                    final long startTimeToDeserialize = System.nanoTime();
                    final long timeToLoad = startTimeToDeserialize - startTime;
                    return rawRecord == null ? CompletableFuture.completedFuture(null) :
                            deserializeRecord(metaData, rawRecord, versionFutureOptional)
                                    .thenApply(storedRecord -> storedRecord.setTimeToLoad(timeToLoad).setTimeToDeserialize(System.nanoTime() - startTimeToDeserialize).build());
                });
        return context.instrument(FDBStoreTimer.Events.LOAD_RECORD, result);
    }

    /**
     * Async version of {@link #loadRecordVersion(Tuple)}. If the
     * record does not have a version, but that cannot be determined
     * before making a call to the database, this might return a
     * completable future that wraps <code>null</code>.
     * @param primaryKey the primary key of the record
     * @return a future that completes with the version of the record of {@code Optional.empty()} if versions are not enabled for this store
     */
    @Nonnull
    public Optional<CompletableFuture<FDBRecordVersion>> loadRecordVersionAsync(@Nonnull final Tuple primaryKey) {
        return loadRecordVersionAsync(primaryKey, false);
    }

    /**
     * Async version of {@link #loadRecordVersion(Tuple, boolean)}.
     * @param primaryKey the primary key of the record
     * @param snapshot whether to snapshot read
     * @return a future that completes with the version of the record of {@code Optional.empty()} if versions are not enabled for this store
     */
    @Nonnull
    public Optional<CompletableFuture<FDBRecordVersion>> loadRecordVersionAsync(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        if (useOldVersionFormat() && !metaData.isStoreRecordVersions()) {
            // Because we clear out the version space whenever the user specifies that they
            // are not storing record versions (in the older format version), we can know
            // a priori that this will return an empty optional, so we return it without doing any I/O.
            return Optional.empty();
        } else {
            Optional<CompletableFuture<FDBRecordVersion>> cachedOptional = context.getLocalVersion(primaryKey)
                    .map(localVersion -> CompletableFuture.completedFuture(FDBRecordVersion.incomplete(localVersion)));
            if (cachedOptional.isPresent()) {
                return cachedOptional;
            }

            byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
            final ReadTransaction tr = snapshot ? ensureContextActive().snapshot() : ensureContextActive();
            return Optional.of(tr.get(versionKey).thenApply(valueBytes -> {
                if (valueBytes == null) {
                    return null;
                } else if (useOldVersionFormat()) {
                    return FDBRecordVersion.complete(valueBytes, false);
                } else {
                    return SplitHelper.unpackVersion(valueBytes);
                }
            }));
        }
    }

    /**
     * Load the version of the last time a record with the
     * given primary key was saved. If the record does
     * not have a version (because the record does not exist
     * or the record was last loaded when versions were
     * not being stored), it will return the empty {@link Optional}.
     * @param primaryKey the primary key for the record
     * @return an {@link Optional} that, if not empty, contains record's version
     */
    @Nonnull
    public Optional<FDBRecordVersion> loadRecordVersion(@Nonnull final Tuple primaryKey) {
        return loadRecordVersionAsync(primaryKey).map(future -> context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_VERSION, future));
    }

    /**
     * Overload of {@link #loadRecordVersion(Tuple)} that supports snapshot
     * reads. If <code>snapshot</code> is set to <code>true</code>,
     * reading the record's version does not add a read conflict to the
     * store's transaction.
     * @param primaryKey the primary key for the record
     * @param snapshot whether this operation should be done with a <code>snapshot</code>
     * @return an {@link Optional} that, if not empty, contain's the record version
     */
    @Nonnull
    public Optional<FDBRecordVersion> loadRecordVersion(@Nonnull final Tuple primaryKey, final boolean snapshot) {
        return loadRecordVersionAsync(primaryKey, snapshot).map(future -> context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_VERSION, future));
    }

    // This only needs to return the builder instead of the built record to support the temporary timing fields.
    // TODO: Simplify after that is removed.
    private CompletableFuture<FDBStoredRecordBuilder<M>> deserializeRecord(@Nonnull final RecordMetaData metaData,
                                                                           @Nonnull final FDBRawRecord rawRecord,
                                                                           @Nonnull final Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional) {
        final Tuple primaryKey = rawRecord.getPrimaryKey();
        final byte[] serialized = rawRecord.getRawRecord();

        try {
            final M record = serializer.deserialize(metaData, primaryKey, rawRecord.getRawRecord(), getTimer());
            final RecordType recordType = metaData.getRecordTypeForDescriptor(record.getDescriptorForType());
            countKeysAndValues(FDBStoreTimer.Counts.LOAD_RECORD_KEY, FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES, FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES,
                    rawRecord);

            final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(record)
                    .setPrimaryKey(primaryKey).setRecordType(recordType).setSize(rawRecord);

            if (rawRecord.hasVersion()) {
                // In the current format version, the version should be read along with the version,
                // so this should be hit the majority of the time.
                recordBuilder.setVersion(rawRecord.getVersion());
                return CompletableFuture.completedFuture(recordBuilder);
            } else if (versionFutureOptional.isPresent()) {
                // In an old format version, the record version was stored separately and requires
                // another read (which has hopefully happened in parallel with the main record read in the background).
                return versionFutureOptional.get().thenApply(recordBuilder::setVersion);
            } else {
                // Look for the version in the various places that it might be. If we can't find it, then
                // this will return an FDBStoredRecord where the version is unset.
                return CompletableFuture.completedFuture(recordBuilder);
            }
        } catch (Exception ex) {
            final LoggableException ex2 = new RecordCoreException("Failed to deserialize record", ex);
            ex2.addLogInfo(
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.PRIMARY_KEY, primaryKey);
            if (LOGGER.isDebugEnabled()) {
                ex2.addLogInfo("serialized", ByteArrayUtil2.loggable(serialized),
                        "descriptor", metaData.getUnionDescriptor().getFile().toProto());
            }
            throw ex2;
        }
    }

    protected void countKeysAndValues(@Nonnull final FDBStoreTimer.Count key,
                                      @Nonnull final FDBStoreTimer.Count keyBytes,
                                      @Nonnull final FDBStoreTimer.Count valueBytes,
                                      @Nonnull final FDBStoredSizes sizeInfo) {
        final FDBStoreTimer timer = getTimer();
        if (timer != null) {
            timer.increment(key, sizeInfo.getKeyCount());
            timer.increment(keyBytes, sizeInfo.getKeySize());
            timer.increment(valueBytes, sizeInfo.getValueSize());
        }
    }

    public void countKeyValue(@Nonnull final FDBStoreTimer.Count key,
                              @Nonnull final FDBStoreTimer.Count keyBytes,
                              @Nonnull final FDBStoreTimer.Count valueBytes,
                              @Nonnull final KeyValue keyValue) {
        countKeyValue(key, keyBytes, valueBytes, keyValue.getKey(), keyValue.getValue());
    }

    public void countKeyValue(@Nonnull final FDBStoreTimer.Count key,
                              @Nonnull final FDBStoreTimer.Count keyBytes,
                              @Nonnull final FDBStoreTimer.Count valueBytes,
                              @Nonnull final byte[] k, @Nonnull final byte[] v) {
        final FDBStoreTimer timer = getTimer();
        if (timer != null) {
            timer.increment(key);
            timer.increment(keyBytes, k.length);
            timer.increment(valueBytes, v.length);
        }
    }

    /**
     * Get record into FDB RYW cache.
     * Caller needs to hold on to result until ready or else there is a chance it will get
     * GC'ed and cancelled before then.
     * @param primaryKey the primary key for the record to retrieve
     * @return a future that will return null when the record is preloaded
     */
    @Nonnull
    public CompletableFuture<Void> preloadRecordAsync(@Nonnull final Tuple primaryKey) {
        return loadRawRecordAsync(primaryKey, null, false).thenAccept(fdbRawRecord -> preloadCache.put(primaryKey, fdbRawRecord));
    }


    /**
     * Asynchronously load a record.
     * @param primaryKey the key for the record to be loaded
     * @param sizeInfo a size info to fill in from serializer
     * @param snapshot whether to snapshot read
     * @return a CompletableFuture that will return a message or null if there was no record with that key
     */
    @Nonnull
    public CompletableFuture<FDBRawRecord> loadRawRecordAsync(@Nonnull final Tuple primaryKey,
                                                              @Nullable final SplitHelper.SizeInfo sizeInfo,
                                                              final boolean snapshot) {
        final FDBRawRecord recordFromCache = preloadCache.getIfPresent(primaryKey);
        if (recordFromCache != null) {
            return CompletableFuture.completedFuture(recordFromCache);
        }
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final ReadTransaction tr = snapshot ? ensureContextActive().snapshot() : ensureContextActive();
        return SplitHelper.loadWithSplit(tr, context, recordsSubspace(),
                primaryKey, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix, sizeInfo);
    }

    /**
     * Scan the records in the database.
     *
     * @param continuation any continuation from a previous scan
     * @param scanProperties skip, limit and other scan properties
     *
     * @return a cursor that will scan everything in the range, picking up at continuation, and honoring the given scan properties
     */
    @Nonnull
    public RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
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
    public RecordCursor<FDBStoredRecord<M>> scanRecords(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
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
    public RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable final Tuple low, @Nullable final Tuple high,
                                                        @Nonnull final EndpointType lowEndpoint, @Nonnull final EndpointType highEndpoint,
                                                        @Nullable byte[] continuation,
                                                        @Nonnull ScanProperties scanProperties) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Subspace recordsSubspace = recordsSubspace();
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        final RecordCursor<FDBRawRecord> rawRecords;
        if (metaData.isSplitLongRecords()) {
            RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(recordsSubspace)
                    .setContext(context).setContinuation(continuation)
                    .setLow(low, lowEndpoint)
                    .setHigh(high, highEndpoint)
                    .setScanProperties(scanProperties.with(ExecuteProperties::clearRowAndTimeLimits).with(ExecuteProperties::clearState))
                    .build();
            rawRecords = new SplitHelper.KeyValueUnsplitter(context, recordsSubspace, keyValues, useOldVersionFormat(), sizeInfo, scanProperties.isReverse(),
                    new CursorLimitManager(context, scanProperties.with(ExecuteProperties::clearReturnedRowLimit)))
                .skip(scanProperties.getExecuteProperties().getSkip())
                .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        } else {
            KeyValueCursor.Builder keyValuesBuilder = KeyValueCursor.Builder.withSubspace(recordsSubspace)
                    .setContext(context).setContinuation(continuation)
                    .setLow(low, lowEndpoint)
                    .setHigh(high, highEndpoint);
            if (omitUnsplitRecordSuffix) {
                rawRecords = keyValuesBuilder.setScanProperties(scanProperties).build().map(kv -> {
                    sizeInfo.set(kv);
                    Tuple primaryKey = SplitHelper.unpackKey(recordsSubspace, kv);
                    return new FDBRawRecord(primaryKey, kv.getValue(), null, sizeInfo);
                });
            } else {
                // Adjust limit to twice the supplied limit in case there are versions in the records
                final ScanProperties finalScanProperties = scanProperties
                        .with(executeProperties -> {
                            final ExecuteProperties adjustedExecuteProperties = executeProperties.clearSkipAndAdjustLimit().clearState();
                            int returnedRowLimit = adjustedExecuteProperties.getReturnedRowLimitOrMax();
                            if (returnedRowLimit != Integer.MAX_VALUE) {
                                return adjustedExecuteProperties.setReturnedRowLimit(2 * returnedRowLimit);
                            } else {
                                return adjustedExecuteProperties;
                            }
                        });
                rawRecords = new SplitHelper.KeyValueUnsplitter(context, recordsSubspace, keyValuesBuilder
                        .setScanProperties(finalScanProperties).build(),
                        useOldVersionFormat(), sizeInfo, scanProperties.isReverse(),
                        new CursorLimitManager(context, scanProperties.with(ExecuteProperties::clearReturnedRowLimit)))
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
            }
        }
        RecordCursor<FDBStoredRecord<M>> result = rawRecords.mapPipelined(rawRecord -> {
            final Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional;
            if (useOldVersionFormat()) {
                // Older format versions: do a separate read to get the version.
                versionFutureOptional = loadRecordVersionAsync(rawRecord.getPrimaryKey(), scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot());
            } else {
                // Newer format versions: the version is either in the record or it is not -- do not do another read.
                versionFutureOptional = Optional.empty();
            }
            return deserializeRecord(metaData,
                    rawRecord,
                    versionFutureOptional).thenApply(FDBStoredRecordBuilder::build);
        }, pipelineSizer.getPipelineSize(PipelineOperation.KEY_TO_RECORD));
        return context.instrument(FDBStoreTimer.Events.SCAN_RECORDS, result);
    }

    @Nonnull
    public CompletableFuture<Integer> countRecords(
            @Nullable Tuple low, @Nullable Tuple high,
            @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        return countRecords(low, high, lowEndpoint, highEndpoint, null, ScanProperties.FORWARD_SCAN);
    }

    @Nonnull
    public CompletableFuture<Integer> countRecords(
            @Nullable Tuple low, @Nullable Tuple high,
            @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint,
            @Nullable byte[] continuation,
            @Nonnull ScanProperties scanProperties) {
        final Subspace recordsSubspace = recordsSubspace();
        RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(recordsSubspace)
                .setContext(context)
                .setLow(low, lowEndpoint)
                .setHigh(high, highEndpoint)
                .setContinuation(continuation)
                .setScanProperties(scanProperties.with(ExecuteProperties::clearRowAndTimeLimits)
                        .with(ExecuteProperties::clearState))
                .build();
        if (getRecordMetaData().isSplitLongRecords()) {
            return new SplitHelper.KeyValueUnsplitter(context, recordsSubspace, keyValues, useOldVersionFormat(), null, scanProperties.isReverse(),
                    new CursorLimitManager(context, scanProperties.with(ExecuteProperties::clearRowAndTimeLimits))).getCount();
        } else {
            return keyValues.getCount();
        }
    }

    @Nonnull
    public RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanType scanType,
                                              @Nonnull TupleRange range,
                                              @Nullable byte[] continuation,
                                              @Nonnull ScanProperties scanProperties) {
        return scanIndex(index, scanType, range, continuation, scanProperties, null);
    }

    @Nonnull
    public RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanType scanType,
                                              @Nonnull TupleRange range,
                                              @Nullable byte[] continuation,
                                              @Nonnull ScanProperties scanProperties,
                                              @Nullable RecordScanLimiter recordScanLimiter) {
        if (!isIndexReadable(index)) {
            throw new RecordCoreException("Cannot scan non-readable index " + index.getName());
        }
        RecordCursor<IndexEntry> result = getIndexMaintainer(index)
                .scan(scanType, range, continuation, scanProperties);
        return context.instrument(FDBStoreTimer.Events.SCAN_INDEX_KEYS, result);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName) {
        return scanIndexRecords(indexName, IsolationLevel.SERIALIZABLE);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName, IsolationLevel isolationLevel) {
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationLevel).build()));
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName,
                                                              @Nonnull final IndexScanType scanType,
                                                              @Nonnull final TupleRange range,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull ScanProperties scanProperties) {
        return scanIndexRecords(indexName, scanType, range, continuation, IndexOrphanBehavior.ERROR, scanProperties, null);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName, @Nonnull final IndexScanType scanType,
                                                              @Nonnull final TupleRange range,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull ScanProperties scanProperties,
                                                              @Nullable RecordScanLimiter recordScanLimiter) {
        return scanIndexRecords(indexName, scanType, range, continuation, IndexOrphanBehavior.ERROR, scanProperties, recordScanLimiter);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName,
                                                              @Nonnull final IndexScanType scanType,
                                                              @Nonnull final TupleRange range,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull IndexOrphanBehavior orphanBehavior,
                                                              @Nonnull ScanProperties scanProperties) {
        return scanIndexRecords(indexName, scanType, range, continuation, orphanBehavior, scanProperties, null);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecords(@Nonnull final String indexName,
                                                              @Nonnull final IndexScanType scanType,
                                                              @Nonnull final TupleRange range,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull IndexOrphanBehavior orphanBehavior,
                                                              @Nonnull ScanProperties scanProperties,
                                                              @Nullable RecordScanLimiter recordScanLimiter) {
        final Index index = metaDataProvider.getRecordMetaData().getIndex(indexName);
        return fetchIndexRecords(index, scanIndex(index, scanType, range, continuation, scanProperties, recordScanLimiter), orphanBehavior);
    }

    /**
     * Given a cursor that iterates over entries in an index, attempts to fetch the associated records for those entries.
     *
     * @param index The definition of the index being scanned.
     * @param indexCursor A cursor iterating over entries in the index.
     * @param orphanBehavior How the iteration process should respond in the face of entries in the index for which
     *    there is no associated record.
     * @return A cursor returning indexed record entries.
     */
    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> fetchIndexRecords(@Nonnull Index index,
                                                               @Nonnull RecordCursor<IndexEntry> indexCursor,
                                                               @Nonnull IndexOrphanBehavior orphanBehavior) {
        RecordCursor<FDBIndexedRecord<M>> recordCursor = indexCursor.mapPipelined(entry ->
                loadIndexEntryRecord(index, entry, orphanBehavior), getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
        if (orphanBehavior == IndexOrphanBehavior.SKIP) {
            recordCursor = recordCursor.filter(Objects::nonNull);
        }
        return recordCursor;
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecordsEqual(@Nonnull final String indexName, @Nonnull final Object... values) {
        final Tuple tuple = Tuple.from(values);
        final TupleRange range = TupleRange.allOf(tuple);
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN);
    }

    @Nonnull
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRecordsBetween(@Nonnull final String indexName,
                                                                     @Nullable final Object low, @Nullable final Object high) {
        final Tuple lowTuple = Tuple.from(low);
        final Tuple highTuple = Tuple.from(high);
        final TupleRange range = new TupleRange(lowTuple, highTuple,
                EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
        return scanIndexRecords(indexName, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Determine if a given index entry points to a record.
     * @param index the index to check
     * @param entry the index entry to check
     * @param snapshot whether to use snapshot read
     * @return a future that completes with {@code true} if the given index entry still points to a record
     */
    public CompletableFuture<Boolean> hasIndexEntryRecord(@Nonnull final Index index,
                                                          @Nonnull final IndexEntry entry,
                                                          boolean snapshot) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Tuple primaryKey = indexEntryPrimaryKey(index, entry.getKey());
        final ReadTransaction tr = snapshot ? ensureContextActive().snapshot() : ensureContextActive();
        return SplitHelper.keyExists(tr, context, recordsSubspace(),
                primaryKey, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix);
    }

    @Nonnull
    protected CompletableFuture<FDBIndexedRecord<M>> loadIndexEntryRecord(@Nonnull final Index index,
                                                                          @Nonnull final IndexEntry entry,
                                                                          @Nonnull final IndexOrphanBehavior orphanBehavior) {
        final Tuple primaryKey = indexEntryPrimaryKey(index, entry.getKey());

        return loadRecordInternal(primaryKey, false).thenApply(record -> {
            if (record == null) {
                switch (orphanBehavior) {
                    case SKIP:
                        return null;
                    case RETURN:
                        break;
                    case ERROR:
                        throw new RecordCoreStorageException("record not found from index entry").addLogInfo(
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.PRIMARY_KEY, primaryKey,
                                LogMessageKeys.INDEX_KEY, entry.getKey(),
                                subspaceProvider.logKey(), subspaceProvider);
                    default:
                        throw new RecordCoreException("Unexpected index orphan behavior: " + orphanBehavior);
                }
            }
            return new FDBIndexedRecord<>(index, entry, record);
        });
    }

    /**
     * Scan the list of uniqueness violations identified for an index. It looks only for violations
     * within the given range subject to the given limit and (possibly) will go in reverse.
     * They will be returned in an order that is grouped by the index value keys that they have in common
     * and will be ordered within the grouping by the primary key.
     *
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull TupleRange range,
                                                                                 @Nullable byte[] continuation,
                                                                                 @Nonnull ScanProperties scanProperties) {
        RecordCursor<IndexEntry> tupleCursor = getIndexMaintainer(index).scanUniquenessViolations(range, continuation, scanProperties);
        return tupleCursor.map(entry -> {
            int indexColumns = index.getColumnSize();
            Tuple valueKey = TupleHelpers.subTuple(entry.getKey(), 0, indexColumns);
            Tuple primaryKey = TupleHelpers.subTuple(entry.getKey(), indexColumns, entry.getKey().size());
            Tuple existingKey = entry.getValue();
            return new RecordIndexUniquenessViolation(index, new IndexEntry(valueKey, entry.getValue()), primaryKey, existingKey);
        });
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Key.Evaluated indexKey, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Tuple valueKey) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull Key.Evaluated indexKey) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, int limit) {
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
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index) {
        return scanUniquenessViolations(index, Integer.MAX_VALUE);
    }

    /**
     * Removes all of the records that have the given value set as their index value (and are thus causing a
     * uniqueness violation) except for the one that has the given primary key (if the key is not <code>null</code>).
     * It also cleans up the set of uniqueness violations so that none of the remaining entries will
     * have be associated with the given value key.
     * @param index the index to resolve uniqueness violations for
     * @param valueKey the value of the index that is being removed
     * @param primaryKey the primary key of the record that should remain (or <code>null</code> to remove all of them)
     * @return a future that will complete when all of the records have been removed
     */
    @Nonnull
    public CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable Tuple primaryKey) {
        return scanUniquenessViolations(index, valueKey).forEachAsync(uniquenessViolation -> {
            if (primaryKey == null || !primaryKey.equals(uniquenessViolation.getPrimaryKey())) {
                return deleteRecordAsync(uniquenessViolation.getPrimaryKey()).thenApply(ignore -> null);
            } else {
                getIndexMaintainer(index).updateUniquenessViolations(valueKey, primaryKey, null, true);
                return AsyncUtil.DONE;
            }
        }, getPipelineSize(PipelineOperation.RESOLVE_UNIQUENESS));
    }

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
    public CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Key.Evaluated indexKey, @Nullable Tuple primaryKey) {
        return resolveUniquenessViolation(index, indexKey.toTuple(), primaryKey);
    }

    /**
     * Get the primary key portion of an index entry.
     * @param index the index associated with this entry
     * @param entry the index entry
     * @return the primary key extracted from the entry
     */
    @Nonnull
    public static Tuple indexEntryPrimaryKey(@Nonnull Index index, @Nonnull Tuple entry) {
        List<Object> entryKeys = entry.getItems();
        List<Object> primaryKeys;
        int[] positions = index.getPrimaryKeyComponentPositions();
        if (positions == null) {
            primaryKeys = entryKeys.subList(index.getColumnSize(), entryKeys.size());
        } else {
            primaryKeys = new ArrayList<>(positions.length);
            int after = index.getColumnSize();
            for (int i = 0; i < positions.length; i++) {
                primaryKeys.add(entryKeys.get(positions[i] < 0 ? after++ : positions[i]));
            }
        }
        return Tuple.fromList(primaryKeys);
    }

    /**
     * Return the key portion of <code>entry</code>, which should be the key with the index value
     * as a tuple. This is used to store the index uniqueness violations when building a
     * unique index.
     * @param valueKey the value of the index for a record
     * @param primaryKey the primary key for a record
     * @return a tuple that is the two keys appended together
     */
    @Nonnull
    public static Tuple uniquenessViolationKey(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        return valueKey.addAll(primaryKey);
    }

    /**
     * Return a tuple to be used as the key for an index entry for the given value and primary key.
     * @param index the index for which this will be an entry
     * @param valueKey the indexed value(s) for the entry
     * @param primaryKey the primary key for the record
     * @return the key to use for an index entry, the two tuples appended with redundant parts of the primary key removed
     */
    @Nonnull
    public static Tuple indexEntryKey(@Nonnull Index index, @Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        List<Object> primaryKeys = primaryKey.getItems();
        index.trimPrimaryKey(primaryKeys);
        if (primaryKeys.isEmpty()) {
            return valueKey;
        } else {
            return valueKey.addAll(primaryKeys);
        }
    }

    @Nonnull
    private Tuple recordVersionKey(@Nonnull Tuple primaryKey) {
        if (useOldVersionFormat()) {
            return Tuple.from(RECORD_VERSION_KEY).addAll(primaryKey);
        } else {
            return Tuple.from(RECORD_KEY).addAll(primaryKey).add(SplitHelper.RECORD_VERSION);
        }
    }

    /**
     * Async version of {@link #deleteRecord}.
     * @param primaryKey the primary key of the record to delete
     * @return a future that completes {@code true} if the record was present to be deleted
     */
    @Nonnull
    public CompletableFuture<Boolean> deleteRecordAsync(@Nonnull final Tuple primaryKey) {
        preloadCache.invalidate(primaryKey);
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        CompletableFuture<Boolean> result = loadRecordAsync(primaryKey).thenCompose(oldRecord -> {
            if (oldRecord == null) {
                return AsyncUtil.READY_FALSE;
            }
            SplitHelper.deleteSplit(getRecordContext(), recordsSubspace(), primaryKey, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix, true, oldRecord);
            countKeysAndValues(FDBStoreTimer.Counts.DELETE_RECORD_KEY, FDBStoreTimer.Counts.DELETE_RECORD_KEY_BYTES, FDBStoreTimer.Counts.DELETE_RECORD_VALUE_BYTES,
                    oldRecord);
            addRecordCount(metaData, oldRecord, LITTLE_ENDIAN_INT64_MINUS_ONE);
            final boolean oldHasIncompleteVersion = oldRecord.hasVersion() && !oldRecord.getVersion().isComplete();
            if (useOldVersionFormat()) {
                if (oldHasIncompleteVersion) {
                    byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
                    context.removeVersionMutation(versionKey);
                } else if (metaData.isStoreRecordVersions()) {
                    ensureContextActive().clear(getSubspace().pack(recordVersionKey(primaryKey)));
                }
            }
            CompletableFuture<Void> updateIndexesFuture = updateSecondaryIndexes(oldRecord, null);
            if (oldHasIncompleteVersion) {
                return updateIndexesFuture.thenApply(vignore -> {
                    context.removeLocalVersion(primaryKey);
                    return true;
                });
            } else {
                return updateIndexesFuture.thenApply(vignore -> true);
            }
        });
        return context.instrument(FDBStoreTimer.Events.DELETE_RECORD, result);
    }

    /**
     * Delete the record with the given primary key.
     *
     * @param primaryKey the primary key for the record to be deleted
     *
     * @return true if something was there to delete, false if the record didn't exist
     */
    public boolean deleteRecord(@Nonnull Tuple primaryKey) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordAsync(primaryKey));
    }

    /**
     * Delete all the data in the record store.
     * <p>
     * Everything except the store header is cleared from the database.
     * This is an efficient operation, since all the data is contiguous.
     */
    public void deleteAllRecords() {
        preloadCache.invalidateAll();
        Transaction tr = ensureContextActive();
        tr.clear(recordsSubspace().getKey(),
                 getSubspace().range().end);
    }

    /**
     * Delete records and associated index entries matching a query filter.
     * <p>
     * Throws an exception if the operation cannot be done efficiently in a small number of contiguous range
     * clears. In practice, this means that the query filter must constrain a prefix of all record types' primary keys
     * and of all indexes' root expressions.
     *
     * @param component the query filter for records to delete efficiently
     */
    public void deleteRecordsWhere(@Nonnull QueryComponent component) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordsWhereAsync(component));
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
    public void deleteRecordsWhere(@Nonnull String recordType, @Nullable QueryComponent component) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_RECORD, deleteRecordsWhereAsync(recordType, component));
    }

    /**
     * Async version of {@link #deleteRecordsWhereAsync}.
     *
     * @param component the query filter for records to delete efficiently
     * @return a future that will be complete when the delete is done
     */
    public CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull QueryComponent component) {
        preloadCache.invalidateAll();
        return new RecordsWhereDeleter(component).run();
    }

    /**
     * Async version of {@link #deleteRecordsWhere(String, QueryComponent)}.
     * @param recordType the type of records to delete
     * @param component the query filter for records to delete efficiently or {@code null} to delete all records of the given type
     * @return a future that will be complete when the delete is done
     */
    public CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull String recordType, @Nullable QueryComponent component) {
        return deleteRecordsWhereAsync(mergeRecordTypeAndComponent(recordType, component));
    }

    private static QueryComponent mergeRecordTypeAndComponent(@Nonnull String recordType, @Nullable QueryComponent component) {
        if (component == null) {
            return new RecordTypeKeyComparison(recordType);
        }
        List<QueryComponent> components = new ArrayList<>();
        components.add(new RecordTypeKeyComparison(recordType));
        if (component instanceof AndComponent) {
            components.addAll(((AndComponent)component).getChildren());
        } else {
            components.add(component);
        }
        return Query.and(components);
    }

    class RecordsWhereDeleter {
        @Nonnull final RecordMetaData recordMetaData;
        @Nullable final RecordType recordType;

        @Nonnull final QueryToKeyMatcher matcher;
        @Nullable final QueryToKeyMatcher indexMatcher;

        @Nonnull final Collection<RecordType> allRecordTypes;
        @Nonnull final Collection<Index> allIndexes;

        @Nonnull final List<IndexMaintainer<M>> indexMaintainers;

        @Nullable final Key.Evaluated evaluated;
        @Nullable final Key.Evaluated indexEvaluated;

        public RecordsWhereDeleter(@Nonnull QueryComponent component) {
            RecordTypeKeyComparison recordTypeKeyComparison = null;
            QueryComponent remainingComponent = null;
            if (component instanceof RecordTypeKeyComparison) {
                recordTypeKeyComparison = (RecordTypeKeyComparison)component;
            } else if (component instanceof AndComponent && ((AndComponent)component).getChildren().stream().anyMatch(c -> c instanceof RecordTypeKeyComparison)) {
                final List<QueryComponent> remainingChildren = new ArrayList<>(((AndComponent)component).getChildren());
                final Iterator<QueryComponent> iter = remainingChildren.iterator();
                while (iter.hasNext()) {
                    final QueryComponent child = iter.next();
                    if (child instanceof RecordTypeKeyComparison) {
                        recordTypeKeyComparison = (RecordTypeKeyComparison)child;
                        iter.remove();
                    }
                }
                if (remainingChildren.size() == 1) {
                    remainingComponent = remainingChildren.get(0);
                } else {
                    remainingComponent = Query.and(remainingChildren);
                }
            }
            if (recordTypeKeyComparison != null && !getRecordMetaData().primaryKeyHasRecordTypePrefix()) {
                throw new RecordCoreException("record type version of deleteRecordsWhere can only be used when all record types have a type prefix");
            }

            matcher = new QueryToKeyMatcher(component);
            recordMetaData = getRecordMetaData();
            if (recordTypeKeyComparison == null) {
                indexMatcher = matcher;
                allRecordTypes = recordMetaData.getRecordTypes().values();
                allIndexes = recordMetaData.getAllIndexes();
                recordType = null;
            } else {
                recordType = recordMetaData.getRecordType(recordTypeKeyComparison.getName());
                if (remainingComponent == null) {
                    indexMatcher = null;
                } else {
                    indexMatcher = new QueryToKeyMatcher(remainingComponent);
                }
                allRecordTypes = Collections.singletonList(recordType);
                allIndexes = recordType.getAllIndexes();
            }

            indexMaintainers = allIndexes.stream().map(FDBRecordStoreBase.this::getIndexMaintainer).collect(Collectors.toList());

            evaluated = deleteRecordsWhereCheckRecordTypes();
            if (recordTypeKeyComparison == null) {
                indexEvaluated = evaluated;
            } else {
                indexEvaluated = Key.Evaluated.concatenate(evaluated.values().subList(1, evaluated.values().size()));
            }
            deleteRecordsWhereCheckIndexes();
        }

        private Key.Evaluated deleteRecordsWhereCheckRecordTypes() {
            final FDBEvaluationContext<M> evaluationContext = emptyEvaluationContext();
            Key.Evaluated evaluated = null;

            for (RecordType recordType : allRecordTypes) {
                final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(recordType.getPrimaryKey());
                if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
                    throw new Query.InvalidExpressionException("deleteRecordsWhere not matching primary key " +
                                                               recordType.getName());
                }
                if (evaluated == null) {
                    evaluated = match.getEquality(evaluationContext);
                } else if (!evaluated.equals(match.getEquality(evaluationContext))) {
                    throw new RecordCoreException("Primary key prefixes don't align",
                            "initialPrefix", evaluated,
                            "secondPrefix", match.getEquality(evaluationContext),
                            "recordType", recordType.getName());
                }
            }
            if (evaluated == null) {
                return null;
            }

            final KeyExpression recordCountKey = getRecordMetaData().getRecordCountKey();
            if (recordCountKey != null) {
                final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(recordCountKey);
                if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
                    throw new Query.InvalidExpressionException("Record count key not matching for deleteRecordsWhere");
                }
                final Key.Evaluated subkey = match.getEquality(evaluationContext);
                if (!evaluated.equals(subkey)) {
                    throw new RecordCoreException("Record count key prefix doesn't align",
                            "initialPrefix", evaluated,
                            "secondPrefix", match.getEquality());
                }
            }

            return evaluated;
        }

        private void deleteRecordsWhereCheckIndexes() {
            if (evaluated == null) {
                return;
            }
            for (IndexMaintainer<M> index : indexMaintainers) {
                boolean canDelete;
                if (recordType == null) {
                    canDelete = index.canDeleteWhere(matcher, evaluated);
                } else {
                    if (Key.Expressions.hasRecordTypePrefix(index.state.index.getRootExpression())) {
                        canDelete = index.canDeleteWhere(matcher, evaluated);
                    } else {
                        if (recordMetaData.recordTypesForIndex(index.state.index).size() > 1) {
                            throw new RecordCoreException("Index " + index.state.index.getName() +
                                                          " applies to more record types than just " + recordType.getName());
                        }
                        if (indexMatcher != null) {
                            canDelete = index.canDeleteWhere(indexMatcher, indexEvaluated);
                        } else {
                            canDelete = true;
                        }
                    }
                }
                if (!canDelete) {
                    throw new Query.InvalidExpressionException("deleteRecordsWhere not supported by index " +
                                                               index.state.index.getName());
                }
            }
        }

        private CompletableFuture<Void> run() {
            if (evaluated == null) {
                // no record types
                LOGGER.warn("Tried to delete prefix with no record types");
                return AsyncUtil.DONE;
            }

            final Transaction tr = ensureContextActive();

            final Tuple prefix = evaluated.toTuple();
            final Subspace recordSubspace = recordsSubspace().subspace(prefix);
            tr.clear(recordSubspace.range());
            if (useOldVersionFormat() && getRecordMetaData().isStoreRecordVersions()) {
                final Subspace versionSubspace = getSubspace().subspace(Tuple.from(RECORD_VERSION_KEY).addAll(prefix));
                tr.clear(versionSubspace.range());
            }

            final KeyExpression recordCountKey = getRecordMetaData().getRecordCountKey();
            if (recordCountKey != null) {
                if (prefix.size() == recordCountKey.getColumnSize()) {
                    // Delete a single record used for counting
                    tr.clear(getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(prefix)));
                } else {
                    // Delete multiple records used for counting
                    tr.clear(getSubspace().subspace(Tuple.from(RECORD_COUNT_KEY)).subspace(prefix).range());
                }
            }

            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            final Tuple indexPrefix = indexEvaluated.toTuple();
            for (IndexMaintainer<M> index : indexMaintainers) {
                final CompletableFuture<Void> future;
                // Only need to check key expression in the case where a normal index has a different prefix.
                if (prefix == indexPrefix || Key.Expressions.hasRecordTypePrefix(index.state.index.getRootExpression())) {
                    future = index.deleteWhere(tr, prefix);
                } else {
                    future = index.deleteWhere(tr, indexPrefix);
                }
                if (!MoreAsyncUtil.isCompletedNormally(future)) {
                    futures.add(future);
                }
            }
            return AsyncUtil.whenAll(futures);
        }
    }

    /**
     * Hook for checking if store state for client changes.
     */
    public interface UserVersionChecker {
        /**
         * Check the user version.
         * @param oldUserVersion the old user version or <code>-1</code> if this is a new record store
         * @param oldMetaDataVersion the old meta-data version
         * @param metaData the meta-data provider that will be used to get meta-data
         * @return the user version to store in the record info header
         */
        public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion,
                                                           RecordMetaDataProvider metaData);

        /**
         * Determine what to do about an index needing to be built.
         * @param index the index that has not been built for this store
         * @param recordCount the number of records already in the store
         * @param indexOnNewRecordTypes <code>true</code> if all record types for the index are new (the number of
         *                              records related to this index is 0), in which case the index is able to be
         *                              "rebuilt" instantly with no cost.
         * @return the desired state of the new index. If this is {@link IndexState#READABLE}, the index will be built right away
         */
        public default IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
            return writeOnlyIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes);
        }
    }

    public static void deleteStore(FDBRecordContext context, KeySpacePath path) {
        final Subspace subspace = new Subspace(path.toTuple());
        deleteStore(context, subspace);
    }

    public static void deleteStore(FDBRecordContext context, Subspace subspace) {
        final Transaction transaction = context.ensureActive();
        transaction.clear(subspace.range());
    }

    /**
     * Action to take if the record store does / does not already exist.
     * @see FDBRecordStoreBuilder#createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck)
     */
    public enum StoreExistenceCheck {
        /**
         * No special action.
         *
         * This corresponds to {@link FDBRecordStoreBuilder#createOrOpen}
         */
        NONE,

        /**
         * Throw if the record store already exists.
         *
         * This corresponds to {@link FDBRecordStoreBuilder#create}
         * @see RecordStoreAlreadyExistsException
         */
        ERROR_IF_EXISTS,

        /**
         * Throw if the record store does not already exist.
         *
         * This corresponds to {@link FDBRecordStoreBuilder#open}
         * @see RecordStoreDoesNotExistException
         */
        ERROR_IF_NOT_EXISTS
    }

    /**
     * Checks the meta-data version key of the record store and compares it to the version
     * of the local meta-data object. If the meta-data version stored in the record store
     * does not match the version stored in the local meta-data copy, the following might
     * happen: (1) if the stored version is newer than the local version, this can return
     * an exceptionally completed future with an {@link FDBExceptions.FDBStoreException} indicating
     * that the meta-data is stale; (2) if the stored version is older than the local version, this
     * will update the store to include the changes that have happened since the last meta-data
     * version; and (3) if the versions are the same, it will do nothing. If branch (2) is followed,
     * then the process of updating the store based on the meta-data will do the following: (a) if
     * the number of records is small, it will build all of the indexes that have been added
     * between the two meta-data checks, or (b) if the number of records is large, it will
     * mark the new indexes as write-only. It is then up to the client to start online index
     * build jobs to build those indexes.
     *
     * This will also apply the passed {@link UserVersionChecker} instance to check the user-defined
     * version. The result of this check will be handled the same as with meta-data version changes, i.e.,
     * if the user version is newer than the stored version, then version is updated. If the user
     * version is older than the stored version, it will throw an exception about the stale user
     * version. If the two versions are the same, it will do nothing.
     *
     * This will return a future that will either complete exceptionally (in the case of a stale
     * user or meta-data) or will complete with <code>true</code> if the version was changed
     * because the record store had an old version or <code>false</code> if no change to the
     * store was done.
     *
     * @param userVersionChecker hook for checking if store state for client must change
     * @param existenceCheck whether to throw an exception if the record store does or does not already exists
     * @return future with whether the record store was modified by the check
     */
    @Nonnull
    public CompletableFuture<Boolean> checkVersion(@Nullable UserVersionChecker userVersionChecker,
                                                   @Nonnull StoreExistenceCheck existenceCheck) {
        CompletableFuture<byte[]> storeInfoFuture = readStoreInfo();
        if (recordStoreState == null) {
            storeInfoFuture = preloadRecordStoreStateAsync().thenCombine(storeInfoFuture, (v, b) -> b);
        }
        return checkVersion(storeInfoFuture, userVersionChecker, existenceCheck);
    }

    @Nonnull
    protected CompletableFuture<Boolean> checkVersion(@Nonnull CompletableFuture<byte[]> storeInfoFuture,
                                                      @Nullable UserVersionChecker userVersionChecker,
                                                      @Nonnull StoreExistenceCheck existenceCheck) {
        CompletableFuture<Boolean> result = storeInfoFuture.thenCompose(bytes -> {
            RecordMetaDataProto.DataStoreInfo.Builder info = RecordMetaDataProto.DataStoreInfo.newBuilder();
            final int oldMetaDataVersion;
            final int oldUserVersion;
            if (bytes == null) {
                if (existenceCheck == StoreExistenceCheck.ERROR_IF_NOT_EXISTS) {
                    throw new RecordStoreDoesNotExistException("Record store does not exist",
                                                               subspaceProvider.logKey(), subspaceProvider);
                }
                oldMetaDataVersion = oldUserVersion = -1;
                if (getTimer() != null) {
                    getTimer().increment(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
                }
            } else {
                if (existenceCheck == StoreExistenceCheck.ERROR_IF_EXISTS) {
                    throw new RecordStoreAlreadyExistsException("Record store already exists",
                                                                subspaceProvider.logKey(), subspaceProvider);
                }
                try {
                    info.mergeFrom(bytes);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreStorageException("Error reading version", ex);
                }
                if (info.getFormatVersion() < MIN_FORMAT_VERSION || info.getFormatVersion() > MAX_SUPPORTED_FORMAT_VERSION) {
                    throw new UnsupportedFormatVersionException("Unsupported format version " + info.getFormatVersion(),
                                                                subspaceProvider.logKey(), subspaceProvider);
                }
                oldMetaDataVersion = info.getMetaDataversion();
                oldUserVersion = info.getUserVersion();
            }
            if (info.hasFormatVersion() && info.getFormatVersion() >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
                // If the store is already using a format version greater than or equal to the version
                // where the unsplit records are now written with an extra suffix, use the value for
                // that stored from the database. (Note that this depends on the property that calling "get"
                // on an unset boolean field results in getting back "false".)
                omitUnsplitRecordSuffix = info.getOmitUnsplitRecordSuffix();
            }
            final boolean[] dirty = new boolean[1];
            final CompletableFuture<Void> checkedUserVersion = checkUserVersion(userVersionChecker, oldUserVersion, oldMetaDataVersion, info, dirty);
            final CompletableFuture<Void> checkedRebuild = checkedUserVersion.thenCompose(vignore -> checkPossiblyRebuild(userVersionChecker, info, dirty));
            return checkedRebuild.thenApply(vignore -> {
                if (dirty[0]) {
                    info.setLastUpdateTime(System.currentTimeMillis());
                    ensureContextActive().set(getSubspace().pack(STORE_INFO_KEY), info.build().toByteArray());
                }
                return dirty[0];
            });
        });
        return context.instrument(FDBStoreTimer.Events.CHECK_VERSION, result);
    }

    private CompletableFuture<Void> checkUserVersion(@Nullable UserVersionChecker userVersionChecker, int oldUserVersion, int oldMetaDataVersion,
                                                     @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info, @Nonnull boolean[] dirty) {
        if (userVersionChecker == null) {
            return AsyncUtil.DONE;
        }
        return userVersionChecker.checkUserVersion(oldUserVersion, oldMetaDataVersion, metaDataProvider)
                .thenApply(newUserVersion -> {
                    userVersion = newUserVersion;
                    if (newUserVersion != oldUserVersion) {
                        if (oldUserVersion > newUserVersion) {
                            LOGGER.error(KeyValueLogMessage.of("stale user version",
                                    "storedVersion", oldUserVersion,
                                    "localVersion", newUserVersion,
                                    subspaceProvider.logKey(), subspaceProvider));
                            throw new RecordStoreStaleUserVersionException("Stale user version with local version " + newUserVersion + " and stored version " + oldUserVersion);
                        }
                        info.setUserVersion(newUserVersion);
                        dirty[0] = true;
                        if (oldUserVersion < 0) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(KeyValueLogMessage.of("setting initial user version",
                                        LogMessageKeys.NEW_VERSION, newUserVersion,
                                        subspaceProvider.logKey(), subspaceProvider));
                            }
                        } else {
                            LOGGER.info(KeyValueLogMessage.of("changing user version",
                                    LogMessageKeys.OLD_VERSION, oldUserVersion,
                                    LogMessageKeys.NEW_VERSION, newUserVersion,
                                    subspaceProvider.logKey(), subspaceProvider));
                        }
                    }
                    return null;
                });
    }

    @Nonnull
    protected CompletableFuture<byte[]> readStoreInfo() {
        return context.instrument(FDBStoreTimer.Events.LOAD_RECORD_STORE_INFO,
                context.ensureActive().get(getSubspace().pack(STORE_INFO_KEY)));
    }

    @Nonnull
    public CompletableFuture<Void> rebuildAllIndexes() {
        Transaction tr = ensureContextActive();
        tr.clear(getSubspace().range(Tuple.from(INDEX_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_STATE_SPACE_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY)));
        List<CompletableFuture<Void>> work = new LinkedList<>();
        addRebuildRecordCountsJob(work);
        return rebuildIndexes(getRecordMetaData().getIndexesSince(-1), Collections.emptyMap(), work, RebuildIndexReason.REBUILD_ALL, null);
    }

    @Nonnull
    public CompletableFuture<Void> clearAndMarkIndexWriteOnly(@Nonnull String indexName) {
        return clearAndMarkIndexWriteOnly(metaDataProvider.getRecordMetaData().getIndex(indexName));
    }

    /**
     * Prepare an index for rebuilding by clearing any existing data and marking the index as write-only.
     * @param index the index to build
     * @return a future that completes when the index has been cleared and marked write-only for building
     */
    @Nonnull
    public CompletableFuture<Void> clearAndMarkIndexWriteOnly(@Nonnull Index index) {
        return markIndexWriteOnly(index).thenApply(changed -> {
            clearIndexData(index);
            return null;
        });
    }

    @Nonnull
    private CompletableFuture<Boolean> markIndexNotReadable(@Nonnull String indexName, @Nonnull IndexState indexState) {
        if (recordStoreState == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexNotReadable(indexName, indexState));
        }

        addIndexStateReadConflict(indexName);

        recordStoreState.beginWrite();
        boolean haveFuture = false;
        try {
            // A read is done before the write in order to avoid having unnecessary
            // updates cause spurious not_committed errors.
            byte[] indexKey = indexStateSubspace().pack(indexName);
            Transaction tr = context.ensureActive();
            CompletableFuture<Boolean> future = tr.get(indexKey).thenApply(previous -> {
                if (previous == null || !Tuple.fromBytes(previous).get(0).equals(indexState.code())) {
                    tr.set(indexKey, Tuple.from(indexState.code()).pack());
                    recordStoreState.setState(indexName, indexState);
                    return true;
                } else {
                    return false;
                }
            }).whenComplete((b, t) -> recordStoreState.endWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                recordStoreState.endWrite();
            }
        }
    }

    /**
     * Adds the index of the given name to the list of write-only indexes stored within the store.
     * This will update the list stored within database.
     * This will return <code>true</code> if the store had to
     * be modified in order to mark the index as write only (i.e., the index
     * was not already write-only) and <code>false</code> otherwise.
     *
     * @param indexName the name of the index to mark as write-only
     * @return a future that will contain <code>true</code> if the store was modified and <code>false</code>
     * otherwise
     * @throws IllegalArgumentException if the index is not present in the meta-data
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexWriteOnly(@Nonnull String indexName) {
        return markIndexNotReadable(indexName, IndexState.WRITE_ONLY);
    }

    /**
     * Adds the given index to the list of write-only indexes stored within the store.
     * See the version of {@link FDBRecordStoreBase#markIndexWriteOnly(String) markIndexWriteOnly()}
     * that takes a {@link String} for more details.
     *
     * @param index the index to mark as write only
     * @return a future that will contain <code>true</code> if the store was modified and
     * <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexWriteOnly(@Nonnull Index index) {
        return markIndexWriteOnly(index.getName());
    }

    /**
     * Adds the index of the given name to the list of disabled indexes stored
     * within the store. Because reading a disabled index later is unsafe unless
     * one does a rebuild between the time it is marked disabled and the
     * time it is marked readable, marking an index as disabled will also clear
     * the store of all data associated with that index. This will return <code>true</code>
     * if the store had to be modified to mark the index as diabled (i.e., the
     * index was not already disabled) and <code>false</code> otherwise.
     *
     * @param indexName the name of the index to mark as disabled
     * @return a future that will contain <code>true</code> if the store was modified and
     * <code>false</code> otherwise
     * @throws IllegalArgumentException if the index is not present in the meta-data
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexDisabled(@Nonnull String indexName) {
        return markIndexDisabled(metaDataProvider.getRecordMetaData().getIndex(indexName));
    }

    /**
     * Adds the index to the list of disabled indexes stored within the store. See the
     * version of {@link FDBRecordStoreBase#markIndexDisabled(Index)} markIndexDisabled()}
     * that takes a {@link String} for more details.
     *
     * @param index the index to mark as disabled
     * @return a future that will contain <code>true</code> if the store was modified and
     * <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexDisabled(@Nonnull Index index) {
        return markIndexNotReadable(index.getName(), IndexState.DISABLED).thenApply(changed -> {
            if (changed) {
                clearIndexData(index);
            }
            return changed;
        });
    }

    /**
     * Returns the first unbuilt range of an index that is currently being bulit.
     * If there is no range that is currently unbuilt, it will return an
     * empty {@link Optional}. If there is one, it will return an {@link Optional}
     * set to the first unbuilt range it finds.
     * @param index the index to check built state
     * @return a future that will contain the first unbuilt range if any
     */
    @Nonnull
    public CompletableFuture<Optional<Range>> firstUnbuiltRange(@Nonnull Index index) {
        if (!getRecordMetaData().hasIndex(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " does not exist in meta-data.");
        }
        Transaction tr = ensureContextActive();
        RangeSet rangeSet = new RangeSet(indexRangeSubspace(index));
        AsyncIterator<Range> missingRangeIterator = rangeSet.missingRanges(tr, null, null, 1).iterator();
        return missingRangeIterator.onHasNext().thenApply(hasFirst -> {
            if (hasFirst) {
                return Optional.of(missingRangeIterator.next());
            } else {
                return Optional.empty();
            }
        });
    }

    /**
     * Exception that can be thrown if one attempts to mark an index as readable
     * if it is not yet readable.
     */
    @SuppressWarnings({"serial", "squid:S1948", "squid:MaximumInheritanceDepth"})
    public static class IndexNotBuiltException extends RecordCoreException {
        @Nullable private final Range unbuiltRange;

        /**
         * Creates an exception with the given message and the given
         * range set as one of the unbuilt ranges.
         * @param message the message associated with this exception
         * @param unbuiltRange one of the unbuilt ranges associated with this exception
         * @param keyValues additional information for logging
         */
        public IndexNotBuiltException(@Nonnull String message, @Nullable Range unbuiltRange, @Nullable Object ... keyValues) {
            super(message, keyValues);
            this.unbuiltRange = unbuiltRange;
        }

        /**
         * Get the unbuilt range associated with this exception.
         * If none has been set for this exception, then it will
         * return <code>null</code>.
         * @return the unbuilt range
         */
        @Nullable
        public Range getUnbuiltRange() {
            return unbuiltRange;
        }
    }

    /**
     * Marks an index as readable. See the version of
     * {@link FDBRecordStoreBase#markIndexReadable(String) markIndexReadable()}
     * that takes a {@link String} as a parameter for more details.
     *
     * @param index the index to mark readable
     * @return a future that will either complete exceptionally if the index can not
     * be made readable or will contain <code>true</code> if the store was modified
     * and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexReadable(@Nonnull Index index) {
        if (recordStoreState == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexReadable(index));
        }

        addIndexStateReadConflict(index.getName());

        recordStoreState.beginWrite();
        boolean haveFuture = false;
        try {
            Transaction tr = ensureContextActive();
            byte[] indexKey = indexStateSubspace().pack(index.getName());
            CompletableFuture<Boolean> future = tr.get(indexKey).thenCompose(previous -> {
                if (previous != null) {
                    CompletableFuture<Optional<Range>> builtFuture = firstUnbuiltRange(index);
                    CompletableFuture<Optional<RecordIndexUniquenessViolation>> uniquenessFuture = scanUniquenessViolations(index, 1).first();
                    return CompletableFuture.allOf(builtFuture, uniquenessFuture).thenApply(vignore -> {
                        Optional<Range> firstUnbuilt = builtFuture.join();
                        Optional<RecordIndexUniquenessViolation> uniquenessViolation = uniquenessFuture.join();
                        if (firstUnbuilt.isPresent()) {
                            throw new IndexNotBuiltException("Attempted to make unbuilt index readable" , firstUnbuilt.get(),
                                    LogMessageKeys.INDEX_NAME, index.getName(),
                                    "unbuiltRangeBegin", ByteArrayUtil2.loggable(firstUnbuilt.get().begin),
                                    "unbuiltRangeEnd", ByteArrayUtil2.loggable(firstUnbuilt.get().end),
                                    subspaceProvider.logKey(), subspaceProvider);
                        } else if (uniquenessViolation.isPresent()) {
                            RecordIndexUniquenessViolation wrapped = new RecordIndexUniquenessViolation("Uniqueness violation when making index readable",
                                                                                                        uniquenessViolation.get());
                            wrapped.addLogInfo(
                                    LogMessageKeys.INDEX_NAME, index.getName(),
                                    subspaceProvider.logKey(), subspaceProvider);
                            throw wrapped;
                        } else {
                            tr.clear(indexKey);
                            recordStoreState.setState(index.getName(), IndexState.READABLE);
                            return true;
                        }
                    });
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            }).whenComplete((b, t) -> recordStoreState.endWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                recordStoreState.endWrite();
            }
        }
    }

    /**
     * Marks the index with the given name as readable. More correctly, it removes if from the write-only list,
     * so future queries (assuming that they reload the {@link RecordStoreState}) will
     * be able to use that index. This will check to make sure that the index is actually
     * built and ready to go before making it readable. If it is not, the future
     * will complete exceptionally with an {@link IndexNotBuiltException}. If the
     * store is modified when marking the index as readable (i.e., if it was previously
     * write-only), then this will return <code>true</code>. Otherwise, it will
     * return <code>false</code>.
     *
     * @param indexName the name of the index to mark readable
     * @return a future that will either complete exceptionally if the index can not
     * be made readable or will contain <code>true</code> if the store was modified
     * and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexReadable(@Nonnull String indexName) {
        return markIndexReadable(getRecordMetaData().getIndex(indexName));
    }

    /**
     * Marks the index with the given name as readable without checking to see if it is
     * ready. This is dangerous to do if one has not first verified that the
     * index is ready to be readable as it can cause half-built indexes to be
     * used within queries and can thus produce inconsistent results.
     *
     * @param indexName the name of the index to mark readable
     * @return a future that will contain <code>true</code> if the store was modified
     * and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> uncheckedMarkIndexReadable(@Nonnull String indexName) {
        if (recordStoreState == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> uncheckedMarkIndexReadable(indexName));
        }

        addIndexStateReadConflict(indexName);

        recordStoreState.beginWrite();
        boolean haveFuture = false;
        try {
            Transaction tr = ensureContextActive();
            byte[] indexKey = indexStateSubspace().pack(indexName);
            CompletableFuture<Boolean> future = tr.get(indexKey).thenApply(previous -> {
                if (previous != null) {
                    tr.clear(indexKey);
                    recordStoreState.setState(indexName, IndexState.READABLE);
                    return true;
                } else {
                    return false;
                }
            }).whenComplete((b, t) -> recordStoreState.endWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                recordStoreState.endWrite();
            }
        }
    }

    /**
     * Loads the current state of the record store asynchronously.
     * @return a future that will be complete when this store has loaded its record store state
     */
    @Nonnull
    public CompletableFuture<Void> preloadRecordStoreStateAsync() {
        return loadRecordStoreStateAsync(context, getSubspace()).thenAccept(state -> this.recordStoreState = state);
    }

    /**
     * Loads the current state of the record store within the given subspace asynchronously.
     * This behaves exactly like the three-parameter version of
     * {@link #loadRecordStoreStateAsync(FDBRecordContext, Subspace, IsolationLevel) loadRecordStoreStateAsync()},
     * but this fixes the {@link IsolationLevel} to {@link IsolationLevel#SNAPSHOT SNAPSHOT}. This is
     * because most operations will not depend on the entirety of the record store state but instead
     * only on the state of the indexes used. For this reason, to decrease conflicts, in the normal
     * case, the record store state should be loaded with the <code>SNAPSHOT</code> isolation level and
     * then consistency is maintained by only adding read conflicts for indexes whose state is actually
     * used.
     *
     * @param context the record context in which the retrieve the record store state
     * @param subspace the subspace of the record store
     * @return a future that will contain the state of the record state located at the given subspace
     */
    @Nonnull
    public static CompletableFuture<MutableRecordStoreState> loadRecordStoreStateAsync(@Nonnull FDBRecordContext context,
                                                                                       @Nonnull Subspace subspace) {
        return loadRecordStoreStateAsync(context, subspace, IsolationLevel.SNAPSHOT);
    }

    /**
     * Loads the current state of the record store within the given subspace asynchronously.
     * This method is static so that one can load the record store state before instantiating
     * the instance. This method is called for the user by the various static <code>open</code> methods.
     * @param context the record context in which to retrieve the record store state
     * @param subspace the subspace of the record store
     * @param isolationLevel the isolation level to use when reading
     * @return a future that will contain the state of the record store located at the given subspace
     */
    @Nonnull
    public static CompletableFuture<MutableRecordStoreState> loadRecordStoreStateAsync(@Nonnull FDBRecordContext context,
                                                                                       @Nonnull Subspace subspace,
                                                                                       @Nonnull IsolationLevel isolationLevel) {
        Subspace isSubspace = subspace.subspace(Tuple.from(INDEX_STATE_SPACE_KEY));
        KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(isSubspace)
                .setContext(context)
                .setRange(TupleRange.ALL)
                .setContinuation(null)
                .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationLevel).build()))
                .build();
        FDBStoreTimer timer = context.getTimer();
        CompletableFuture<MutableRecordStoreState> result = cursor.asList().thenApply(list -> {
            Map<String, IndexState> indexStateMap;
            if (list.isEmpty()) {
                indexStateMap = Collections.emptyMap();
            } else {
                ImmutableMap.Builder<String, IndexState> indexStateMapBuilder = ImmutableMap.builder();

                for (KeyValue kv : list) {
                    String indexName = isSubspace.unpack(kv.getKey()).getString(0);
                    Object code = Tuple.fromBytes(kv.getValue()).get(0);
                    indexStateMapBuilder.put(indexName, IndexState.fromCode(code));
                    if (timer != null) {
                        timer.increment(FDBStoreTimer.Counts.LOAD_STORE_STATE_KEY);
                        timer.increment(FDBStoreTimer.Counts.LOAD_STORE_STATE_KEY_BYTES, kv.getKey().length);
                        timer.increment(FDBStoreTimer.Counts.LOAD_STORE_STATE_VALUE_BYTES, kv.getValue().length);
                    }
                }

                indexStateMap = indexStateMapBuilder.build();
            }
            return new MutableRecordStoreState(indexStateMap);
        });
        if (timer != null) {
            result = timer.instrument(FDBStoreTimer.Events.LOAD_RECORD_STORE_STATE, result, context.getExecutor());
        }
        return result;
    }

    // add a read conflict key so that the transaction will fail if the index
    // state has changed
    private void addIndexStateReadConflict(@Nonnull String indexName) {
        if (!getRecordMetaData().hasIndex(indexName)) {
            throw new MetaDataException("Index " + indexName + " does not exist in meta-data.");
        }
        Transaction tr = ensureContextActive();
        byte[] indexStateKey = getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, indexName));
        tr.addReadConflictKey(indexStateKey);
    }

    private boolean checkIndexState(@Nonnull String indexName, @Nonnull IndexState indexState) {
        addIndexStateReadConflict(indexName);
        return getRecordStoreState().getState(indexName).equals(indexState);
    }

    /**
     * Determine if the index is readable for this record store. This method will not perform
     * any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     *
     * @param index the index to check for readability
     * @return <code>true</code> if the index is readable and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the same name as this index
     */
    public boolean isIndexReadable(@Nonnull Index index) {
        return isIndexReadable(index.getName());
    }

    /**
     * Determine if the index with the given name is readable for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param indexName the name of the index to check for readability
     * @return <code>true</code> if the named index is readable and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexReadable(@Nonnull String indexName) {
        return checkIndexState(indexName, IndexState.READABLE);
    }

    /**
     * Determine if the index is write-only for this record store. This method will not perform
     * any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     *
     * @param index the index to check if write-only
     * @return <code>true</code> if the index is write-only and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the same name as this index
     */
    public boolean isIndexWriteOnly(@Nonnull Index index) {
        return isIndexWriteOnly(index.getName());
    }

    /**
     * Determine if the index with the given name is write-only for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param indexName the name of the index to check if write-only
     * @return <code>true</code> if the named index is write-only and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexWriteOnly(@Nonnull String indexName) {
        return checkIndexState(indexName, IndexState.WRITE_ONLY);
    }

    /**
     * Determine if the index is disabled for this record store. This method will not perform
     * any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     *
     * @param index the index to check if write-only
     * @return <code>true</code> if the index is disabled and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the same name as this index
     */
    public boolean isIndexDisabled(@Nonnull Index index) {
        return isIndexDisabled(index.getName());
    }

    /**
     * Determine if the index with the given name is disabled for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param indexName the name of the index to check if disabled
     * @return <code>true</code> if the named index is disabled and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexDisabled(@Nonnull String indexName) {
        return checkIndexState(indexName, IndexState.DISABLED);
    }

    // Remove any indexes that do not match the filter.
    // NOTE: This assumes that the filter will not filter out any indexes if all indexes are readable.
    private List<Index> sanitizeIndexes(@Nonnull List<Index> indexes, @Nonnull Predicate<Index> filter) {
        final RecordStoreState localRecordStoreState = getRecordStoreState();
        localRecordStoreState.beginRead();
        try {
            if (localRecordStoreState.allIndexesReadable()) {
                indexes.forEach(index -> addIndexStateReadConflict(index.getName()));
                return indexes;
            } else {
                return indexes.stream().filter(filter).collect(Collectors.toList());
            }
        } finally {
            localRecordStoreState.endRead();
        }
    }

    /**
     * Gets the list of readable indexes that are on the given record type.
     * This only include those {@link Index}es that are readable, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of write-only indexes or its list of disabled indexes.
     * @param recordType type of record to get the readable indexes of
     * @return the list of readable indexes on the given type
     */
    @Nonnull
    public List<Index> getReadableIndexes(@Nonnull RecordTypeOrBuilder recordType) {
        return sanitizeIndexes(recordType.getIndexes(), this::isIndexReadable);
    }

    /**
     * Gets the list of enabled indexes that are on the given record type.
     * This only include those {@link Index}es that are enabled, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of disabled indexes.
     * @param recordType type of record to get the enabled indexes of
     * @return the list of enabled indexes on the given type
     */
    @Nonnull
    public List<Index> getEnabledIndexes(@Nonnull RecordTypeOrBuilder recordType) {
        return sanitizeIndexes(recordType.getIndexes(), index -> !isIndexDisabled(index));
    }

    /**
     * Gets the list of readable indexes that are on multiple record types one of which is the
     * given type. This only include those {@link Index}es that are readable, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of write-only indexes or its list of disabled indexes.
     * @param recordType type of record to get the readable multi-type indexes of
     * @return the list of readable indexes on multiple types including the given type
     */
    @Nonnull
    public List<Index> getReadableMultiTypeIndexes(@Nonnull RecordTypeOrBuilder recordType) {
        return sanitizeIndexes(recordType.getMultiTypeIndexes(), this::isIndexReadable);
    }

    /**
     * Gets the list of enabled indexes that are on multiple record types one of which is the
     * given type. This only include those {@link Index}es that are enabled, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of disabled indexes.
     * @param recordType type of record to get the enabled multi-type indexes of
     * @return the list of readable indexes on multiple types including the given type
     */
    @Nonnull
    public List<Index> getEnabledMultiTypeIndexes(@Nonnull RecordTypeOrBuilder recordType) {
        return sanitizeIndexes(recordType.getMultiTypeIndexes(), index -> !isIndexDisabled(index));
    }

    /**
     * Gets the list of readable universal indexes, i.e., the indexes on all record types.
     * This only include those {@link Index}es that are readable, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of write-only indexes or its list of disabled indexes.
     * @return the list of readable universal indexes
     */
    @Nonnull
    public List<Index> getReadableUniversalIndexes() {
        return sanitizeIndexes(getRecordMetaData().getUniversalIndexes(), this::isIndexReadable);
    }

    /**
     * Gets the list of enabled universal indexes, i.e., the indexes on all record types.
     * This only include those {@link Index}es that are enabled, i.e., the index name is not
     * within the {@link RecordStoreState}'s list of disabled indexes.
     * @return the list of readable universal indexes
     */
    @Nonnull
    public List<Index> getEnabledUniversalIndexes() {
        return sanitizeIndexes(getRecordMetaData().getUniversalIndexes(), index -> !isIndexDisabled(index));
    }


    public static final int MAX_PARALLEL_INDEX_REBUILD = 10;

    @Nonnull
    protected CompletableFuture<Void> rebuildIndexes(@Nonnull Map<Index, List<RecordType>> indexes, @Nonnull Map<Index, IndexState> newStates,
                                                     @Nonnull List<CompletableFuture<Void>> work, @Nonnull RebuildIndexReason reason,
                                                     @Nullable Integer oldMetaDataVersion) {
        Iterator<Map.Entry<Index, List<RecordType>>> indexIter = indexes.entrySet().iterator();
        return AsyncUtil.whileTrue(() -> {
            Iterator<CompletableFuture<Void>> workIter = work.iterator();
            while (workIter.hasNext()) {
                CompletableFuture<Void> workItem = workIter.next();
                if (workItem.isDone()) {
                    context.asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, workItem); // Just for error handling.
                    workIter.remove();
                }
            }
            while (work.size() < MAX_PARALLEL_INDEX_REBUILD) {
                if (indexIter.hasNext()) {
                    Map.Entry<Index, List<RecordType>> indexItem = indexIter.next();
                    Index index = indexItem.getKey();
                    List<RecordType> recordTypes = indexItem.getValue();
                    IndexState indexState = newStates.getOrDefault(index, IndexState.READABLE);
                    work.add(rebuildOrMarkIndex(index, indexState, recordTypes, reason, oldMetaDataVersion));
                } else {
                    break;
                }
            }
            if (work.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(work).thenApply(v -> true);
        }, getExecutor());
    }

    /**
     * Reason that an index is being rebuilt now.
     */
    public enum RebuildIndexReason {
        NEW_STORE, FEW_RECORDS, COUNTS_UNKNOWN, REBUILD_ALL, EXPLICIT, TEST
    }

    private boolean areAllRecordTypesSince(@Nullable Collection<RecordType> recordTypes, @Nullable Integer oldMetaDataVersion) {
        return recordTypes != null && oldMetaDataVersion != null && recordTypes.stream().allMatch(recordType -> {
            Integer sinceVersion = recordType.getSinceVersion();
            return sinceVersion != null && sinceVersion > oldMetaDataVersion;
        });
    }

    protected CompletableFuture<Void> rebuildOrMarkIndex(@Nonnull Index index, @Nonnull IndexState indexState,
                                                         @Nullable List<RecordType> recordTypes, @Nonnull RebuildIndexReason reason,
                                                         @Nullable Integer oldMetaDataVersion) {
        // Skip index rebuild if the index is on new record types.
        if (indexState != IndexState.DISABLED && areAllRecordTypesSince(recordTypes, oldMetaDataVersion)) {
            return rebuildIndexWithNoRecord(index, recordTypes, reason);
        }

        switch (indexState) {
            case WRITE_ONLY:
                return clearAndMarkIndexWriteOnly(index).thenApply(b -> null);
            case DISABLED:
                return markIndexDisabled(index).thenApply(b -> null);
            case READABLE:
            default:
                return rebuildIndex(index, recordTypes, reason);
        }
    }

    @Nonnull
    private CompletableFuture<Void> rebuildIndexWithNoRecord(@Nonnull final Index index, @Nullable final Collection<RecordType> recordTypes, @Nonnull RebuildIndexReason reason) {
        final boolean newStore = reason == RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index with no record",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    "indexVersion", index.getVersion(),
                    "reason", reason.name(),
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                LOGGER.debug(msg.toString());
            } else {
                LOGGER.info(msg.toString());
            }
        }

        return markIndexReadable(index).thenApply(b -> null);
    }

    public CompletableFuture<Void> rebuildIndex(@Nonnull Index index) {
        return rebuildIndex(index, getRecordMetaData().recordTypesForIndex(index), RebuildIndexReason.EXPLICIT);
    }

    @Nonnull
    @SuppressWarnings("squid:S2095")    // Resource usage for indexBuilder is too complicated for rule.
    public CompletableFuture<Void> rebuildIndex(@Nonnull final Index index, @Nullable final Collection<RecordType> recordTypes, @Nonnull RebuildIndexReason reason) {
        final boolean newStore = reason == RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    "indexVersion", index.getVersion(),
                    "reason", reason.name(),
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                LOGGER.debug(msg.toString());
            } else {
                LOGGER.info(msg.toString());
            }
        }

        long startTime = System.nanoTime();
        OnlineIndexBuilderBase<M> indexBuilder = new OnlineIndexBuilderBase<>(this, index, recordTypes);
        CompletableFuture<Void> future = indexBuilder.rebuildIndexAsync(this)
                .thenCompose(vignore -> markIndexReadable(index)
                        .handle((b, t) -> {
                            // Only call method that builds in the current transaction, so never any pending work,
                            // so it would work to close before returning future, which would look better to SonarQube.
                            // But this is better if close ever does more.
                            indexBuilder.close();
                            return null;
                        }));

        return context.instrument(FDBStoreTimer.Events.REBUILD_INDEX, future, startTime);
    }

    private CompletableFuture<Void> checkPossiblyRebuild(@Nullable UserVersionChecker userVersionChecker,
                                                         @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                         @Nonnull boolean[] dirty) {
        final int oldFormatVersion = info.getFormatVersion();
        final int newFormatVersion = Math.max(oldFormatVersion, formatVersion);
        final boolean formatVersionChanged = oldFormatVersion != newFormatVersion;
        formatVersion = newFormatVersion;

        final int oldMetaDataVersion = info.getMetaDataversion();
        final RecordMetaData metaData = getRecordMetaData();
        final int newMetaDataVersion = metaData.getVersion();
        if (oldMetaDataVersion > newMetaDataVersion) {
            CompletableFuture<Void> ret = new CompletableFuture<>();
            ret.completeExceptionally(new RecordStoreStaleMetaDataVersionException("Local meta-data has stale version",
                    "localVersion", newMetaDataVersion,
                    "storedVersion", oldMetaDataVersion,
                    subspaceProvider.logKey(), subspaceProvider));
            return ret;
        }
        final boolean metaDataVersionChanged = oldMetaDataVersion != newMetaDataVersion;

        if (!formatVersionChanged && !metaDataVersionChanged) {
            return AsyncUtil.DONE;
        }

        if (LOGGER.isInfoEnabled()) {
            final boolean newStore = oldFormatVersion == 0;
            if (newStore) {
                LOGGER.info(KeyValueLogMessage.of("new record store",
                        LogMessageKeys.FORMAT_VERSION, newFormatVersion,
                        LogMessageKeys.META_DATA_VERSION, newMetaDataVersion,
                        subspaceProvider.logKey(), subspaceProvider));
            } else {
                if (formatVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("format version changed",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, newFormatVersion,
                            subspaceProvider.logKey(), subspaceProvider));
                }
                if (metaDataVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("meta-data version changed",
                            LogMessageKeys.OLD_VERSION, oldMetaDataVersion,
                            LogMessageKeys.NEW_VERSION, newMetaDataVersion,
                            subspaceProvider.logKey(), subspaceProvider));
                }
            }
        }

        dirty[0] = true;
        return checkRebuild(userVersionChecker, info, oldFormatVersion, metaData, oldMetaDataVersion);
    }

    private CompletableFuture<Void> checkRebuild(@Nullable UserVersionChecker userVersionChecker, @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                 int oldFormatVersion, @Nonnull RecordMetaData metaData, int oldMetaDataVersion) {
        final List<CompletableFuture<Void>> work = new LinkedList<>();

        if (oldFormatVersion != formatVersion) {
            info.setFormatVersion(formatVersion);
            // We must check whether we have to save unsplit records without a suffix before
            // attempting to read data, i.e., before we update any indexes.
            if ((oldFormatVersion >= MIN_FORMAT_VERSION
                    && oldFormatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION
                    && formatVersion >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION
                    && !metaData.isSplitLongRecords())) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("unsplit records stored at old format",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, formatVersion,
                            subspaceProvider.logKey(), subspaceProvider));
                }
                info.setOmitUnsplitRecordSuffix(true);
                omitUnsplitRecordSuffix = true;
            }
            if (oldFormatVersion >= MIN_FORMAT_VERSION && oldFormatVersion < SAVE_VERSION_WITH_RECORD_FORMAT_VERSION
                    && metaData.isStoreRecordVersions() && !useOldVersionFormat()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("migrating record versions to new format"),
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, formatVersion,
                            subspaceProvider.logKey(), subspaceProvider);
                }
                addConvertRecordVersions(work);
            }
        }

        final int newMetaDataVersion = metaData.getVersion();
        final boolean metaDataVersionChanged = oldMetaDataVersion != newMetaDataVersion;
        if (metaDataVersionChanged) {
            // Clear the version table if we are no longer storing record versions.
            if (!metaData.isStoreRecordVersions()) {
                final Transaction tr = ensureContextActive();
                tr.clear(getSubspace().subspace(Tuple.from(RECORD_VERSION_KEY)).range());
            }
            info.setMetaDataversion(newMetaDataVersion);
        }

        final boolean rebuildRecordCounts = checkPossiblyRebuildRecordCounts(metaData, info, work, oldFormatVersion);

        // Done if we just needed to update format version (which might trigger record count rebuild).
        if (!metaDataVersionChanged) {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenReady(work.get(0));
        }

        // Remove former indexes.
        for (FormerIndex formerIndex : metaData.getFormerIndexesSince(oldMetaDataVersion)) {
            removeFormerIndex(formerIndex);
        }

        return checkRebuildIndexes(userVersionChecker, info, oldFormatVersion, metaData, oldMetaDataVersion, rebuildRecordCounts, work);
    }

    private CompletableFuture<Void> checkRebuildIndexes(@Nullable UserVersionChecker userVersionChecker, @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                        int oldFormatVersion, @Nonnull RecordMetaData metaData, int oldMetaDataVersion,
                                                        boolean rebuildRecordCounts, List<CompletableFuture<Void>> work) {
        final boolean newStore = oldFormatVersion == 0;
        final Map<Index, List<RecordType>> indexes = metaData.getIndexesSince(oldMetaDataVersion);
        if (!indexes.isEmpty()) {
            CompletableFuture<Long> recordCountFuture = getRecordCountForRebuildIndexes(newStore, rebuildRecordCounts, indexes);
            return recordCountFuture.thenCompose(recordCount -> {
                if (recordCount == 0) {
                    // There are no records. Don't use the legacy split format.
                    if (formatVersion >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION
                            && omitUnsplitRecordSuffix
                            && (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled())) {
                        KeyValueLogMessage msg = KeyValueLogMessage.build("upgrading unsplit format on empty store",
                                "recordCount", recordCount,
                                "newFormatVersion", formatVersion,
                                subspaceProvider.logKey(), subspaceProvider);
                        if (newStore) {
                            LOGGER.debug(msg.toString());
                        } else {
                            LOGGER.info(msg.toString());
                        }
                    }
                    omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
                    info.clearOmitUnsplitRecordSuffix();
                }
                Map<Index, IndexState> newStates = getStatesForRebuildIndexes(userVersionChecker, indexes, recordCount, newStore, rebuildRecordCounts, oldMetaDataVersion, oldFormatVersion);
                return rebuildIndexes(indexes, newStates, work, newStore ? RebuildIndexReason.NEW_STORE : RebuildIndexReason.FEW_RECORDS, oldMetaDataVersion);
            });
        } else {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenAll(work);
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.EmptyCatchBlock")
    protected CompletableFuture<Long> getRecordCountForRebuildIndexes(boolean newStore, boolean rebuildRecordCounts,
                                                                      @Nonnull Map<Index, List<RecordType>> indexes) {
        if (recordStoreState == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> getRecordCountForRebuildIndexes(newStore, rebuildRecordCounts, indexes));
        }
        // Do this with the new indexes in write-only mode to avoid using one of them
        // when evaluating the snapshot record count.
        MutableRecordStoreState writeOnlyState = recordStoreState.withWriteOnlyIndexes(indexes.keySet().stream().map(Index::getName).collect(Collectors.toList()));
        // If all the new indexes are only for a record type whose primary key has a type prefix, then we can scan less.
        RecordType singleRecordTypeWithPrefixKey = singleRecordTypeWithPrefixKey(indexes);
        if (singleRecordTypeWithPrefixKey != null) {
            // Get a count for just those records, either from a COUNT index on just that type or from a universal COUNT index grouped by record type.
            MutableRecordStoreState saveState = recordStoreState;
            try {
                recordStoreState = writeOnlyState;
                return getSnapshotRecordCountForRecordType(singleRecordTypeWithPrefixKey.getName());
            } catch (RecordCoreException ex) {
                // No such index; have to use total record count.
            } finally {
                recordStoreState = saveState;
            }
        }
        if (!rebuildRecordCounts) {
            MutableRecordStoreState saveState = recordStoreState;
            try {
                recordStoreState = writeOnlyState;
                // TODO: FDBRecordStoreBase.checkPossiblyRebuild() could take a long time if the record count index is split into many groups (https://github.com/FoundationDB/fdb-record-layer/issues/7)
                return getSnapshotRecordCount();
            } catch (RecordCoreException ex) {
                // Probably this was from the lack of appropriate index on count; treat like rebuildRecordCounts = true.
            } finally {
                recordStoreState = saveState;
            }
        }
        // Do a scan (limited to a single record) to see if the store is empty.
        final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1)
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        final ScanProperties scanProperties = new ScanProperties(executeProperties);
        final RecordCursor<FDBStoredRecord<M>> records;
        if (singleRecordTypeWithPrefixKey == null) {
            records = scanRecords(null, scanProperties);
        } else {
            records = scanRecords(TupleRange.allOf(Tuple.from(singleRecordTypeWithPrefixKey.getRecordTypeKey())), null, scanProperties);
        }
        return records.onHasNext()
                .thenApply(hasAny -> {
                    if (hasAny) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(KeyValueLogMessage.of("version check scan found non-empty store",
                                    subspaceProvider.logKey(), subspaceProvider));
                        }
                        return Long.MAX_VALUE;
                    } else {
                        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                            KeyValueLogMessage msg = KeyValueLogMessage.build("version check scan found empty store",
                                    subspaceProvider.logKey(), subspaceProvider);
                            if (newStore) {
                                LOGGER.debug(msg.toString());
                            } else {
                                LOGGER.info(msg.toString());
                            }
                        }
                        return 0L;
                    }
                });
    }

    @Nullable
    protected RecordType singleRecordTypeWithPrefixKey(@Nonnull Map<Index, List<RecordType>> indexes) {
        RecordType recordType = null;
        for (List<RecordType> entry : indexes.values()) {
            Collection<RecordType> types = entry != null ? entry : getRecordMetaData().getRecordTypes().values();
            if (types.size() != 1) {
                return null;
            }
            RecordType type1 = entry != null ? entry.get(0) : types.iterator().next();
            if (recordType == null) {
                if (!type1.primaryKeyHasRecordTypePrefix()) {
                    return null;
                }
                recordType = type1;
            } else if (type1 != recordType) {
                return null;
            }
        }
        return recordType;
    }

    @Nonnull
    private void addConvertRecordVersions(@Nonnull List<CompletableFuture<Void>> work) {
        if (useOldVersionFormat()) {
            throw new RecordCoreException("attempted to convert record versions when still using older format");
        }
        final Subspace legacyVersionSubspace = getSubspace().subspace(Tuple.from(RECORD_VERSION_KEY));

        // Read all of the keys in the old record version location. For each
        // record, copy its version to the new location within the primary record
        // subspace. Then once they are all copied, delete the old subspace.
        KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(legacyVersionSubspace)
                .setContext(getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
        CompletableFuture<Void> workFuture = kvCursor.forEach(kv -> {
            final Tuple primaryKey = legacyVersionSubspace.unpack(kv.getKey());
            final FDBRecordVersion version = FDBRecordVersion.fromBytes(kv.getValue(), false);
            final byte[] newKeyBytes = getSubspace().pack(recordVersionKey(primaryKey));
            final byte[] newValueBytes = SplitHelper.packVersion(version);
            ensureContextActive().set(newKeyBytes, newValueBytes);
        }).thenAccept(ignore -> ensureContextActive().clear(legacyVersionSubspace.range()));
        work.add(workFuture);
    }

    @Nonnull
    protected Map<Index, IndexState> getStatesForRebuildIndexes(@Nullable UserVersionChecker userVersionChecker,
                                                                @Nonnull Map<Index, List<RecordType>> indexes, long recordCount,
                                                                boolean newStore, boolean rebuildRecordCounts, int oldMetaDataVersion,
                                                                int oldFormatVersion) {
        Map<Index, IndexState> newStates = new HashMap<>();
        for (Map.Entry<Index, List<RecordType>> entry : indexes.entrySet()) {
            Index index = entry.getKey();
            List<RecordType> recordTypes = entry.getValue();
            boolean indexOnNewRecordTypes = areAllRecordTypesSince(recordTypes, oldMetaDataVersion);
            IndexState state = userVersionChecker == null ?
                    writeOnlyIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes) :
                    userVersionChecker.needRebuildIndex(index, recordCount, indexOnNewRecordTypes);
            if (index.getType().equals(IndexTypes.VERSION)
                    && !newStore
                    && oldFormatVersion < SAVE_VERSION_WITH_RECORD_FORMAT_VERSION
                    && !useOldVersionFormat()
                    && state.equals(IndexState.READABLE)) {
                // Do not rebuild any version indexes while the format conversion is going on.
                // Otherwise, the process moving the versions might race against the index
                // build and some versions won't be indexed correctly.
                state = IndexState.WRITE_ONLY;
            }
            newStates.put(index, state);
        }
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("indexes need rebuilding",
                    "recordCount", recordCount == Long.MAX_VALUE ? "unknown" : Long.toString(recordCount),
                    subspaceProvider.logKey(), subspaceProvider);
            if (rebuildRecordCounts) {
                msg.addKeyAndValue("rebuildRecordCounts", "true");
            }
            Map<String, List<String>> stateNames = new HashMap<>();
            for (Map.Entry<Index, IndexState> stateEntry : newStates.entrySet()) {
                stateNames.compute(stateEntry.getValue().getLogName(), (key, names) -> {
                    if (names == null) {
                        names = new ArrayList<>();
                    }
                    names.add(stateEntry.getKey().getName());
                    return names;
                });
            }
            msg.addKeysAndValues(stateNames);
            if (newStore) {
                msg.addKeyAndValue("newStore", "true");
                LOGGER.debug(msg.toString());
            } else {
                LOGGER.info(msg.toString());
            }
        }
        return newStates;
    }

    static IndexState writeOnlyIfTooManyRecordsForRebuild(long recordCount, boolean indexOnNewRecordTypes) {
        if (indexOnNewRecordTypes || recordCount <= MAX_RECORDS_FOR_REBUILD) {
            return IndexState.READABLE;
        } else {
            return IndexState.WRITE_ONLY;
        }
    }

    // Clear the data associated with a given index. This is only safe to do if one is
    // either going to rebuild it or disable it. It is therefore package private.
    // TODO: Better to go through the index maintainer?
    void clearIndexData(@Nonnull Index index) {
        Transaction tr = ensureContextActive();
        tr.clear(Range.startsWith(indexSubspace(index).pack())); // startsWith to handle ungrouped aggregate indexes
        tr.clear(indexSecondarySubspace(index).range());
        tr.clear(indexRangeSubspace(index).range());
        tr.clear(indexUniquenessViolationsSubspace(index).range());
    }

    public void removeFormerIndex(FormerIndex formerIndex) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("removing index",
                                           subspaceProvider.logKey(), subspaceProvider,
                                           LogMessageKeys.SUBSPACE_KEY, formerIndex.getSubspaceKey()));
        }

        final long startTime = System.nanoTime();
        Transaction tr = ensureContextActive();
        tr.clear(getSubspace().range(Tuple.from(INDEX_KEY, formerIndex.getSubspaceKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY, formerIndex.getSubspaceKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY, formerIndex.getSubspaceKey())));
        tr.clear(getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, formerIndex.getSubspaceKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY, formerIndex.getSubspaceKey())));
        if (getTimer() != null) {
            getTimer().recordSinceNanoTime(FDBStoreTimer.Events.REMOVE_FORMER_INDEX, startTime);
        }
    }

    protected boolean checkPossiblyRebuildRecordCounts(@Nonnull RecordMetaData metaData,
                                                       @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                       @Nonnull List<CompletableFuture<Void>> work,
                                                       int oldFormatVersion) {
        RecordMetaDataProto.KeyExpression countKeyExpression = null;
        if (metaData.getRecordCountKey() != null) {
            try {
                countKeyExpression = metaData.getRecordCountKey().toKeyExpression();
            } catch (KeyExpression.SerializationException e) {
                throw new RecordCoreException("Error converting count key expression to protobuf", e);
            }
        }
        boolean rebuildRecordCounts =
                (oldFormatVersion > 0 && oldFormatVersion < RECORD_COUNT_ADDED_FORMAT_VERSION) ||
                        (countKeyExpression != null && formatVersion >= RECORD_COUNT_KEY_ADDED_FORMAT_VERSION &&
                                (!info.hasRecordCountKey() || !info.getRecordCountKey().equals(countKeyExpression)));
        if (rebuildRecordCounts) {
            // We want to clear all record counts.
            final Transaction tr = ensureContextActive();
            tr.clear(getSubspace().range(Tuple.from(RECORD_COUNT_KEY)));

            // Set the new record count key if we have one.
            if (formatVersion >= RECORD_COUNT_KEY_ADDED_FORMAT_VERSION) {
                if (countKeyExpression != null) {
                    info.setRecordCountKey(countKeyExpression);
                } else {
                    info.clearRecordCountKey();
                }
            }

            // Add the record rebuild job.
            addRebuildRecordCountsJob(work);
        }
        return rebuildRecordCounts;
    }

    public void addRebuildRecordCountsJob(List<CompletableFuture<Void>> work) {
        final KeyExpression recordCountKey = getRecordMetaData().getRecordCountKey();
        if (recordCountKey == null) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("recounting all records",
                                           subspaceProvider.logKey(), subspaceProvider));
        }
        final Map<Key.Evaluated, Long> counts = new HashMap<>();
        final RecordCursor<FDBStoredRecord<M>> records = scanRecords(null, ScanProperties.FORWARD_SCAN);
        CompletableFuture<Void> future = records.forEach(record -> {
            Key.Evaluated subkey = recordCountKey.evaluateSingleton(emptyEvaluationContext(), record);
            counts.compute(subkey, (k, v) -> (v == null) ? 1 : v + 1);
        }).thenApply(vignore -> {
            final Transaction tr = ensureContextActive();
            final byte[] bytes = new byte[8];
            final ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            for (Map.Entry<Key.Evaluated, Long> entry : counts.entrySet()) {
                buf.putLong(entry.getValue());
                tr.set(getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(entry.getKey().toTupleAppropriateList())),
                        bytes);
                buf.clear();
            }
            return null;
        });
        future = context.instrument(FDBStoreTimer.Events.RECOUNT_RECORDS, future);
        work.add(future);
    }

    /**
     * Validate the current meta-data for this store.
     */
    public void validateMetaData() {
        final MetaDataValidator validator = new MetaDataValidator(metaDataProvider, indexMaintainerRegistry) {
                @Override
                protected void validateIndex(@Nonnull Index index) {
                    super.validateIndex(index);
                    getIndexMaintainer(index).validate();
                }
        };
        validator.validate();
    }

    /**
     * Get an empty evaluation context associated with this store. 
     * @return a new evaluation context initialized with empty bindings
     */
    @Nonnull
    public FDBEvaluationContext<M> emptyEvaluationContext() {
        return createEvaluationContext(Bindings.EMPTY_BINDINGS);
    }

    /**
     * Get an evaluation context associated with this store for the given bindings. 
     * @param bindings value bindings
     * @return a new evaluation context initialized with the given bindings
     */
    @Nonnull
    public FDBEvaluationContext<M> createEvaluationContext(@Nonnull Bindings bindings) {
        return new FDBRecordStoreEvaluationContext<>(this, bindings);
    }

    protected static class FDBRecordStoreEvaluationContext<M extends Message> extends FDBEvaluationContext<M> {
        @Nonnull
        protected final FDBRecordStoreBase<M> store;

        public FDBRecordStoreEvaluationContext(@Nonnull FDBRecordStoreBase<M> store, @Nonnull Bindings bindings) {
            super(bindings);
            this.store = store;
        }

        @Override
        public FDBRecordStoreBase<M> getStore() {
            return store;
        }

        @Override
        @Nonnull
        protected FDBEvaluationContext<M> withBindings(@Nonnull Bindings newBindings) {
            return new FDBRecordStoreEvaluationContext<>(store, newBindings);
        }
    }    

    /**
     * Function for computing the number of elements to allow in the asynchronous pipeline for an operation of the given
     * type.
     */
    public static interface PipelineSizer {
        public int getPipelineSize(@Nonnull PipelineOperation pipelineOperation);
    }

    public static final int DEFAULT_PIPELINE_SIZE = 10;
    public static final PipelineSizer DEFAULT_PIPELINE_SIZER = pipelineOperation -> DEFAULT_PIPELINE_SIZE;

    public PipelineSizer getPipelineSizer() {
        return pipelineSizer;
    }

    public int getPipelineSize(@Nonnull PipelineOperation pipelineOperation) {
        return pipelineSizer.getPipelineSize(pipelineOperation);
    }

    protected void addRecordCount(@Nonnull RecordMetaData metaData, @Nonnull FDBStoredRecord<M> record, @Nonnull byte[] increment) {
        if (metaData.getRecordCountKey() == null) {
            return;
        }
        final Transaction tr = ensureContextActive();
        Key.Evaluated subkey = metaData.getRecordCountKey().evaluateSingleton(emptyEvaluationContext(), record);
        final byte[] keyBytes = getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(subkey.toTupleAppropriateList()));
        tr.mutate(MutationType.ADD, keyBytes, increment);
    }

    /**
     * Get the number of records in the record store.
     *
     * There must be a suitable {@code COUNT} type index defined.
     * @return a future that will complete to the number of records in the store
     */
    public CompletableFuture<Long> getSnapshotRecordCount() {
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
    public CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value) {
        if (getRecordMetaData().getRecordCountKey() != null) {
            if (key.getColumnSize() != value.size()) {
                throw new RecordCoreException("key and value are not the same size");
            }
            final ReadTransaction tr = context.readTransaction(true);
            final Tuple subkey = Tuple.from(RECORD_COUNT_KEY).addAll(value.toTupleAppropriateList());
            if (getRecordMetaData().getRecordCountKey().equals(key)) {
                return tr.get(getSubspace().pack(subkey)).thenApply(FDBRecordStoreBase::decodeRecordCount);
            } else if (key.isPrefixKey(getRecordMetaData().getRecordCountKey())) {
                AsyncIterable<KeyValue> kvs = tr.getRange(getSubspace().range(Tuple.from(RECORD_COUNT_KEY)));
                return MoreAsyncUtil.reduce(getExecutor(), kvs.iterator(), 0L, (count, kv) -> count + decodeRecordCount(kv.getValue()));
            }
        }
        return evaluateAggregateFunction(Collections.emptyList(), IndexFunctionHelper.count(key), value, IsolationLevel.SNAPSHOT)
                .thenApply(tuple -> tuple.getLong(0));
    }

    /**
     * Get the number of records in the record store of the given record type.
     *
     * The record type must have a {@code COUNT} index defined for it.
     * @param recordTypeName record type for which to count records
     * @return a future that will complete to the number of records
     */
    public CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName) {
        // A COUNT index on this record type.
        IndexAggregateFunction aggregateFunction = IndexFunctionHelper.count(EmptyKeyExpression.EMPTY);
        Optional<IndexMaintainer<M>> indexMaintainer = IndexFunctionHelper.indexMaintainerForAggregateFunction(this, aggregateFunction, Collections.singletonList(recordTypeName));
        if (indexMaintainer.isPresent()) {
            return indexMaintainer.get().evaluateAggregateFunction(aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT)
                    .thenApply(tuple -> tuple.getLong(0));
        }
        // A universal COUNT index by record type.
        // In fact, any COUNT index by record type that applied to this record type would work, no matter what other
        // types it applied to.
        aggregateFunction = IndexFunctionHelper.count(Key.Expressions.recordType());
        indexMaintainer = IndexFunctionHelper.indexMaintainerForAggregateFunction(this, aggregateFunction, Collections.emptyList());
        if (indexMaintainer.isPresent()) {
            RecordType recordType = getRecordMetaData().getRecordType(recordTypeName);
            return indexMaintainer.get().evaluateAggregateFunction(aggregateFunction, TupleRange.allOf(Tuple.from(recordType.getRecordTypeKey())), IsolationLevel.SNAPSHOT)
                    .thenApply(tuple -> tuple.getLong(0));
        }
        throw new RecordCoreException("Require a COUNT index on " + recordTypeName);
    }

    public static long decodeRecordCount(@Nullable byte[] bytes) {
        return bytes == null ? 0 : ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public CompletableFuture<Long> getSnapshotRecordUpdateCount() {
        return getSnapshotRecordUpdateCount(EmptyKeyExpression.EMPTY, Key.Evaluated.EMPTY);
    }

    public CompletableFuture<Long> getSnapshotRecordUpdateCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value) {
        return evaluateAggregateFunction(Collections.emptyList(), IndexFunctionHelper.countUpdates(key), value, IsolationLevel.SNAPSHOT)
                .thenApply(tuple -> tuple.getLong(0));
    }

    /**
     * Evaluate a {@link RecordFunction} against a record.
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    public <T> CompletableFuture<T> evaluateRecordFunction(@Nonnull RecordFunction<T> function,
                                                           @Nonnull FDBRecord<M> record) {
        return evaluateRecordFunction(emptyEvaluationContext(), function, record);
    }

    /**
     * Evaluate a {@link RecordFunction} against a record.
     * @param evaluationContext evaluation context containing parameter bindings
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    public <T> CompletableFuture<T> evaluateRecordFunction(@Nonnull FDBEvaluationContext<M> evaluationContext,
                                                           @Nonnull RecordFunction<T> function,
                                                           @Nonnull FDBRecord<M> record) {
        if (function instanceof IndexRecordFunction<?>) {
            IndexRecordFunction<T> indexRecordFunction = (IndexRecordFunction<T>)function;
            return IndexFunctionHelper.indexMaintainerForRecordFunction(this, indexRecordFunction, record)
                    .orElseThrow(() -> new RecordCoreException("Record function " + function +
                                                               " requires appropriate index on " + record.getRecordType().getName()))
                    .evaluateRecordFunction(evaluationContext, indexRecordFunction, record);
        } else if (function instanceof StoreRecordFunction<?>) {
            StoreRecordFunction<T> storeRecordFunction = (StoreRecordFunction<T>)function;
            return evaluateStoreFunction(evaluationContext, storeRecordFunction, record);
        }
        throw new RecordCoreException("Cannot evaluate record function " + function);
    }

    /**
     * Evaluate a {@link StoreRecordFunction} against a record.
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull StoreRecordFunction<T> function,
                                                          @Nonnull FDBRecord<M> record) {
        return evaluateStoreFunction(emptyEvaluationContext(), function, record);
    }

    /**
     * Evaluate a {@link StoreRecordFunction} against a record.
     * @param evaluationContext evaluation context containing parameter bindings
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @param <T> the type of the result
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull FDBEvaluationContext<M> evaluationContext,
                                                          @Nonnull StoreRecordFunction<T> function,
                                                          @Nonnull FDBRecord<M> record) {
        if (function.getName().equals(FunctionNames.VERSION)) {
            if (record.hasVersion() && record.getVersion().isComplete()) {
                return CompletableFuture.completedFuture((T) record.getVersion());
            }
            return (CompletableFuture<T>) loadRecordVersionAsync(record.getPrimaryKey()).orElse(CompletableFuture.completedFuture(null));
        } else {
            throw new RecordCoreException("Unknown store function " + function.getName());
        }
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     * @param evaluationContext evaluation context containing parameter bindings
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @param isolationLevel whether to use snapshot reads
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull FDBEvaluationContext<M> evaluationContext,
                                                              @Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction,
                aggregateFunction.adjustRange(evaluationContext, range), isolationLevel);
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
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return IndexFunctionHelper.indexMaintainerForAggregateFunction(this, aggregateFunction, recordTypeNames)
                .orElseThrow(() -> new RecordCoreException("Aggregate function " + aggregateFunction + " requires appropriate index"))
                .evaluateAggregateFunction(aggregateFunction, range, isolationLevel);
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
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull Key.Evaluated value,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction, TupleRange.allOf(value.toTuple()), isolationLevel);
    }

    /**
     * Get a query result record from a stored record.
     * This is from a direct record scan / lookup without an associated index.
     * @param storedRecord the stored record to convert to a queried record
     * @return a {@link FDBQueriedRecord} corresponding to {@code storedRecord}
     */
    @Nonnull
    public FDBQueriedRecord<M> queriedRecord(@Nonnull FDBStoredRecord<M> storedRecord) {
        return FDBQueriedRecord.stored(storedRecord);
    }

    /**
     * Get a query result record from an indexed record.
     * This is from an index scan and permits access to the underlying index entry.
     * @param indexedRecord the indexed record to convert to a queried record
     * @return a {@link FDBQueriedRecord} corresponding to {@code indexedRecord}
     */
    @Nonnull
    public FDBQueriedRecord<M> queriedRecord(@Nonnull FDBIndexedRecord<M> indexedRecord) {
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
    public FDBQueriedRecord<M> coveredIndexQueriedRecord(@Nonnull Index index, @Nonnull IndexEntry indexEntry, @Nonnull RecordType recordType, @Nonnull M partialRecord, boolean hasPrimaryKey) {
        return FDBQueriedRecord.covered(index, indexEntry,
                hasPrimaryKey ? indexEntryPrimaryKey(index, indexEntry.getKey()) : TupleHelpers.EMPTY,
                recordType, partialRecord);
    }

    /**
     * Plan and execute a query.
     * @param query the query to plan and execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQuery query) {
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
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQuery query,
                                                          @Nullable byte[] continuation,
                                                          @Nonnull ExecuteProperties executeProperties) {
        return executeQuery(planQuery(query), continuation, executeProperties);
    }

    /**
     * Execute a query.
     * @param query the query to execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQueryPlan query) {
        return query.execute(emptyEvaluationContext());
    }

    /**
     * Execute a query.
     * @param query the query to execute
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQueryPlan query,
                                                          @Nullable byte[] continuation,
                                                          @Nonnull ExecuteProperties executeProperties) {
        return query.execute(emptyEvaluationContext(), continuation, executeProperties);
    }

    /**
     * Plan a query.
     * @param query the query to plan
     * @return a query plan
     * @see RecordQueryPlanner#plan
     */
    @Nonnull
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query) {
        final RecordQueryPlanner planner = new RecordQueryPlanner(getRecordMetaData(), getRecordStoreState());
        return planner.plan(query);
    }
}
