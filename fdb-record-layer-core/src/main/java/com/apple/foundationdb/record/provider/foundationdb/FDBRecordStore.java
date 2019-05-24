/*
 * FDBRecordStore.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ByteScanLimiter;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.MutableRecordStoreState;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
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
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
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
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A multi-type record store.
 *
 * By default, this uses Protobuf dynamic messages to process records. However, one can specify a custom {@link RecordSerializer}
 * such as a {@link com.apple.foundationdb.record.provider.common.MessageBuilderRecordSerializer MessageBuilderRecordSerializer}
 * or a {@link com.apple.foundationdb.record.provider.common.TransformedRecordSerializer TransformedRecordSerializer} to use
 * as an alternative. Unlike the serializers used by an {@link FDBTypedRecordStore} which only need to be able to process records
 * of the appropriate record type, the provided serializer must be able to serialize and deseralize all record types specified by
 * the record store's {@link RecordMetaData}.
 *
 * <p>
 * <b>Warning</b>: It is unsafe to create and use two {@code FDBRecordStore}s concurrently over the same {@link Subspace}
 * within the context of a single transaction, i.e., with the same {@link FDBRecordContext}. This is because the {@code FDBRecordStore}
 * object maintains state about certain uncommitted operations, and concurrent access through two objects will not see
 * changes to this in-memory state. See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/489">Issue #489</a>
 * for more details. Note also that the record stores returned by {@link #getTypedRecordStore(RecordSerializer)} and
 * {@link #getUntypedRecordStore()} will share an {@code FDBRecordStore} with the record store on which they are called,
 * so it <em>is</em> safe to have a typed- and untyped-record store open over the same {@code Subspace} within the context
 * of the same transaction if one uses one of those methods.
 * </p>
 *
 * @see FDBRecordStoreBase
 */
@API(API.Status.STABLE)
public class FDBRecordStore extends FDBStoreBase implements FDBRecordStoreBase<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStore.class);

    public static final int DEFAULT_PIPELINE_SIZE = 10;
    public static final PipelineSizer DEFAULT_PIPELINE_SIZER = pipelineOperation -> DEFAULT_PIPELINE_SIZE;

    // The maximum number of records to allow before triggering online index builds
    // instead of a transactional rebuild.
    public static final int MAX_RECORDS_FOR_REBUILD = 200;

    // The maximum number of index rebuilds to run in parellel
    // TODO: This should probably be configured through the PipelineSizer
    public static final int MAX_PARALLEL_INDEX_REBUILD = 10;

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

    // The size of preload cache
    private static final int PRELOAD_CACHE_SIZE = 100;

    protected static final Object STORE_INFO_KEY = FDBRecordStoreKeyspace.STORE_INFO.key();
    protected static final Object RECORD_KEY = FDBRecordStoreKeyspace.RECORD.key();
    protected static final Object INDEX_KEY = FDBRecordStoreKeyspace.INDEX.key();
    protected static final Object INDEX_SECONDARY_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE.key();
    protected static final Object RECORD_COUNT_KEY = FDBRecordStoreKeyspace.RECORD_COUNT.key();
    protected static final Object INDEX_STATE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_STATE_SPACE.key();
    protected static final Object INDEX_RANGE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_RANGE_SPACE.key();
    protected static final Object INDEX_UNIQUENESS_VIOLATIONS_KEY = FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE.key();
    protected static final Object RECORD_VERSION_KEY = FDBRecordStoreKeyspace.RECORD_VERSION_SPACE.key();

    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_ONE = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };
    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_MINUS_ONE = new byte[] { -1, -1, -1, -1, -1, -1, -1, -1 };

    protected int formatVersion;
    protected int userVersion;

    private boolean omitUnsplitRecordSuffix;

    @Nonnull
    protected final RecordMetaDataProvider metaDataProvider;

    @Nullable
    protected MutableRecordStoreState recordStoreState;

    @Nonnull
    protected final RecordSerializer<Message> serializer;

    @Nonnull
    protected final IndexMaintainerRegistry indexMaintainerRegistry;

    @Nonnull
    protected final IndexMaintenanceFilter indexMaintenanceFilter;

    @Nonnull
    protected final PipelineSizer pipelineSizer;

    @Nullable
    protected final FDBRecordStoreStateCache storeStateCache;

    @Nullable
    private Subspace cachedRecordsSubspace;

    @Nonnull
    private final Cache<Tuple, FDBRawRecord> preloadCache;

    @SuppressWarnings("squid:S00107")
    protected FDBRecordStore(@Nonnull FDBRecordContext context,
                             @Nonnull SubspaceProvider subspaceProvider,
                             int formatVersion,
                             @Nonnull RecordMetaDataProvider metaDataProvider,
                             @Nonnull RecordSerializer<Message> serializer,
                             @Nonnull IndexMaintainerRegistry indexMaintainerRegistry,
                             @Nonnull IndexMaintenanceFilter indexMaintenanceFilter,
                             @Nonnull PipelineSizer pipelineSizer,
                             @Nullable FDBRecordStoreStateCache storeStateCache) {
        super(context, subspaceProvider);
        this.formatVersion = formatVersion;
        this.metaDataProvider = metaDataProvider;
        this.serializer = serializer;
        this.indexMaintainerRegistry = indexMaintainerRegistry;
        this.indexMaintenanceFilter = indexMaintenanceFilter;
        this.pipelineSizer = pipelineSizer;
        this.storeStateCache = storeStateCache;
        this.omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
        this.preloadCache = CacheBuilder.newBuilder().maximumSize(PRELOAD_CACHE_SIZE).build();
    }

    @Override
    public FDBRecordStore getUntypedRecordStore() {
        return this;
    }

    @Nonnull
    @Override
    public FDBRecordContext getContext() {
        return context;
    }

    /**
     * Get the storage format version currently in use for this record store.
     *
     * After calling {@link FDBRecordStore.Builder#open} or {@link #checkVersion} directly, this will be the format stored in the store's info header.
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
     * After calling {@link FDBRecordStore.Builder#open} or {@link #checkVersion} directly, this will be the value stored in the store's info header.
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

    /**
     * Get the provider for the record store's meta-data.
     * @return the meta-data source to use
     */
    @Nullable
    public RecordMetaDataProvider getMetaDataProvider() {
        return metaDataProvider;
    }

    /**
     * Get the {@link RecordMetaData} used by this store.
     * @return the associated meta-data
     */
    @Override
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
            recordStoreState = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_STORE_STATE,
                    preloadSubspaceAsync().thenCompose(vignore -> loadRecordStoreStateAsync(StoreExistenceCheck.NONE).thenApply(RecordStoreState::toMutable)));
        }
        return recordStoreState;
    }

    @Override
    @Nonnull
    public RecordSerializer<Message> getSerializer() {
        return serializer;
    }

    @Nonnull
    public IndexMaintainerRegistry getIndexMaintainerRegistry() {
        return indexMaintainerRegistry;
    }

    @Nonnull
    public IndexMaintenanceFilter getIndexMaintenanceFilter() {
        return indexMaintenanceFilter;
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param record the record to save
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    @Override
    @Nonnull
    public CompletableFuture<FDBStoredRecord<Message>> saveRecordAsync(@Nonnull final Message record, @Nonnull RecordExistenceCheck existenceCheck,
                                                                       @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveTypedRecord(serializer, record, existenceCheck, version, behavior);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    protected <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                        @Nonnull M record,
                                                                                        @Nonnull RecordExistenceCheck existenceCheck,
                                                                                        @Nullable FDBRecordVersion version,
                                                                                        @Nonnull VersionstampSaveBehavior behavior) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Descriptors.Descriptor recordDescriptor = record.getDescriptorForType();
        final RecordType recordType = metaData.getRecordTypeForDescriptor(recordDescriptor);
        final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();

        final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(record).setRecordType(recordType);
        final FDBRecordVersion recordVersion = recordVersionForSave(metaData, version, behavior);
        recordBuilder.setVersion(recordVersion);
        final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(recordBuilder).toTuple();
        recordBuilder.setPrimaryKey(primaryKey);

        final CompletableFuture<FDBStoredRecord<M>> result = loadExistingRecord(typedSerializer, primaryKey).thenCompose(oldRecord -> {
            if (oldRecord == null) {
                if (existenceCheck.errorIfNotExists()) {
                    throw new RecordDoesNotExistException("record does not exist",
                            LogMessageKeys.PRIMARY_KEY, primaryKey);
                }
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
            }
            final FDBStoredRecord<M> newRecord = serializeAndSaveRecord(typedSerializer, recordBuilder, metaData, oldRecord);
            if (oldRecord == null) {
                addRecordCount(metaData, newRecord, LITTLE_ENDIAN_INT64_ONE);
            } else {
                if (getTimer() != null) {
                    getTimer().increment(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES, oldRecord.getValueSize());
                }
            }
            return updateSecondaryIndexes(oldRecord, newRecord).thenApply(v -> newRecord);
        });
        return context.instrument(FDBStoreTimer.Events.SAVE_RECORD, result);
    }

    private <M extends Message> void addRecordCount(@Nonnull RecordMetaData metaData, @Nonnull FDBStoredRecord<M> record, @Nonnull byte[] increment) {
        if (metaData.getRecordCountKey() == null) {
            return;
        }
        final Transaction tr = ensureContextActive();
        Key.Evaluated subkey = metaData.getRecordCountKey().evaluateSingleton(record);
        final byte[] keyBytes = getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(subkey.toTupleAppropriateList()));
        tr.mutate(MutationType.ADD, keyBytes, increment);
    }

    @Nullable
    private FDBRecordVersion recordVersionForSave(@Nonnull RecordMetaData metaData, @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
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

    @Nonnull
    private <M extends Message> CompletableFuture<FDBStoredRecord<M>> loadExistingRecord(@Nonnull RecordSerializer<M> typedSerializer, @Nonnull Tuple primaryKey) {
        // Note: this assumes that any existing record is compatible with the serializer (even if not of the same record type).
        // To relax that would perhaps mean catching errors and falling back to the untyped serializer.
        // This would in turn require care with the type parameters to updateSecondaryIndexes.
        // In no case is an index maintainer called with incompatible record type, so its signature should still be valid.
        return loadTypedRecord(typedSerializer, primaryKey, false);
    }

    @Nonnull
    private <M extends Message> FDBStoredRecord<M> serializeAndSaveRecord(@Nonnull RecordSerializer<M> typedSerializer, @Nonnull final FDBStoredRecordBuilder<M> recordBuilder,
                                                                          @Nonnull final RecordMetaData metaData, @Nullable FDBStoredSizes oldSizeInfo) {
        final Tuple primaryKey = recordBuilder.getPrimaryKey();
        final FDBRecordVersion version = recordBuilder.getVersion();
        final byte[] serialized = typedSerializer.serialize(metaData, recordBuilder.getRecordType(), recordBuilder.getRecord(), getTimer());
        final FDBRecordVersion splitVersion = useOldVersionFormat() ? null : version;
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        preloadCache.invalidate(primaryKey); // clear out cache of older value if present
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

    @Nonnull
    private Tuple recordVersionKey(@Nonnull Tuple primaryKey) {
        if (useOldVersionFormat()) {
            return Tuple.from(RECORD_VERSION_KEY).addAll(primaryKey);
        } else {
            return Tuple.from(RECORD_KEY).addAll(primaryKey).add(SplitHelper.RECORD_VERSION);
        }
    }

    @Nonnull
    private <M extends Message> CompletableFuture<Void> updateSecondaryIndexes(@Nullable final FDBStoredRecord<M> oldRecord,
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

    private <M extends Message> void updateSecondaryIndexes(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                            @Nullable final FDBIndexableRecord<M> newRecord,
                                                            @Nonnull final List<CompletableFuture<Void>> futures,
                                                            @Nonnull final List<Index> indexes) {
        if (oldRecord == null && newRecord == null) {
            return;
        }
        for (Index index : indexes) {
            final IndexMaintainer maintainer = getIndexMaintainer(index);
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
        return getSubspace().subspace(Tuple.from(INDEX_KEY, index.getSubspaceTupleKey()));
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
        return getSubspace().subspace(Tuple.from(INDEX_SECONDARY_SPACE_KEY, index.getSubspaceTupleKey()));
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
        return getSubspace().subspace(Tuple.from(INDEX_RANGE_SPACE_KEY, index.getSubspaceTupleKey()));
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
        return getSubspace().subspace(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY, index.getSubspaceTupleKey()));
    }

    /**
     * Get the maintainer for a given index.
     * @param index the required index
     * @return the maintainer for the given index
     */
    public IndexMaintainer getIndexMaintainer(@Nonnull Index index) {
        return indexMaintainerRegistry.getIndexMaintainer(new IndexMaintainerState(this, index, indexMaintenanceFilter));
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
        final IndexMaintainer indexMaintainer = getIndexMaintainer(index);
        final CompletableFuture<Void> checker = context.instrument(FDBStoreTimer.Events.CHECK_INDEX_UNIQUENESS,
                AsyncUtil.forEach(kvs, kv -> {
                    Tuple existingEntry = SplitHelper.unpackKey(indexMaintainer.getIndexSubspace(), kv);
                    Tuple existingKey = index.getEntryPrimaryKey(existingEntry);
                    if (!TupleHelpers.equals(primaryKey, existingKey)) {
                        if (isIndexWriteOnly(index)) {
                            Tuple valueKey = indexEntry.getKey();
                            indexMaintainer.updateUniquenessViolations(valueKey, primaryKey, existingKey, false);
                            indexMaintainer.updateUniquenessViolations(valueKey, existingKey, primaryKey, false);
                        } else {
                            throw new RecordIndexUniquenessViolation(index, indexEntry, primaryKey, existingKey);
                        }
                    }
                }, getExecutor()));
        getRecordContext().addCommitCheck(checker);
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

    @Override
    @Nonnull
    public CompletableFuture<FDBStoredRecord<Message>> loadRecordInternal(@Nonnull final Tuple primaryKey,
                                                                          @Nonnull ExecuteState executeState,
                                                                          final boolean snapshot) {
        return loadTypedRecord(serializer, primaryKey, executeState, snapshot);
    }

    @Nonnull
    protected <M extends Message> CompletableFuture<FDBStoredRecord<M>> loadTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                        @Nonnull final Tuple primaryKey,
                                                                                        final boolean snapshot) {
        return loadTypedRecord(typedSerializer, primaryKey, ExecuteState.NO_LIMITS, snapshot);
    }

    @Nonnull
    protected <M extends Message> CompletableFuture<FDBStoredRecord<M>> loadTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                        @Nonnull final Tuple primaryKey,
                                                                                        @Nonnull ExecuteState executeState,
                                                                                        final boolean snapshot) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();

        final Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional;
        if (useOldVersionFormat()) {
            versionFutureOptional = loadRecordVersionAsync(primaryKey);
        } else {
            versionFutureOptional = Optional.empty();
        }

        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        CompletableFuture<FDBStoredRecord<M>> result = loadRawRecordAsync(primaryKey, sizeInfo, snapshot)
                .thenCompose(rawRecord -> {
                    final ByteScanLimiter byteScanLimiter = executeState.getByteScanLimiter();
                    if (byteScanLimiter != null) {
                        byteScanLimiter.registerScannedBytes(sizeInfo.getKeySize() + sizeInfo.getValueSize());
                    }
                    return rawRecord == null ? CompletableFuture.completedFuture(null) :
                            deserializeRecord(typedSerializer, rawRecord, metaData, versionFutureOptional);
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

    private <M extends Message> CompletableFuture<FDBStoredRecord<M>> deserializeRecord(@Nonnull RecordSerializer<M> typedSerializer, @Nonnull final FDBRawRecord rawRecord,
                                                                                        @Nonnull final RecordMetaData metaData,
                                                                                        @Nonnull final Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional) {
        final Tuple primaryKey = rawRecord.getPrimaryKey();
        final byte[] serialized = rawRecord.getRawRecord();

        try {
            final M record = typedSerializer.deserialize(metaData, primaryKey, rawRecord.getRawRecord(), getTimer());
            final RecordType recordType = metaData.getRecordTypeForDescriptor(record.getDescriptorForType());
            countKeysAndValues(FDBStoreTimer.Counts.LOAD_RECORD_KEY, FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES, FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES,
                    rawRecord);

            final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(record)
                    .setPrimaryKey(primaryKey).setRecordType(recordType).setSize(rawRecord);

            if (rawRecord.hasVersion()) {
                // In the current format version, the version should be read along with the version,
                // so this should be hit the majority of the time.
                recordBuilder.setVersion(rawRecord.getVersion());
                return CompletableFuture.completedFuture(recordBuilder.build());
            } else if (versionFutureOptional.isPresent()) {
                // In an old format version, the record version was stored separately and requires
                // another read (which has hopefully happened in parallel with the main record read in the background).
                return versionFutureOptional.get().thenApply(version -> {
                    recordBuilder.setVersion(version);
                    return recordBuilder.build();
                });
            } else {
                // Look for the version in the various places that it might be. If we can't find it, then
                // this will return an FDBStoredRecord where the version is unset.
                return CompletableFuture.completedFuture(recordBuilder.build());
            }
        } catch (Exception ex) {
            final LoggableException ex2 = new RecordCoreException("Failed to deserialize record", ex);
            ex2.addLogInfo(
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
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

    @Override
    @Nonnull
    public CompletableFuture<Void> preloadRecordAsync(@Nonnull final Tuple primaryKey) {
        return loadRawRecordAsync(primaryKey, null, false).thenAccept(fdbRawRecord -> {
            if (fdbRawRecord != null) {
                preloadCache.put(primaryKey, fdbRawRecord);
            }
        });
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> recordExistsAsync(@Nonnull final Tuple primaryKey, @Nonnull final IsolationLevel isolationLevel) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final ReadTransaction tr = isolationLevel.isSnapshot() ? ensureContextActive().snapshot() : ensureContextActive();
        return SplitHelper.keyExists(tr, context, recordsSubspace(),
                primaryKey, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix);
    }

    /**
     * Asynchronously read a record from the database.
     * @param primaryKey the key for the record to be loaded
     * @param sizeInfo a size info to fill in from serializer
     * @param snapshot whether to snapshot read
     * @return a CompletableFuture that will return a message or null if there was no record with that key
     */
    @Nonnull
    private CompletableFuture<FDBRawRecord> loadRawRecordAsync(@Nonnull final Tuple primaryKey,
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

    @Override
    @Nonnull
    public RecordCursor<FDBStoredRecord<Message>> scanRecords(@Nullable final Tuple low, @Nullable final Tuple high,
                                                              @Nonnull final EndpointType lowEndpoint, @Nonnull final EndpointType highEndpoint,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull ScanProperties scanProperties) {
        return scanTypedRecords(serializer, low, high, lowEndpoint, highEndpoint, continuation, scanProperties);
    }

    @Nonnull
    public <M extends Message> RecordCursor<FDBStoredRecord<M>> scanTypedRecords(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                 @Nullable final Tuple low, @Nullable final Tuple high,
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
            return deserializeRecord(typedSerializer, rawRecord, metaData, versionFutureOptional);
        }, pipelineSizer.getPipelineSize(PipelineOperation.KEY_TO_RECORD));
        return context.instrument(FDBStoreTimer.Events.SCAN_RECORDS, result);
    }

    @Override
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

    @Override
    @Nonnull
    public RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanType scanType,
                                              @Nonnull TupleRange range,
                                              @Nullable byte[] continuation,
                                              @Nonnull ScanProperties scanProperties) {
        if (!isIndexReadable(index)) {
            throw new RecordCoreException("Cannot scan non-readable index " + index.getName());
        }
        RecordCursor<IndexEntry> result = getIndexMaintainer(index)
                .scan(scanType, range, continuation, scanProperties);
        return context.instrument(FDBStoreTimer.Events.SCAN_INDEX_KEYS, result);
    }

    @Override
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
            return new RecordIndexUniquenessViolation(index, new IndexEntry(index, valueKey, entry.getValue()), primaryKey, existingKey);
        });
    }

    @Override
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

    @Override
    @Nonnull
    public CompletableFuture<Boolean> deleteRecordAsync(@Nonnull final Tuple primaryKey) {
        return deleteTypedRecord(serializer, primaryKey);
    }

    @Nonnull
    protected <M extends Message> CompletableFuture<Boolean> deleteTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                               @Nonnull Tuple primaryKey) {
        preloadCache.invalidate(primaryKey);
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        CompletableFuture<Boolean> result = loadTypedRecord(typedSerializer, primaryKey, false).thenCompose(oldRecord -> {
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

    public static void deleteStore(FDBRecordContext context, KeySpacePath path) {
        final Subspace subspace = new Subspace(path.toTuple(context));
        deleteStore(context, subspace);
    }

    public static void deleteStore(FDBRecordContext context, Subspace subspace) {
        final Transaction transaction = context.ensureActive();
        transaction.clear(subspace.range());
        context.setDirtyStoreState(true);
    }

    @Override
    public void deleteAllRecords() {
        preloadCache.invalidateAll();
        Transaction tr = ensureContextActive();

        // Clear out all data except for the store header key and the index state space.
        // Those two subspaces are determined by the configuration of the record store rather then
        // the records.
        Range indexStateRange = indexStateSubspace().range();
        tr.clear(recordsSubspace().getKey(), indexStateRange.begin);
        tr.clear(indexStateRange.end, getSubspace().range().end);
    }

    @Override
    public CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull QueryComponent component) {
        preloadCache.invalidateAll();
        return new RecordsWhereDeleter(component).run();
    }

    /**
     * Returns whether or not record keys contain a trailing suffix indicating whether or not the record has been (or
     * could have been) split across multiple records.
     * @return <code>true</code> if the keys have split suffixes, otherwise <code>false</code>.
     */
    private boolean hasSplitRecordSuffix() {
        return getRecordMetaData().isSplitLongRecords() || !omitUnsplitRecordSuffix;
    }

    class RecordsWhereDeleter {
        @Nonnull final RecordMetaData recordMetaData;
        @Nullable final RecordType recordType;

        @Nonnull final QueryToKeyMatcher matcher;
        @Nullable final QueryToKeyMatcher indexMatcher;

        @Nonnull final Collection<RecordType> allRecordTypes;
        @Nonnull final Collection<Index> allIndexes;

        @Nonnull final List<IndexMaintainer> indexMaintainers;

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

            indexMaintainers = allIndexes.stream().map(FDBRecordStore.this::getIndexMaintainer).collect(Collectors.toList());

            evaluated = deleteRecordsWhereCheckRecordTypes();
            if (recordTypeKeyComparison == null) {
                indexEvaluated = evaluated;
            } else {
                indexEvaluated = Key.Evaluated.concatenate(evaluated.values().subList(1, evaluated.values().size()));
            }
            deleteRecordsWhereCheckIndexes();
        }

        private Key.Evaluated deleteRecordsWhereCheckRecordTypes() {
            Key.Evaluated evaluated = null;

            for (RecordType recordType : allRecordTypes) {
                final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(recordType.getPrimaryKey());
                if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
                    throw new Query.InvalidExpressionException("deleteRecordsWhere not matching primary key " +
                                                               recordType.getName());
                }
                if (evaluated == null) {
                    evaluated = match.getEquality(FDBRecordStore.this, EvaluationContext.EMPTY);
                } else if (!evaluated.equals(match.getEquality(FDBRecordStore.this, EvaluationContext.EMPTY))) {
                    throw new RecordCoreException("Primary key prefixes don't align",
                            "initialPrefix", evaluated,
                            "secondPrefix", match.getEquality(FDBRecordStore.this, EvaluationContext.EMPTY),
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
                final Key.Evaluated subkey = match.getEquality(FDBRecordStore.this, EvaluationContext.EMPTY);
                if (!evaluated.equals(subkey)) {
                    throw new RecordCoreException("Record count key prefix doesn't align",
                            "initialPrefix", evaluated,
                            "secondPrefix", match.getEquality(FDBRecordStore.this, EvaluationContext.EMPTY));
                }
            }

            return evaluated;
        }

        private void deleteRecordsWhereCheckIndexes() {
            if (evaluated == null) {
                return;
            }
            for (IndexMaintainer index : indexMaintainers) {
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
            for (IndexMaintainer index : indexMaintainers) {
                final CompletableFuture<Void> future;
                // Only need to check key expression in the case where a normal index has a different prefix.
                if (TupleHelpers.equals(prefix, indexPrefix) || Key.Expressions.hasRecordTypePrefix(index.state.index.getRootExpression())) {
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

    @Nonnull
    protected static QueryComponent mergeRecordTypeAndComponent(@Nonnull String recordType, @Nullable QueryComponent component) {
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

    @Override
    public PipelineSizer getPipelineSizer() {
        return pipelineSizer;
    }

    @Nonnull
    private FDBRecordStoreStateCache getStoreStateCache() {
        return storeStateCache == null ? context.getDatabase().getStoreStateCache() : storeStateCache;
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value) {
        if (getRecordMetaData().getRecordCountKey() != null) {
            if (key.getColumnSize() != value.size()) {
                throw new RecordCoreException("key and value are not the same size");
            }
            final ReadTransaction tr = context.readTransaction(true);
            final Tuple subkey = Tuple.from(RECORD_COUNT_KEY).addAll(value.toTupleAppropriateList());
            if (getRecordMetaData().getRecordCountKey().equals(key)) {
                return tr.get(getSubspace().pack(subkey)).thenApply(FDBRecordStore::decodeRecordCount);
            } else if (key.isPrefixKey(getRecordMetaData().getRecordCountKey())) {
                AsyncIterable<KeyValue> kvs = tr.getRange(getSubspace().range(Tuple.from(RECORD_COUNT_KEY)));
                return MoreAsyncUtil.reduce(getExecutor(), kvs.iterator(), 0L, (count, kv) -> count + decodeRecordCount(kv.getValue()));
            }
        }
        return evaluateAggregateFunction(Collections.emptyList(), IndexFunctionHelper.count(key), value, IsolationLevel.SNAPSHOT)
                .thenApply(tuple -> tuple.getLong(0));
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName) {
        // A COUNT index on this record type.
        IndexAggregateFunction aggregateFunction = IndexFunctionHelper.count(EmptyKeyExpression.EMPTY);
        Optional<IndexMaintainer> indexMaintainer = IndexFunctionHelper.indexMaintainerForAggregateFunction(this, aggregateFunction, Collections.singletonList(recordTypeName));
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
            return indexMaintainer.get().evaluateAggregateFunction(aggregateFunction, TupleRange.allOf(recordType.getRecordTypeKeyTuple()), IsolationLevel.SNAPSHOT)
                    .thenApply(tuple -> tuple.getLong(0));
        }
        throw new RecordCoreException("Require a COUNT index on " + recordTypeName);
    }

    public static long decodeRecordCount(@Nullable byte[] bytes) {
        return bytes == null ? 0 : ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> evaluateIndexRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                                @Nonnull IndexRecordFunction<T> function,
                                                                @Nonnull FDBRecord<Message> record) {
        return evaluateTypedIndexRecordFunction(evaluationContext, function, record);
    }

    @Nonnull
    protected <T, M extends Message> CompletableFuture<T> evaluateTypedIndexRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                                                           @Nonnull IndexRecordFunction<T> indexRecordFunction,
                                                                                           @Nonnull FDBRecord<M> record) {
        return IndexFunctionHelper.indexMaintainerForRecordFunction(this, indexRecordFunction, record)
                .orElseThrow(() -> new RecordCoreException("Record function " + indexRecordFunction +
                                                           " requires appropriate index on " + record.getRecordType().getName()))
            .evaluateRecordFunction(evaluationContext, indexRecordFunction, record);
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull EvaluationContext evaluationContext,
                                                          @Nonnull StoreRecordFunction<T> function,
                                                          @Nonnull FDBRecord<Message> record) {
        return evaluateTypedStoreFunction(evaluationContext, function, record);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <T, M extends Message> CompletableFuture<T> evaluateTypedStoreFunction(@Nonnull EvaluationContext evaluationContext,
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

    @Override
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return IndexFunctionHelper.indexMaintainerForAggregateFunction(this, aggregateFunction, recordTypeNames)
                .orElseThrow(() -> new RecordCoreException("Aggregate function " + aggregateFunction + " requires appropriate index"))
                .evaluateAggregateFunction(aggregateFunction, range, isolationLevel);
    }

    @Override
    @Nonnull
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query) {
        final RecordQueryPlanner planner = new RecordQueryPlanner(getRecordMetaData(), getRecordStoreState());
        return planner.plan(query);
    }


    @Nonnull
    public static IndexState writeOnlyIfTooManyRecordsForRebuild(long recordCount, boolean indexOnNewRecordTypes) {
        if (indexOnNewRecordTypes || recordCount <= MAX_RECORDS_FOR_REBUILD) {
            return IndexState.READABLE;
        } else {
            return IndexState.WRITE_ONLY;
        }
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
     * @param existenceCheck when to throw an exception if the record store does or does not already exist
     * @return future with whether the record store was modified by the check
     */
    @Nonnull
    public CompletableFuture<Boolean> checkVersion(@Nullable UserVersionChecker userVersionChecker,
                                                   @Nonnull StoreExistenceCheck existenceCheck) {
        return checkVersion(userVersionChecker, existenceCheck, AsyncUtil.DONE);
    }

    @Nonnull
    private CompletableFuture<Boolean> checkVersion(@Nullable UserVersionChecker userVersionChecker,
                                                    @Nonnull StoreExistenceCheck existenceCheck,
                                                    @Nonnull CompletableFuture<Void> metaDataPreloadFuture) {
        CompletableFuture<Void> subspacePreloadFuture = preloadSubspaceAsync();
        CompletableFuture<RecordMetaDataProto.DataStoreInfo> storeHeaderFuture = getStoreStateCache().get(this, existenceCheck).thenApply(storeInfo -> {
            recordStoreState = storeInfo.getRecordStoreState().toMutable();
            return recordStoreState.getStoreHeader();
        });
        if (!MoreAsyncUtil.isCompletedNormally(metaDataPreloadFuture)) {
            storeHeaderFuture = metaDataPreloadFuture.thenCombine(storeHeaderFuture, (vignore, storeHeader) -> storeHeader);
        }
        if (!MoreAsyncUtil.isCompletedNormally(subspacePreloadFuture)) {
            storeHeaderFuture = subspacePreloadFuture.thenCombine(storeHeaderFuture, (vignore, storeHeader) -> storeHeader);
        }
        CompletableFuture<Boolean> result = storeHeaderFuture.thenCompose(storeHeader -> checkVersion(storeHeader, userVersionChecker));
        return context.instrument(FDBStoreTimer.Events.CHECK_VERSION, result);
    }

    @Nonnull
    private CompletableFuture<Boolean> checkVersion(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader, @Nullable UserVersionChecker userVersionChecker) {
        RecordMetaDataProto.DataStoreInfo.Builder info = storeHeader.toBuilder();
        final boolean newStore = info.getFormatVersion() == 0;
        final int oldMetaDataVersion = newStore ? -1 : info.getMetaDataversion();
        final int oldUserVersion = newStore ? -1 : info.getUserVersion();
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
                saveStoreHeader(info.build());
            }
            return dirty[0];
        });
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
                                            LogMessageKeys.STORED_VERSION, oldUserVersion,
                                            LogMessageKeys.LOCAL_VERSION, newUserVersion,
                                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
                            throw new RecordStoreStaleUserVersionException("Stale user version with local version " + newUserVersion + " and stored version " + oldUserVersion);
                        }
                        info.setUserVersion(newUserVersion);
                        dirty[0] = true;
                        if (oldUserVersion < 0) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(KeyValueLogMessage.of("setting initial user version",
                                        LogMessageKeys.NEW_VERSION, newUserVersion,
                                        subspaceProvider.logKey(), subspaceProvider.toString(context)));
                            }
                        } else {
                            LOGGER.info(KeyValueLogMessage.of("changing user version",
                                    LogMessageKeys.OLD_VERSION, oldUserVersion,
                                    LogMessageKeys.NEW_VERSION, newUserVersion,
                                    subspaceProvider.logKey(), subspaceProvider.toString(context)));
                        }
                    }
                    return null;
                });
    }

    @Nonnull
    private RecordMetaDataProto.DataStoreInfo checkAndParseStoreHeader(@Nullable KeyValue firstKeyValue,
                                                                       @Nonnull StoreExistenceCheck existenceCheck) {
        RecordMetaDataProto.DataStoreInfo info;
        if (firstKeyValue == null) {
            info = RecordMetaDataProto.DataStoreInfo.getDefaultInstance();
        } else if (!checkFirstKeyIsHeader(firstKeyValue, getContext(), getSubspaceProvider(), getSubspace(), existenceCheck)) {
            // Treat as brand new, although there is no way to be sure that what was written is compatible
            // with the current default versions.
            info = RecordMetaDataProto.DataStoreInfo.getDefaultInstance();
        } else {
            try {
                info = RecordMetaDataProto.DataStoreInfo.parseFrom(firstKeyValue.getValue());
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreStorageException("Error reading version", ex);
            }
        }
        checkStoreHeaderInternal(info, getContext(), getSubspaceProvider(), existenceCheck);
        return info;
    }

    @API(API.Status.INTERNAL)
    @Nonnull
    public static CompletableFuture<Void> checkStoreHeader(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader,
                                                           @Nonnull FDBRecordContext context,
                                                           @Nonnull SubspaceProvider subspaceProvider,
                                                           @Nonnull Subspace subspace,
                                                           @Nonnull StoreExistenceCheck existenceCheck) {
        if (storeHeader == RecordMetaDataProto.DataStoreInfo.getDefaultInstance()) {
            // Validate that the store is empty
            return readStoreFirstKey(context, subspace, IsolationLevel.SNAPSHOT).thenAccept(firstKeyValue -> {
                if (firstKeyValue != null && checkFirstKeyIsHeader(firstKeyValue, context, subspaceProvider, subspace, existenceCheck)) {
                    // Somehow, we had a default store header, which indicates that this was an uninitialized store,
                    // but when we read the first key, it was a store header.
                    throw new RecordCoreException("Record store with no header had header in database",
                            subspaceProvider.logKey(), subspaceProvider.toString(context));
                }
                // We have relied on the value of the store header key itself. We performed the read at SNAPSHOT,
                // though, to avoid conflicts on the first key if the store isn't empty.
                context.ensureActive().addReadConflictKey(subspace.pack(STORE_INFO_KEY));
                checkStoreHeaderInternal(storeHeader, context, subspaceProvider, existenceCheck);
            });
        } else {
            try {
                checkStoreHeaderInternal(storeHeader, context, subspaceProvider, existenceCheck);
                return AsyncUtil.DONE;
            } catch (RecordCoreException e) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        }
    }

    private static boolean checkFirstKeyIsHeader(@Nonnull KeyValue firstKeyValue,
                                                 @Nonnull FDBRecordContext context,
                                                 @Nonnull SubspaceProvider subspaceProvider,
                                                 @Nonnull Subspace subspace,
                                                 @Nonnull StoreExistenceCheck existenceCheck) {
        if (TupleHelpers.equals(subspace.unpack(firstKeyValue.getKey()), Tuple.from(STORE_INFO_KEY))) {
            return true;
        } else if (existenceCheck == StoreExistenceCheck.NONE) {
            LOGGER.warn(KeyValueLogMessage.of("Record store has no info but is not empty",
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                    LogMessageKeys.KEY, subspace.unpack(firstKeyValue.getKey())));
            return false;
        } else {
            throw new RecordStoreNoInfoAndNotEmptyException("Record store has no info but is not empty",
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                    LogMessageKeys.KEY, subspace.unpack(firstKeyValue.getKey()));
        }
    }

    private static void checkStoreHeaderInternal(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader,
                                                 @Nonnull FDBRecordContext context,
                                                 @Nonnull SubspaceProvider subspaceProvider,
                                                 @Nonnull StoreExistenceCheck existenceCheck) {
        if (storeHeader == RecordMetaDataProto.DataStoreInfo.getDefaultInstance()) {
            if (existenceCheck == StoreExistenceCheck.ERROR_IF_NOT_EXISTS) {
                throw new RecordStoreDoesNotExistException("Record store does not exist",
                        subspaceProvider.logKey(), subspaceProvider.toString(context));
            }
            context.increment(FDBStoreTimer.Counts.CREATE_RECORD_STORE);
        } else if (existenceCheck == StoreExistenceCheck.ERROR_IF_EXISTS) {
            throw new RecordStoreAlreadyExistsException("Record store already exists",
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        } else {
            if (storeHeader.getFormatVersion() < MIN_FORMAT_VERSION || storeHeader.getFormatVersion() > MAX_SUPPORTED_FORMAT_VERSION) {
                throw new UnsupportedFormatVersionException("Unsupported format version " + storeHeader.getFormatVersion(),
                        subspaceProvider.logKey(), subspaceProvider);
            }
        }
    }

    @Nonnull
    private CompletableFuture<RecordMetaDataProto.DataStoreInfo> loadStoreHeaderAsync(@Nonnull StoreExistenceCheck existenceCheck, @Nonnull IsolationLevel isolationLevel) {
        return readStoreFirstKey(context, getSubspace(), isolationLevel).thenApply(keyValue -> checkAndParseStoreHeader(keyValue, existenceCheck));
    }

    private void saveStoreHeader(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader) {
        if (recordStoreState == null) {
            throw new RecordCoreException("cannot update store header with a null record store state");
        }
        recordStoreState.beginWrite();
        try {
            context.setDirtyStoreState(true);
            synchronized (recordStoreState) {
                recordStoreState.setStoreHeader(storeHeader);
                ensureContextActive().set(getSubspace().pack(STORE_INFO_KEY), storeHeader.toByteArray());
            }
        } finally {
            recordStoreState.endWrite();
        }
    }

    @Nonnull
    private static CompletableFuture<KeyValue> readStoreFirstKey(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace, @Nonnull IsolationLevel isolationLevel) {
        final AsyncIterator<KeyValue> iterator = context.readTransaction(isolationLevel.isSnapshot()).getRange(subspace.range(), 1).iterator();
        return context.instrument(FDBStoreTimer.Events.LOAD_RECORD_STORE_INFO,
                iterator.onHasNext().thenApply(hasNext -> hasNext ? iterator.next() : null));
    }

    /**
     * Rebuild all of this store's indexes. All indexes will then be marked as {@linkplain IndexState#READABLE readable}
     * when this function completes. Note that this will attempt to read all of the records within
     * this store in a single transaction, so for large record stores, this can run up against transaction
     * time and size limits. For larger stores, one should use the {@link OnlineIndexer} to build
     * each index instead.
     *
     * @return a future that will complete when all of the indexes are built
     */
    @Nonnull
    public CompletableFuture<Void> rebuildAllIndexes() {
        Transaction tr = ensureContextActive();
        // Note that index states are *not* cleared, as rebuilding the indexes resets each state
        tr.clear(getSubspace().range(Tuple.from(INDEX_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY)));
        tr.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY)));
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

    // Actually (1) writes the index state to the database and (2) updates the cached state with the new state
    private void updateIndexState(@Nonnull String indexName, byte[] indexKey, @Nonnull IndexState indexState) {
        // This is generally called by someone who should already have a write lock, but adding them here
        // defensively shouldn't cause problems.
        recordStoreState.beginWrite();
        try {
            context.setDirtyStoreState(true);
            Transaction tr = context.ensureActive();
            if (IndexState.READABLE.equals(indexState)) {
                tr.clear(indexKey);
            } else {
                tr.set(indexKey, Tuple.from(indexState.code()).pack());
            }
            recordStoreState.setState(indexName, indexState);
        } finally {
            recordStoreState.endWrite();
        }
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
                    updateIndexState(indexName, indexKey, indexState);
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
     * See the version of {@link #markIndexWriteOnly(String) markIndexWriteOnly()}
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
     * version of {@link #markIndexDisabled(Index)} markIndexDisabled()}
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
     * {@link #markIndexReadable(String) markIndexReadable()}
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
                        Optional<Range> firstUnbuilt = context.join(builtFuture);
                        Optional<RecordIndexUniquenessViolation> uniquenessViolation = context.join(uniquenessFuture);
                        if (firstUnbuilt.isPresent()) {
                            throw new IndexNotBuiltException("Attempted to make unbuilt index readable" , firstUnbuilt.get(),
                                    LogMessageKeys.INDEX_NAME, index.getName(),
                                    "unbuiltRangeBegin", ByteArrayUtil2.loggable(firstUnbuilt.get().begin),
                                    "unbuiltRangeEnd", ByteArrayUtil2.loggable(firstUnbuilt.get().end),
                                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                                    LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
                        } else if (uniquenessViolation.isPresent()) {
                            RecordIndexUniquenessViolation wrapped = new RecordIndexUniquenessViolation("Uniqueness violation when making index readable",
                                                                                                        uniquenessViolation.get());
                            wrapped.addLogInfo(
                                    LogMessageKeys.INDEX_NAME, index.getName(),
                                    subspaceProvider.logKey(), subspaceProvider.toString(context));
                            throw wrapped;
                        } else {
                            updateIndexState(index.getName(), indexKey, IndexState.READABLE);
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
                    updateIndexState(indexName, indexKey, IndexState.READABLE);
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
     * Ensure that subspace provider has cached {@code Subspace} so that calling {@link #getSubspace} will not block.
     * @return a future that will be complete when the subspace is available
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    protected CompletableFuture<Void> preloadSubspaceAsync() {
        return getSubspaceAsync().thenApply(subspace -> null);
    }

    /**
     * Loads the current state of the record store asynchronously.
     * @return a future that will be complete when this store has loaded its record store state
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    protected CompletableFuture<Void> preloadRecordStoreStateAsync() {
        return loadRecordStoreStateAsync(StoreExistenceCheck.NONE, IsolationLevel.SNAPSHOT, IsolationLevel.SNAPSHOT)
                .thenAccept(state -> this.recordStoreState = state.toMutable());
    }

    /**
     * Loads the current state of the record store within the given subspace asynchronously.
     *
     * @param existenceCheck the action to be taken when the record store already exists (or does not exist yet)
     * @return a future that will contain the state of the record state located at the given subspace
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public CompletableFuture<RecordStoreState> loadRecordStoreStateAsync(@Nonnull StoreExistenceCheck existenceCheck) {
        return loadRecordStoreStateAsync(existenceCheck, IsolationLevel.SERIALIZABLE, IsolationLevel.SNAPSHOT);
    }

    @Nonnull
    private CompletableFuture<RecordStoreState> loadRecordStoreStateAsync(@Nonnull StoreExistenceCheck existenceCheck,
                                                                          @Nonnull IsolationLevel storeHeaderIsolationLevel,
                                                                          @Nonnull IsolationLevel indexStateIsolationLevel) {
        // Don't rely on the subspace being loaded as this is called as part of store initialization
        return getSubspaceAsync().thenCompose(subspace ->
                loadRecordStoreStateInternalAsync(existenceCheck, storeHeaderIsolationLevel, indexStateIsolationLevel)
        );
    }

    @Nonnull
    private CompletableFuture<RecordStoreState> loadRecordStoreStateInternalAsync(@Nonnull StoreExistenceCheck existenceCheck,
                                                                                  @Nonnull IsolationLevel storeHeaderIsolationLevel,
                                                                                  @Nonnull IsolationLevel indexStateIsolationLevel) {
        CompletableFuture<RecordMetaDataProto.DataStoreInfo> storeHeaderFuture = loadStoreHeaderAsync(existenceCheck, storeHeaderIsolationLevel);
        CompletableFuture<Map<String, IndexState>> loadIndexStates = loadIndexStatesAsync(indexStateIsolationLevel);
        return context.instrument(FDBStoreTimer.Events.LOAD_RECORD_STORE_STATE, storeHeaderFuture.thenCombine(loadIndexStates, RecordStoreState::new));
    }

    @Nonnull
    private CompletableFuture<Map<String, IndexState>> loadIndexStatesAsync(@Nonnull IsolationLevel isolationLevel) {
        Subspace isSubspace = getSubspace().subspace(Tuple.from(INDEX_STATE_SPACE_KEY));
        KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(isSubspace)
                .setContext(getContext())
                .setRange(TupleRange.ALL)
                .setContinuation(null)
                .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationLevel).build()))
                .build();
        FDBStoreTimer timer = getTimer();
        CompletableFuture<Map<String, IndexState>> result = cursor.asList().thenApply(list -> {
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
            return indexStateMap;
        });
        if (timer != null) {
            result = timer.instrument(FDBStoreTimer.Events.LOAD_RECORD_STORE_INDEX_META_DATA, result, context.getExecutor());
        }
        return result;
    }

    /**
     * Add a read conflict key for all records.
     */
    private void addRecordsReadConflict() {
        Transaction tr = ensureContextActive();
        byte[] recordKey = getSubspace().pack(Tuple.from(RECORD_KEY));
        tr.addReadConflictRange(recordKey, ByteArrayUtil.strinc(recordKey));
    }

    /**
     * Add a read conflict key so that the transaction will fail if the index state has changed.
     * @param indexName the index to conflict on, if it's state changes
     */
    private void addIndexStateReadConflict(@Nonnull String indexName) {
        if (!getRecordMetaData().hasIndex(indexName)) {
            throw new MetaDataException("Index " + indexName + " does not exist in meta-data.");
        }
        Transaction tr = ensureContextActive();
        byte[] indexStateKey = getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, indexName));
        tr.addReadConflictKey(indexStateKey);
    }

    /**
     * Add a read conflict key for the whole record store state.
     */
    private void addStoreStateReadConflict() {
        Transaction tr = ensureContextActive();
        byte[] indexStateKey = getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY));
        tr.addReadConflictRange(indexStateKey, ByteArrayUtil.strinc(indexStateKey));
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

    /**
     * Gets a map from {@link Index} to {@link IndexState} for all the indexes in the meta-data.
     * This method will not perform any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     * @return a map of all the index states.
     */
    @Nonnull
    public Map<Index, IndexState> getAllIndexStates() {
        final RecordStoreState localRecordStoreState = getRecordStoreState();
        localRecordStoreState.beginRead();
        try {
            addStoreStateReadConflict();
            return getRecordMetaData().getAllIndexes().stream()
                    .collect(Collectors.toMap(Function.identity(), localRecordStoreState::getState));
        } finally {
            localRecordStoreState.endRead();
        }
    }

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
            return rebuildIndexWithNoRecord(index, reason);
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
    private CompletableFuture<Void> rebuildIndexWithNoRecord(@Nonnull final Index index, @Nonnull RebuildIndexReason reason) {
        final boolean newStore = reason == RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index with no record",
                                            LogMessageKeys.INDEX_NAME, index.getName(),
                                            LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                                            LogMessageKeys.REASON, reason.name(),
                                            subspaceProvider.logKey(), subspaceProvider.toString(context),
                                            LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                LOGGER.debug(msg.toString());
            } else {
                LOGGER.info(msg.toString());
            }
        }

        return markIndexReadable(index).thenApply(b -> null);
    }

    /**
     * Rebuild an index. This clears the index and attempts to build it within a single transaction. Upon
     * successful completion, the index is marked as {@linkplain IndexState#READABLE readable} and can
     * be used to satisfy queries.
     *
     * <p>
     * Because the operations all occur within a single transaction, for larger record stores,
     * this operation can run into transaction size and time limits. (See:
     * <a href="https://apple.github.io/foundationdb/known-limitations.html">FoundationDB Known Limitations</a>.)
     * To build an index on such a record store, the user should use the {@link OnlineIndexer}, which
     * breaks up work across multiple transactions to avoid those limits.
     * </p>
     *
     * @param index the index to rebuild
     * @return a future that will complete when the index build has finished
     * @see OnlineIndexer
     */
    @Nonnull
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
                                            LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                                            LogMessageKeys.REASON, reason.name(),
                                            subspaceProvider.logKey(), subspaceProvider.toString(context),
                                            LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                LOGGER.debug(msg.toString());
            } else {
                LOGGER.info(msg.toString());
            }
        }

        long startTime = System.nanoTime();
        OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(this).setIndex(index).setRecordTypes(recordTypes).build();
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

        final boolean newStore = oldFormatVersion == 0;
        final int oldMetaDataVersion = newStore ? -1 : info.getMetaDataversion();
        final RecordMetaData metaData = getRecordMetaData();
        final int newMetaDataVersion = metaData.getVersion();
        if (oldMetaDataVersion > newMetaDataVersion) {
            CompletableFuture<Void> ret = new CompletableFuture<>();
            ret.completeExceptionally(new RecordStoreStaleMetaDataVersionException("Local meta-data has stale version",
                                        LogMessageKeys.LOCAL_VERSION, newMetaDataVersion,
                                        LogMessageKeys.STORED_VERSION, oldMetaDataVersion,
                                        subspaceProvider.logKey(), subspaceProvider.toString(context)));
            return ret;
        }
        final boolean metaDataVersionChanged = oldMetaDataVersion != newMetaDataVersion;

        if (!formatVersionChanged && !metaDataVersionChanged) {
            return AsyncUtil.DONE;
        }

        if (LOGGER.isInfoEnabled()) {
            if (newStore) {
                LOGGER.info(KeyValueLogMessage.of("new record store",
                        LogMessageKeys.FORMAT_VERSION, newFormatVersion,
                        LogMessageKeys.META_DATA_VERSION, newMetaDataVersion,
                        subspaceProvider.logKey(), subspaceProvider.toString(context)));
            } else {
                if (formatVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("format version changed",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, newFormatVersion,
                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
                }
                if (metaDataVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("meta-data version changed",
                            LogMessageKeys.OLD_VERSION, oldMetaDataVersion,
                            LogMessageKeys.NEW_VERSION, newMetaDataVersion,
                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
                }
            }
        }

        dirty[0] = true;
        return checkRebuild(userVersionChecker, info, metaData);
    }

    private CompletableFuture<Void> checkRebuild(@Nullable UserVersionChecker userVersionChecker,
                                                 @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                 @Nonnull RecordMetaData metaData) {
        final List<CompletableFuture<Void>> work = new LinkedList<>();

        final int oldFormatVersion = info.getFormatVersion();
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
                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
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
                            subspaceProvider.logKey(), subspaceProvider.toString(context));
                }
                addConvertRecordVersions(work);
            }
        }

        final boolean newStore = oldFormatVersion == 0;
        final int oldMetaDataVersion = newStore ? -1 : info.getMetaDataversion();
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
            // Can only determine overall emptiness (without an additional read) if got count for all record types.
            Pair<CompletableFuture<Long>, Boolean> counterAndIsAllTypes = getRecordCountForRebuildIndexes(newStore, rebuildRecordCounts, indexes);
            return counterAndIsAllTypes.getLeft().thenCompose(recordCount -> {
                if (recordCount == 0 && counterAndIsAllTypes.getRight() &&
                        formatVersion >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION && omitUnsplitRecordSuffix) {
                    // There are no records. Don't use the legacy split format.
                    if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                        KeyValueLogMessage msg = KeyValueLogMessage.build("upgrading unsplit format on empty store",
                                                    LogMessageKeys.NEW_FORMAT_VERSION, formatVersion,
                                                    subspaceProvider.logKey(), subspaceProvider.toString(context));
                        if (newStore) {
                            LOGGER.debug(msg.toString());
                        } else {
                            LOGGER.info(msg.toString());
                        }
                    }
                    omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
                    info.clearOmitUnsplitRecordSuffix();
                    addRecordsReadConflict(); // We used snapshot to determine emptiness, and are now acting on it.
                }
                Map<Index, IndexState> newStates = getStatesForRebuildIndexes(userVersionChecker, indexes, recordCount, newStore, rebuildRecordCounts, oldMetaDataVersion, oldFormatVersion);
                return rebuildIndexes(indexes, newStates, work, newStore ? RebuildIndexReason.NEW_STORE : RebuildIndexReason.FEW_RECORDS, oldMetaDataVersion);
            });
        } else {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenAll(work);
        }
    }

    /**
     * Get count of records to pass to checker to decide whether to build right away.
     * @param newStore {@code true} if this is a brand new store
     * @param rebuildRecordCounts {@code true} if there is a record count key that needs to be rebuilt
     * @param indexes indexes that need to be built
     * @return a pair of a future that completes to the record count for the version checker
     * and a flag that is {@code true} if this count is in fact for all record types
     */
    @Nonnull
    @SuppressWarnings("PMD.EmptyCatchBlock")
    protected Pair<CompletableFuture<Long>, Boolean> getRecordCountForRebuildIndexes(boolean newStore, boolean rebuildRecordCounts,
                                                                                     @Nonnull Map<Index, List<RecordType>> indexes) {
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
                return Pair.of(getSnapshotRecordCountForRecordType(singleRecordTypeWithPrefixKey.getName()), false);
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
                return Pair.of(getSnapshotRecordCount(), true);
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
        final RecordCursor<FDBStoredRecord<Message>> records;
        if (singleRecordTypeWithPrefixKey == null) {
            records = scanRecords(null, scanProperties);
        } else {
            records = scanRecords(TupleRange.allOf(singleRecordTypeWithPrefixKey.getRecordTypeKeyTuple()), null, scanProperties);
        }
        final CompletableFuture<Long> zeroOrInfinity = records.onNext().thenApply(result -> {
            if (result.hasNext()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("version check scan found non-empty store",
                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
                }
                return Long.MAX_VALUE;
            } else {
                if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                    KeyValueLogMessage msg = KeyValueLogMessage.build("version check scan found empty store",
                            subspaceProvider.logKey(), subspaceProvider.toString(context));
                    if (newStore) {
                        LOGGER.debug(msg.toString());
                    } else {
                        LOGGER.info(msg.toString());
                    }
                }
                return 0L;
            }
        });
        return Pair.of(zeroOrInfinity, singleRecordTypeWithPrefixKey == null);
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
                    FDBRecordStore.writeOnlyIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes) :
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
        if (LOGGER.isDebugEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("indexes need rebuilding",
                                        LogMessageKeys.RECORD_COUNT, recordCount == Long.MAX_VALUE ? "unknown" : Long.toString(recordCount), 
                                        subspaceProvider.logKey(), subspaceProvider.toString(context));
            if (rebuildRecordCounts) {
                msg.addKeyAndValue(LogMessageKeys.REBUILD_RECORD_COUNTS, "true");
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
                msg.addKeyAndValue(LogMessageKeys.NEW_STORE, "true");
            }
            LOGGER.debug(msg.toString());
        }
        context.increment(FDBStoreTimer.Counts.INDEXES_NEED_REBUILDING, newStates.entrySet().size());
        return newStates;
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
            KeyValueLogMessage msg = KeyValueLogMessage.build("removing index",
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                    LogMessageKeys.SUBSPACE_KEY, formerIndex.getSubspaceKey());
            if (formerIndex.getFormerName() != null) {
                msg.addKeyAndValue(LogMessageKeys.INDEX_NAME, formerIndex.getFormerName());
            }
            LOGGER.debug(msg.toString());
        }
        final long startTime = System.nanoTime();
        Transaction tr = ensureContextActive();
        tr.clear(getSubspace().range(Tuple.from(INDEX_KEY, formerIndex.getSubspaceTupleKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        tr.clear(getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        tr.clear(getSubspace().range(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY, formerIndex.getSubspaceTupleKey())));
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
                                           subspaceProvider.logKey(), subspaceProvider.toString(context)));
        }
        final Map<Key.Evaluated, Long> counts = new HashMap<>();
        final RecordCursor<FDBStoredRecord<Message>> records = scanRecords(null, ScanProperties.FORWARD_SCAN);
        CompletableFuture<Void> future = records.forEach(record -> {
            Key.Evaluated subkey = recordCountKey.evaluateSingleton(record);
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
     * Return a cursor of boundaries separating the key ranges maintained by each FDB server. This information can be
     * useful for splitting a large task (e.g., rebuilding an index for a large record store) into smaller tasks (e.g.,
     * rebuilding the index for records in certain primary key ranges) more evenly so that they can be executed in a
     * parallel fashion efficiently. The returned boundaries are an estimate from FDB's locality API and may not
     * represent the exact boundary locations at any database version.
     * <p>
     * The boundaries are returned as a cursor which is sorted and does not contain any duplicates. The first element of
     * the list is greater than or equal to <code>low</code>, and the last element is less than or equal to
     * <code>high</code>.
     * <p>
     * This implementation may not work when there are too many shard boundaries to complete in a single transaction.
     * <p>
     * Note: the returned cursor is blocking and must not be used in an asynchronous context
     *
     * @param low low endpoint of primary key range (inclusive)
     * @param high high endpoint of primary key range (exclusive)
     * @return the list of boundary primary keys
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public RecordCursor<Tuple> getPrimaryKeyBoundaries(@Nonnull Tuple low, @Nonnull Tuple high) {
        final Transaction transaction = ensureContextActive();
        byte[] rangeStart = recordsSubspace().pack(low);
        byte[] rangeEnd = recordsSubspace().pack(high);
        CloseableAsyncIterator<byte[]> cursor = context.getDatabase().getLocalityProvider().getBoundaryKeys(transaction, rangeStart, rangeEnd);
        final boolean hasSplitRecordSuffix = hasSplitRecordSuffix();
        DistinctFilterCursorClosure closure = new DistinctFilterCursorClosure();
        return RecordCursor.fromIterator(getExecutor(), cursor)
                .flatMapPipelined(
                        result -> RecordCursor.fromIterator(getExecutor(),
                                transaction.snapshot().getRange(result, rangeEnd, 1).iterator()),
                        DEFAULT_PIPELINE_SIZE)
                .map(keyValue -> {
                    Tuple recordKey = recordsSubspace().unpack(keyValue.getKey());
                    return hasSplitRecordSuffix ? recordKey.popBack() : recordKey;
                })
                // The input stream is expected to be sorted so this filter can work to de-duplicate the data.
                .filter(closure::pred);
    }

    private static class DistinctFilterCursorClosure {
        private Tuple previousKey = null;

        private boolean pred(Tuple key) {
            if (key.equals(previousKey)) {
                return false;
            } else {
                previousKey = key;
                return true;
            }
        }
    }

    /**
     * Validate the current meta-data for this store.
     * @deprecated validation is done by {@link com.apple.foundationdb.record.RecordMetaDataBuilder}
     */
    @Deprecated
    public void validateMetaData() {
        final MetaDataValidator validator = new MetaDataValidator(metaDataProvider, indexMaintainerRegistry);
        validator.validate();
    }

    @Nonnull
    public CompletableFuture<byte[]> repairRecordKeys(@Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return repairRecordKeys(continuation, scanProperties, false);
    }

    /**
     * Validate and repair known potential issues with record keys. Currently, this method is capable of identifying
     * and repairing the following scenarios:
     * <ul>
     *     <li>For record stores in which record splitting is disabled but the {@code omitUnsplitRecordSuffix} flag
     *         is {@code true}, keys found missing the {@link SplitHelper#UNSPLIT_RECORD} suffix will be repaired.</li>
     *     <li>For record stores in which record splitting is disabled, but the record key suffix is found to be
     *         a value other than {@link SplitHelper#UNSPLIT_RECORD}, the {@link FDBStoreTimer.Counts#INVALID_SPLIT_SUFFIX}
     *         counter will be incremented.
     *     <li>For record stores in which record splitting is disabled, but the record key is longer than expected,
     *         the {@link FDBStoreTimer.Counts#INVALID_KEY_LENGTH} counter will be incremented.
     * </ul>
     *
     * @param continuation continuation from a previous repair attempt or {@code null} to start from the beginning
     * @param scanProperties properties to provide scan limits on the repair process
     *   record keys should be logged
     * @param isDryRun if true, no repairs are made, however counters involving irregular keys or keys that would
     *   would have been repaired are incremented
     * @return a future that completes to a continuation or {@code null} if the repair has been completed
     */
    @Nonnull
    public CompletableFuture<byte[]> repairRecordKeys(@Nullable byte[] continuation,
                                                      @Nonnull ScanProperties scanProperties,
                                                      final boolean isDryRun) {
        // If the records aren't split to begin with, then there is nothing to do.
        if (getRecordMetaData().isSplitLongRecords()) {
            return CompletableFuture.completedFuture(null);
        }

        if (scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot()) {
            throw new RecordCoreArgumentException("Cannot repair record key split markers at SNAPSHOT isolation level")
                    .addLogInfo(LogMessageKeys.SCAN_PROPERTIES, scanProperties);
        }

        final Subspace recordSubspace = recordsSubspace();
        KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(recordSubspace)
                .setContext(getRecordContext())
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();

        final AtomicReference<byte[]> nextContinuation = new AtomicReference<>();
        final FDBRecordContext context = getContext();
        return AsyncUtil.whileTrue( () ->
                cursor.onNext().thenApply( result -> {
                    if (!result.hasNext()) {
                        if (result.hasStoppedBeforeEnd()) {
                            nextContinuation.set(result.getContinuation().toBytes());
                        }
                        return false;
                    }

                    repairRecordKeyIfNecessary(context, recordSubspace, result.get(), isDryRun);
                    return true;
                })).thenApply(ignored -> nextContinuation.get());
    }

    private void repairRecordKeyIfNecessary(@Nonnull FDBRecordContext context, @Nonnull Subspace recordSubspace,
                                            @Nonnull KeyValue keyValue, final boolean isDryRun) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Tuple recordKey = recordSubspace.unpack(keyValue.getKey());

        // Ignore version key
        if (metaData.isStoreRecordVersions() && isMaybeVersion(recordKey)) {
            return;
        }

        final Message record = serializer.deserialize(metaData, recordKey, keyValue.getValue(), getTimer());
        final RecordType recordType = metaData.getRecordTypeForDescriptor(record.getDescriptorForType());

        final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();

        if (recordKey.size() == primaryKeyExpression.getColumnSize()) {
            context.increment(FDBStoreTimer.Counts.REPAIR_RECORD_KEY);

            final Tuple newPrimaryKey = recordKey.add(SplitHelper.UNSPLIT_RECORD);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Repairing primary key",
                        LogMessageKeys.RECORD_TYPE, recordType.getName(),
                        subspaceProvider.logKey(), subspaceProvider.toString(context),
                        "dry_run", isDryRun,
                        "orig_primary_key", recordKey,
                        "new_primary_key", newPrimaryKey));
            }

            if (!isDryRun) {
                final Transaction tr = context.ensureActive();
                tr.clear(keyValue.getKey());
                tr.set(recordSubspace.pack(newPrimaryKey), keyValue.getValue());
            }
        } else if (recordKey.size() == primaryKeyExpression.getColumnSize() + 1) {
            Object suffix = recordKey.get(recordKey.size() - 1);
            if (!(suffix instanceof Long) || !(((Long) suffix) == SplitHelper.UNSPLIT_RECORD)) {
                context.increment(FDBStoreTimer.Counts.INVALID_SPLIT_SUFFIX);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("Invalid split suffix",
                            subspaceProvider.logKey(), subspaceProvider.toString(context),
                            LogMessageKeys.RECORD_TYPE, recordType.getName(),
                            LogMessageKeys.PRIMARY_KEY, recordKey));
                }
            }
        } else  {
            context.increment(FDBStoreTimer.Counts.INVALID_KEY_LENGTH);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Invalid key length",
                        subspaceProvider.logKey(), subspaceProvider.toString(context),
                        LogMessageKeys.RECORD_TYPE, recordType.getName(),
                        LogMessageKeys.PRIMARY_KEY, recordKey));
            }
        }
    }

    private boolean isMaybeVersion(Tuple recordKey) {
        Object suffix = recordKey.get(recordKey.size() - 1);
        return suffix instanceof Long && (((Long) suffix) == SplitHelper.RECORD_VERSION);
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

    /**
     * A builder for {@link FDBRecordStore}.
     *
     * Methods for getting a record store from the builder:
     * <ul>
     * <li>{@link #createOrOpen}: open an existing record store or create it the first time.</li>
     * <li>{@link #open}: open an existing record store or throw an exception if it has never been created.</li>
     * <li>{@link #create}: create a record store or throw an exception if it has already been created.</li>
     * <li>{@link #build}: return a record store without checking its state in the database. One should almost always
     * call {@link FDBRecordStore#checkVersion} before actually using the record store.</li>
     * </ul>
     *
     * <pre><code>
     * FDBRecordStore.newBuilder().setMetaDataProvider(md).setContext(ctx).setSubspace(s).createOrOpen()
     * </code></pre>
     *
     */
    @API(API.Status.STABLE)
    public static class Builder implements BaseBuilder<Message, FDBRecordStore> {
        @Nullable
        private RecordSerializer<Message> serializer = DynamicMessageRecordSerializer.instance();

        private int formatVersion = DEFAULT_FORMAT_VERSION;

        @Nullable
        private RecordMetaDataProvider metaDataProvider;

        @Nullable
        private FDBMetaDataStore metaDataStore;

        @Nullable
        private FDBRecordContext context;

        @Nullable
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;

        @Nonnull
        private IndexMaintainerRegistry indexMaintainerRegistry = IndexMaintainerRegistryImpl.instance();

        @Nonnull
        private IndexMaintenanceFilter indexMaintenanceFilter = IndexMaintenanceFilter.NORMAL;

        @Nonnull
        private FDBRecordStoreBase.PipelineSizer pipelineSizer = DEFAULT_PIPELINE_SIZER;

        @Nullable
        protected SubspaceProvider subspaceProvider = null;

        @Nullable
        private FDBRecordStoreStateCache storeStateCache = null;

        protected Builder() {
        }

        protected Builder(Builder other) {
            copyFrom(other);
        }

        protected Builder(FDBRecordStore store) {
            copyFrom(store);
        }

        /**
         * Copy state from another store builder.
         * @param other the record store builder whose state to take
         */
        public void copyFrom(@Nonnull Builder other) {
            this.serializer = other.serializer;
            this.formatVersion = other.formatVersion;
            this.metaDataProvider = other.metaDataProvider;
            this.metaDataStore = other.metaDataStore;
            this.context = other.context;
            this.subspaceProvider = other.subspaceProvider;
            this.userVersionChecker = other.userVersionChecker;
            this.indexMaintainerRegistry = other.indexMaintainerRegistry;
            this.indexMaintenanceFilter = other.indexMaintenanceFilter;
            this.pipelineSizer = other.pipelineSizer;
            this.storeStateCache = other.storeStateCache;
        }

        /**
         * Copy state from a record store.
         * @param store the record store whose state to take
         */
        public void copyFrom(@Nonnull FDBRecordStore store) {
            this.serializer = store.serializer;
            this.formatVersion = store.formatVersion;
            this.metaDataProvider = store.metaDataProvider;
            this.context = store.context;
            this.subspaceProvider = store.subspaceProvider;
            this.indexMaintainerRegistry = store.indexMaintainerRegistry;
            this.indexMaintenanceFilter = store.indexMaintenanceFilter;
            this.pipelineSizer = store.pipelineSizer;
            this.storeStateCache = store.storeStateCache;
        }

        @Override
        @Nullable
        public RecordSerializer<Message> getSerializer() {
            return serializer;
        }

        @Override
        @Nonnull
        public Builder setSerializer(@Nonnull RecordSerializer<Message> serializer) {
            this.serializer = serializer;
            return this;
        }

        @Override
        public int getFormatVersion() {
            return formatVersion;
        }

        @Override
        @Nonnull
        public Builder setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        @Override
        @Nullable
        public RecordMetaDataProvider getMetaDataProvider() {
            return metaDataProvider;
        }

        @Override
        @Nonnull
        public Builder setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider) {
            this.metaDataProvider = metaDataProvider;
            return this;
        }

        @Override
        @Nullable
        public FDBMetaDataStore getMetaDataStore() {
            return metaDataStore;
        }

        @Override
        @Nonnull
        public Builder setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore) {
            this.metaDataStore = metaDataStore;
            if (metaDataStore != null && context == null) {
                context = metaDataStore.getRecordContext();
            }
            return this;
        }

        @Override
        @Nullable
        public FDBRecordContext getContext() {
            return context;
        }

        @Override
        @Nonnull
        public Builder setContext(@Nullable FDBRecordContext context) {
            this.context = context;
            return this;
        }

        @Override
        @Nullable
        public SubspaceProvider getSubspaceProvider() {
            return subspaceProvider;
        }

        @Override
        @Nonnull
        public Builder setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider) {
            this.subspaceProvider = subspaceProvider;
            return this;
        }

        @Override
        @Nonnull
        @API(API.Status.UNSTABLE)
        public Builder setSubspace(@Nullable Subspace subspace) {
            this.subspaceProvider = subspace == null ? null : new SubspaceProviderBySubspace(subspace);
            return this;
        }

        /**
         * Sets the {@link KeySpacePath} location of the {@link FDBRecordStore}.
         */
        @Override
        @Nonnull
        public Builder setKeySpacePath(@Nullable KeySpacePath keySpacePath) {
            this.subspaceProvider = keySpacePath == null ? null : new SubspaceProviderByKeySpacePath(keySpacePath);
            return this;
        }

        @Override
        @Nullable
        public UserVersionChecker getUserVersionChecker() {
            return userVersionChecker;
        }

        @Override
        @Nonnull
        public Builder setUserVersionChecker(@Nullable UserVersionChecker userVersionChecker) {
            this.userVersionChecker = userVersionChecker;
            return this;
        }

        @Override
        @Nonnull
        public IndexMaintainerRegistry getIndexMaintainerRegistry() {
            return indexMaintainerRegistry;
        }

        @Override
        @Nonnull
        public Builder setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry) {
            this.indexMaintainerRegistry = indexMaintainerRegistry;
            return this;
        }

        @Override
        @Nonnull
        public IndexMaintenanceFilter getIndexMaintenanceFilter() {
            return indexMaintenanceFilter;
        }

        @Override
        @Nonnull
        public Builder setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            this.indexMaintenanceFilter = indexMaintenanceFilter;
            return this;
        }

        @Override
        @Nonnull
        public PipelineSizer getPipelineSizer() {
            return pipelineSizer;
        }

        @Override
        @Nonnull
        public Builder setPipelineSizer(@Nonnull PipelineSizer pipelineSizer) {
            this.pipelineSizer = pipelineSizer;
            return this;
        }

        @Override
        @Nullable
        public FDBRecordStoreStateCache getStoreStateCache() {
            return storeStateCache;
        }

        @Override
        @Nonnull
        public Builder setStoreStateCache(@Nullable FDBRecordStoreStateCache storeStateCache) {
            this.storeStateCache = storeStateCache;
            return this;
        }

        @Override
        @Nonnull
        public Builder copyBuilder() {
            return new Builder(this);
        }

        @Override
        @Nonnull
        public FDBRecordStore build() {
            if (context == null) {
                throw new RecordCoreException("record context must be supplied");
            }

            if (subspaceProvider == null) {
                throw new RecordCoreException("subspace provider must be supplied");
            }

            if (serializer == null) {
                throw new RecordCoreException("serializer must be supplied");
            }
            return new FDBRecordStore(context, subspaceProvider, formatVersion, getMetaDataProviderForBuild(),
                    serializer, indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer, storeStateCache);
        }

        @Override
        @Nonnull
        public CompletableFuture<FDBRecordStore> uncheckedOpenAsync() {
            final CompletableFuture<Void> preloadMetaData = preloadMetaData();
            FDBRecordStore recordStore = build();
            final CompletableFuture<Void> subspaceFuture = recordStore.preloadSubspaceAsync();
            final CompletableFuture<Void> loadStoreState = subspaceFuture.thenCompose(vignore -> recordStore.preloadRecordStoreStateAsync());
            return CompletableFuture.allOf(preloadMetaData, loadStoreState).thenApply(vignore -> recordStore);
        }

        @Override
        @Nonnull
        public CompletableFuture<FDBRecordStore> createOrOpenAsync(@Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
            // Might be as many as four reads: meta-data store, keyspace path, store index state, store info header.
            // Try to do them as much in parallel as possible.
            final CompletableFuture<Void> preloadMetaData = preloadMetaData();
            FDBRecordStore recordStore = build();
            final CompletableFuture<Boolean> checkVersion = recordStore.checkVersion(userVersionChecker, existenceCheck, preloadMetaData);
            return checkVersion.thenApply(vignore -> recordStore);
        }

        @Nonnull
        private RecordMetaDataProvider getMetaDataProviderForBuild() {
            if (metaDataStore != null) {
                return metaDataStore;
            } else if (metaDataProvider != null) {
                return metaDataProvider;
            } else {
                throw new RecordCoreException("Neither metaDataStore nor metaDataProvider was set in builder.");
            }
        }

        @Nonnull
        private CompletableFuture<Void> preloadMetaData() {
            if (metaDataStore != null) {
                return metaDataStore.preloadMetaData(metaDataProvider);
            } else {
                return AsyncUtil.DONE;
            }
        }
    }
}
