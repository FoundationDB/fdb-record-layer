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
import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.AggregateFunctionNotSupportedException;
import com.apple.foundationdb.record.ByteScanLimiter;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.MutableRecordStoreState;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
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
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeOrBuilder;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerializationRegistry;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.LoggableException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
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
    public static final PipelineSizer DEFAULT_PIPELINE_SIZER = pipelineOperation -> {
        if (pipelineOperation == PipelineOperation.UPDATE ||
                pipelineOperation == PipelineOperation.INSERT ||
                pipelineOperation == PipelineOperation.DELETE) {
            return 1;
        } else {
            return DEFAULT_PIPELINE_SIZE;
        }
    };

    // The maximum number of records to allow before triggering online index builds
    // instead of a transactional rebuild.
    public static final int MAX_RECORDS_FOR_REBUILD = 200;

    // The maximum number of index rebuilds to run in parallel
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
    // 7 - allow the record store state to be cached and invalidated with the meta-data version key
    public static final int CACHEABLE_STATE_FORMAT_VERSION = 7;
    // 8 - add custom fields to store header
    public static final int HEADER_USER_FIELDS_FORMAT_VERSION = 8;
    // 9 - add READABLE_UNIQUE_PENDING index state
    public static final int READABLE_UNIQUE_PENDING_FORMAT_VERSION = 9;
    // 10 - check index build type during update
    public static final int CHECK_INDEX_BUILD_TYPE_DURING_UPDATE_FORMAT_VERSION = 10;

    // The current code can read and write up to the format version below
    public static final int MAX_SUPPORTED_FORMAT_VERSION = CHECK_INDEX_BUILD_TYPE_DURING_UPDATE_FORMAT_VERSION;

    // By default, record stores attempt to upgrade to this version
    // NOTE: Updating this can break certain users during upgrades.
    // See: https://github.com/FoundationDB/fdb-record-layer/issues/709
    public static final int DEFAULT_FORMAT_VERSION = CACHEABLE_STATE_FORMAT_VERSION;

    // These agree with the client's values. They could be tunable and even increased with knobs.
    public static final int KEY_SIZE_LIMIT = 10_000;
    public static final int VALUE_SIZE_LIMIT = 100_000;

    // The size of preload cache
    private static final int PRELOAD_CACHE_SIZE = 100;

    @Nonnull
    private static final CompletableFuture<IndexState> READY_READABLE = CompletableFuture.completedFuture(IndexState.READABLE);

    protected static final Object STORE_INFO_KEY = FDBRecordStoreKeyspace.STORE_INFO.key();
    protected static final Object RECORD_KEY = FDBRecordStoreKeyspace.RECORD.key();
    protected static final Object INDEX_KEY = FDBRecordStoreKeyspace.INDEX.key();
    protected static final Object INDEX_SECONDARY_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE.key();
    protected static final Object RECORD_COUNT_KEY = FDBRecordStoreKeyspace.RECORD_COUNT.key();
    protected static final Object INDEX_STATE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_STATE_SPACE.key();
    protected static final Object INDEX_RANGE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_RANGE_SPACE.key();
    protected static final Object INDEX_UNIQUENESS_VIOLATIONS_KEY = FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE.key();
    protected static final Object RECORD_VERSION_KEY = FDBRecordStoreKeyspace.RECORD_VERSION_SPACE.key();
    protected static final Object INDEX_BUILD_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_BUILD_SPACE.key();

    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_ONE = { 1, 0, 0, 0, 0, 0, 0, 0 };
    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] LITTLE_ENDIAN_INT64_MINUS_ONE = { -1, -1, -1, -1, -1, -1, -1, -1 };
    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] INT64_ZERO = { 0, 0, 0, 0, 0, 0, 0, 0 };

    protected int formatVersion;
    protected int userVersion;

    private boolean omitUnsplitRecordSuffix;

    @Nonnull
    protected final RecordMetaDataProvider metaDataProvider;

    private volatile boolean versionChanged;

    @Nonnull
    protected final AtomicReference<MutableRecordStoreState> recordStoreStateRef = new AtomicReference<>();

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
    @Nonnull
    protected final StateCacheabilityOnOpen stateCacheabilityOnOpen;

    @Nullable
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;

    @Nullable
    private Subspace cachedRecordsSubspace;

    @Nonnull
    private final FDBPreloadRecordCache preloadCache;

    private boolean recordsReadConflict;

    private boolean storeStateReadConflict;
    private IndexDeferredMaintenanceControl indexDeferredMaintenanceControl;

    @Nonnull
    private final Set<String> indexStateReadConflicts = ConcurrentHashMap.newKeySet(8);

    @Nonnull
    private final PlanSerializationRegistry planSerializationRegistry;

    @SuppressWarnings("squid:S00107")
    protected FDBRecordStore(@Nonnull FDBRecordContext context,
                             @Nonnull SubspaceProvider subspaceProvider,
                             int formatVersion,
                             @Nonnull RecordMetaDataProvider metaDataProvider,
                             @Nonnull RecordSerializer<Message> serializer,
                             @Nonnull IndexMaintainerRegistry indexMaintainerRegistry,
                             @Nonnull IndexMaintenanceFilter indexMaintenanceFilter,
                             @Nonnull PipelineSizer pipelineSizer,
                             @Nullable FDBRecordStoreStateCache storeStateCache,
                             @Nonnull StateCacheabilityOnOpen stateCacheabilityOnOpen,
                             @Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                             @Nonnull PlanSerializationRegistry planSerializationRegistry) {
        super(context, subspaceProvider);
        this.formatVersion = formatVersion;
        this.metaDataProvider = metaDataProvider;
        this.serializer = serializer;
        this.indexMaintainerRegistry = indexMaintainerRegistry;
        this.indexMaintenanceFilter = indexMaintenanceFilter;
        this.pipelineSizer = pipelineSizer;
        this.storeStateCache = storeStateCache;
        this.stateCacheabilityOnOpen = stateCacheabilityOnOpen;
        this.userVersionChecker = userVersionChecker;
        this.omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
        this.preloadCache = new FDBPreloadRecordCache(PRELOAD_CACHE_SIZE);
        this.planSerializationRegistry = planSerializationRegistry;
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
        return useOldVersionFormat(getFormatVersion(), omitUnsplitRecordSuffix);
    }

    private static boolean useOldVersionFormat(int formatVersion, boolean omitUnsplitRecordSuffix) {
        // If the store is either explicitly using the older format version or if
        // it is using a newer one, but because of how the data were originally stored
        // in this record store, then use the older location for record versions.
        return formatVersion < SAVE_VERSION_WITH_RECORD_FORMAT_VERSION || omitUnsplitRecordSuffix;
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
     * Returns {@code true} if the RecordMetadata version is changed in the RecordStore. Alternatively, returns
     * {@code false} if the version is either not checked (checkVersion() not called) or it is up-to-date.
     * @return the versionChanged boolean
     */
    public boolean isVersionChanged() {
        return versionChanged;
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
    @Override
    @Nonnull
    public RecordStoreState getRecordStoreState() {
        if (recordStoreStateRef.get() == null) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_STORE_STATE,
                    preloadRecordStoreStateAsync(StoreExistenceCheck.NONE, IsolationLevel.SERIALIZABLE, IsolationLevel.SNAPSHOT));
        }
        return recordStoreStateRef.get();
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
     * Method to return the {@link PlanSerializationRegistry} in use.
     * @return the current {@link PlanSerializationRegistry}
     */
    @Nonnull
    public PlanSerializationRegistry getPlanSerializationRegistry() {
        return planSerializationRegistry;
    }

    /**
     * Async version of {@link #saveRecord(Message, RecordExistenceCheck, FDBRecordVersion, VersionstampSaveBehavior)}.
     * @param rec the record to save
     * @param existenceCheck when to throw an exception if a record with the same primary key does or does not already exist
     * @param version the associated record version
     * @param behavior the save behavior w.r.t. the given <code>version</code>
     * @return a future that completes with the stored record form of the saved record
     */
    @Override
    @Nonnull
    public CompletableFuture<FDBStoredRecord<Message>> saveRecordAsync(@Nonnull final Message rec, @Nonnull RecordExistenceCheck existenceCheck,
                                                                       @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        return saveTypedRecord(serializer, rec, existenceCheck, version, behavior);
    }

    @Override
    @Nonnull
    public CompletableFuture<FDBStoredRecord<Message>> dryRunSaveRecordAsync(@Nonnull final Message rec, @Nonnull RecordExistenceCheck existenceCheck,
                                                                             @Nullable FDBRecordVersion version, @Nonnull VersionstampSaveBehavior behavior) {
        return saveTypedRecord(serializer, rec, existenceCheck, null, VersionstampSaveBehavior.DEFAULT, true);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    protected <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                        @Nonnull M rec,
                                                                                        @Nonnull RecordExistenceCheck existenceCheck,
                                                                                        @Nullable FDBRecordVersion version,
                                                                                        @Nonnull VersionstampSaveBehavior behavior) {
        return saveTypedRecord(typedSerializer, rec, existenceCheck, version, behavior, false);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    protected <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                                        @Nonnull M rec,
                                                                                        @Nonnull RecordExistenceCheck existenceCheck,
                                                                                        @Nullable FDBRecordVersion version,
                                                                                        @Nonnull VersionstampSaveBehavior behavior,
                                                                                        boolean isDryRun) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Descriptors.Descriptor recordDescriptor = rec.getDescriptorForType();
        final RecordType recordType = metaData.getRecordTypeForDescriptor(recordDescriptor);
        final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();

        final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
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
            if (isDryRun) {
                final FDBStoredRecord<M> newRecord = dryRunSetSizeInfo(typedSerializer, recordBuilder, metaData);
                return CompletableFuture.completedFuture(newRecord);
            } else {
                final FDBStoredRecord<M> newRecord = serializeAndSaveRecord(typedSerializer, recordBuilder, metaData, oldRecord);
                if (oldRecord == null) {
                    addRecordCount(metaData, newRecord, LITTLE_ENDIAN_INT64_ONE);
                } else {
                    if (getTimer() != null) {
                        getTimer().increment(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES, oldRecord.getValueSize());
                    }
                }
                return updateSecondaryIndexes(oldRecord, newRecord).thenApply(v -> newRecord);
            }
        });
        return context.instrument(FDBStoreTimer.Events.SAVE_RECORD, result);
    }

    @SuppressWarnings("PMD.CloseResource")
    private <M extends Message> void addRecordCount(@Nonnull RecordMetaData metaData, @Nonnull FDBStoredRecord<M> rec, @Nonnull byte[] increment) {
        if (metaData.getRecordCountKey() == null) {
            return;
        }
        final Transaction tr = ensureContextActive();
        Key.Evaluated subkey = metaData.getRecordCountKey().evaluateSingleton(rec);
        final byte[] keyBytes = getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(subkey.toTupleAppropriateList()));
        tr.mutate(MutationType.ADD, keyBytes, increment);
    }

    @Nullable
    private FDBRecordVersion recordVersionForSave(@Nonnull RecordMetaData metaData, @Nullable FDBRecordVersion version, @Nonnull final VersionstampSaveBehavior behavior) {
        if (behavior.equals(VersionstampSaveBehavior.NO_VERSION)) {
            if (version != null) {
                throw recordCoreException("Nonnull version supplied with a NO_VERSION behavior: " + version);
            }
            return null;
        } else if (behavior.equals(VersionstampSaveBehavior.IF_PRESENT)) {
            return version;
        } else if (version == null && (behavior.equals(VersionstampSaveBehavior.WITH_VERSION) || metaData.isStoreRecordVersions())) {
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
    private <M extends Message> FDBStoredRecord<M> dryRunSetSizeInfo(@Nonnull RecordSerializer<M> typedSerializer, @Nonnull final FDBStoredRecordBuilder<M> recordBuilder,
                                                                          @Nonnull final RecordMetaData metaData) {
        final FDBRecordVersion version = recordBuilder.getVersion();
        final byte[] serialized = typedSerializer.serialize(metaData, recordBuilder.getRecordType(), recordBuilder.getRecord(), getTimer());
        final FDBRecordVersion splitVersion = useOldVersionFormat() ? null : version;
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        SplitHelper.dryRunSaveWithSplitOnlySetSizeInfo(recordsSubspace(), recordBuilder.getPrimaryKey(), serialized, splitVersion, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix, sizeInfo);
        recordBuilder.setSize(sizeInfo);
        return recordBuilder.build();
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
            context.addToLocalVersionCache(versionKey, version.getLocalVersion());
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private <M extends Message> CompletableFuture<Void> updateSecondaryIndexes(@Nullable final FDBStoredRecord<M> oldRecord,
                                                                               @Nullable final FDBStoredRecord<M> newRecord) {
        if (oldRecord == null && newRecord == null) {
            return AsyncUtil.DONE;
        }
        if (recordStoreStateRef.get() == null) {
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
        beginRecordStoreStateRead();
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
            if (!getRecordMetaData().getSyntheticRecordTypes().isEmpty()) {
                updateSyntheticIndexes(oldRecord, newRecord, futures);
            }
            haveFuture = true;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateRead();
            }
        }
        if (futures.isEmpty()) {
            endRecordStoreStateRead();
            return AsyncUtil.DONE;
        } else if (futures.size() == 1) {
            return futures.get(0).whenComplete((v, t) -> endRecordStoreStateRead());
        } else {
            return AsyncUtil.whenAll(futures).whenComplete((v, t) -> endRecordStoreStateRead());
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
            if (isIndexWriteOnly(index)) {
                // In this case, the index is still being built. For some index
                // types, the index update needs to check whether indexing
                // process has already built the relevant ranges, and it
                // may adjust the way the index is built in response.
                future = maintainer.updateWhileWriteOnly(oldRecord, newRecord);
            } else {
                future = maintainer.update(oldRecord, newRecord);
            }
            if (!MoreAsyncUtil.isCompletedNormally(future)) {
                futures.add(future);
            }
        }
    }

    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private <M extends Message> void updateSyntheticIndexes(@Nullable FDBStoredRecord<M> oldRecord,
                                                            @Nullable FDBStoredRecord<M> newRecord,
                                                            @Nonnull final List<CompletableFuture<Void>> futures) {
        final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(this);
        // Index maintainers are not required to be thread-safe, so only do one synthetic record at a time.
        final int pipelineSize = 1;
        if (oldRecord != null && newRecord != null && oldRecord.getRecordType() == newRecord.getRecordType()) {
            // TODO: An important optimization here is determining that no field used in the join condition or
            //  indexed in the synthetic record is changed, in which case all this can be skipped.
            final SyntheticRecordFromStoredRecordPlan plan = planner.fromStoredType(newRecord.getRecordType(), true);
            if (plan == null) {
                return;
            }
            final Map<RecordType, Collection<IndexMaintainer>> maintainers = getSyntheticMaintainers(plan.getSyntheticRecordTypes());
            final Map<Tuple, FDBSyntheticRecord> oldRecords = new ConcurrentHashMap<>();
            CompletableFuture<Void> future = plan.execute(this, oldRecord).forEach(syntheticRecord -> oldRecords.put(syntheticRecord.getPrimaryKey(), syntheticRecord));
            @Nonnull final FDBStoredRecord<M> theNewRecord = newRecord; // @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "https://github.com/spotbugs/spotbugs/issues/552")
            future = future.thenCompose(v -> plan.execute(this, theNewRecord).forEachAsync(syntheticRecord -> runSyntheticMaintainers(maintainers, oldRecords.remove(syntheticRecord.getPrimaryKey()), syntheticRecord), pipelineSize));
            future = future.thenCompose(v -> {
                // Any synthetic record that was generated by the plan on the old record but not by the plan on the new record needs to be removed from its indexes.
                final List<CompletableFuture<Void>> subFutures = new ArrayList<>();
                for (FDBSyntheticRecord oldSyntheticRecord : oldRecords.values()) {
                    CompletableFuture<Void> subFuture = runSyntheticMaintainers(maintainers, oldSyntheticRecord, null);
                    if (!MoreAsyncUtil.isCompletedNormally(subFuture)) {
                        subFutures.add(subFuture);
                    }
                }
                if (subFutures.isEmpty()) {
                    return AsyncUtil.DONE;
                } else if (subFutures.size() == 1) {
                    return subFutures.get(0);
                } else {
                    return AsyncUtil.whenAll(subFutures);
                }
            });
            futures.add(future);
        } else {
            if (oldRecord != null) {
                final SyntheticRecordFromStoredRecordPlan plan = planner.fromStoredType(oldRecord.getRecordType(), true);
                if (plan != null) {
                    final Map<RecordType, Collection<IndexMaintainer>> maintainers = getSyntheticMaintainers(plan.getSyntheticRecordTypes());
                    if (!maintainers.isEmpty()) {
                        futures.add(plan.execute(this, oldRecord).forEachAsync(syntheticRecord -> runSyntheticMaintainers(maintainers, syntheticRecord, null), pipelineSize));
                    }
                }
            }
            if (newRecord != null) {
                final SyntheticRecordFromStoredRecordPlan plan = planner.fromStoredType(newRecord.getRecordType(), true);
                if (plan != null) {
                    final Map<RecordType, Collection<IndexMaintainer>> maintainers = getSyntheticMaintainers(plan.getSyntheticRecordTypes());
                    if (!maintainers.isEmpty()) {
                        futures.add(plan.execute(this, newRecord).forEachAsync(syntheticRecord -> runSyntheticMaintainers(maintainers, null, syntheticRecord), pipelineSize));
                    }
                }
            }
        }
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    private Map<RecordType, Collection<IndexMaintainer>> getSyntheticMaintainers(@Nonnull Set<String> syntheticRecordTypes) {
        final RecordMetaData metaData = getRecordMetaData();
        return syntheticRecordTypes.stream()
                .map(metaData::getSyntheticRecordType).collect(Collectors.toMap(Function.identity(), syntheticRecordType -> {
                    List<IndexMaintainer> indexMaintainers = new ArrayList<>();
                    syntheticRecordType.getIndexes().stream().filter(index -> !isIndexDisabled(index)).map(this::getIndexMaintainer).forEach(indexMaintainers::add);
                    syntheticRecordType.getMultiTypeIndexes().stream().filter(index -> !isIndexDisabled(index)).map(this::getIndexMaintainer).forEach(indexMaintainers::add);
                    return indexMaintainers;
                }));
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    private CompletableFuture<Void> runSyntheticMaintainers(@Nonnull Map<RecordType, Collection<IndexMaintainer>> maintainers, @Nullable FDBSyntheticRecord oldRecord, @Nullable FDBSyntheticRecord newRecord) {
        if (oldRecord == null && newRecord == null) {
            return AsyncUtil.DONE;
        }
        final RecordType recordType = oldRecord != null ? oldRecord.getRecordType() : newRecord.getRecordType();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (IndexMaintainer indexMaintainer : maintainers.get(recordType)) {
            CompletableFuture<Void> future = indexMaintainer.update(oldRecord, newRecord);
            if (!MoreAsyncUtil.isCompletedNormally(future)) {
                futures.add(future);
            }
        }
        if (futures.isEmpty()) {
            return AsyncUtil.DONE;
        } else if (futures.size() == 1) {
            return futures.get(0);
        } else {
            return AsyncUtil.whenAll(futures);
        }
    }

    /**
     * Load a {@link FDBSyntheticRecord synthetic record} by loading its stored constituent records and synthesizing it from them.
     * @param primaryKey the primary key of the synthetic record, which includes the primary keys of the constituents
     * @return a future which completes to the synthesized record
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    @Override
    public CompletableFuture<FDBSyntheticRecord> loadSyntheticRecord(@Nonnull Tuple primaryKey) {
        SyntheticRecordType<?> syntheticRecordType = getRecordMetaData().getSyntheticRecordTypeFromRecordTypeKey(primaryKey.get(0));
        if (syntheticRecordType.getConstituents().size() != primaryKey.size() - 1) {
            throw recordCoreException("Primary key does not have correct number of nested keys: " + primaryKey);
        }
        return syntheticRecordType.loadByPrimaryKeyAsync(this, primaryKey);
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
     * Subspace for index to store information for online index build.
     * @param index the index to retrieve the build information for
     * @return the subspace for the build information of the given index
     */
    @Nonnull
    Subspace indexBuildSubspace(@Nonnull Index index) {
        return getSubspace().subspace(Tuple.from(INDEX_BUILD_SPACE_KEY, index.getSubspaceTupleKey()));
    }

    /**
     * Get the maintainer for a given index.
     * @param index the required index
     * @return the maintainer for the given index
     */
    @Nonnull
    @Override
    public IndexMaintainer getIndexMaintainer(@Nonnull Index index) {
        return indexMaintainerRegistry.getIndexMaintainer(new IndexMaintainerState(this, index, indexMaintenanceFilter));
    }

    @Nonnull
    public PlanSerializationContext newPlanSerializationContext(@Nonnull final PlanHashable.PlanHashMode mode) {
        return new PlanSerializationContext(planSerializationRegistry, mode);
    }

    public int getKeySizeLimit() {
        return KEY_SIZE_LIMIT;
    }

    public int getValueSizeLimit() {
        return VALUE_SIZE_LIMIT;
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
            byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
            Optional<CompletableFuture<FDBRecordVersion>> cachedOptional = context.getLocalVersion(versionKey)
                    .map(localVersion -> CompletableFuture.completedFuture(FDBRecordVersion.incomplete(localVersion)));
            if (cachedOptional.isPresent()) {
                return cachedOptional;
            }

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
            final M protoRecord = typedSerializer.deserialize(metaData, primaryKey, rawRecord.getRawRecord(), getTimer());
            final RecordType recordType = metaData.getRecordTypeForDescriptor(protoRecord.getDescriptorForType());
            countKeysAndValues(FDBStoreTimer.Counts.LOAD_RECORD_KEY, FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES, FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES,
                    rawRecord);

            final FDBStoredRecordBuilder<M> recordBuilder = FDBStoredRecord.newBuilder(protoRecord)
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
                    LogMessageKeys.PRIMARY_KEY, primaryKey,
                    LogMessageKeys.META_DATA_VERSION, metaData.getVersion());
            if (LOGGER.isDebugEnabled()) {
                ex2.addLogInfo("serialized", ByteArrayUtil2.loggable(serialized));
            }
            if (LOGGER.isTraceEnabled()) {
                ex2.addLogInfo("descriptor", metaData.getUnionDescriptor().getFile().toProto());
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
        FDBPreloadRecordCache.Future futureRecord = preloadCache.beginPrefetch(primaryKey);
        return loadRawRecordAsync(primaryKey, null, false)
                .whenComplete((rawRecord, ex) -> {
                    if (ex != null) {
                        futureRecord.cancel();
                    } else {
                        futureRecord.complete(rawRecord);
                    }
                })
                .thenApply(rawRecord -> null);
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> recordExistsAsync(@Nonnull final Tuple primaryKey, @Nonnull final IsolationLevel isolationLevel) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final ReadTransaction tr = isolationLevel.isSnapshot() ? ensureContextActive().snapshot() : ensureContextActive();
        return SplitHelper.keyExists(tr, context, recordsSubspace(),
                primaryKey, metaData.isSplitLongRecords(), omitUnsplitRecordSuffix);
    }

    @Nonnull
    private Range getRangeForRecord(@Nonnull Tuple primaryKey) {
        return TupleRange.allOf(primaryKey).toRange(recordsSubspace());
    }

    @Override
    public void addRecordReadConflict(@Nonnull Tuple primaryKey) {
        final Range recordRange = getRangeForRecord(primaryKey);
        ensureContextActive().addReadConflictRange(recordRange.begin, recordRange.end);
    }

    @Override
    public void addRecordWriteConflict(@Nonnull Tuple primaryKey) {
        final Range recordRange = getRangeForRecord(primaryKey);
        ensureContextActive().addWriteConflictRange(recordRange.begin, recordRange.end);
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
        final FDBPreloadRecordCache.Entry entry = preloadCache.get(primaryKey);
        if (entry != null) {
            return CompletableFuture.completedFuture(entry.orElse(null));
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
    @SuppressWarnings("PMD.CloseResource")
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
                final ScanProperties finalScanProperties = scanProperties
                        .with(executeProperties -> {
                            final ExecuteProperties.Builder builder = executeProperties.toBuilder()
                                    .clearTimeLimit()
                                    .clearSkipAndAdjustLimit()
                                    .clearState();
                            int returnedRowLimit = builder.getReturnedRowLimitOrMax();
                            if (returnedRowLimit != Integer.MAX_VALUE) {
                                // Adjust limit to twice the supplied limit in case there are versions in the records
                                builder.setReturnedRowLimit(2 * returnedRowLimit);
                            }
                            return builder.build();
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
    @SuppressWarnings("PMD.CloseResource")
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
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanBounds scanBounds,
                                              @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        if (!isIndexScannable(index)) {
            throw new ScanNonReadableIndexException("Cannot scan non-readable index",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        }
        RecordCursor<IndexEntry> result = getIndexMaintainer(index)
                .scan(scanBounds, continuation, scanProperties);
        return context.instrument(FDBStoreTimer.Events.SCAN_INDEX_KEYS, result);
    }

    @Nonnull
    @Override
    public RecordCursor<FDBIndexedRecord<Message>> scanIndexRemoteFetch(@Nonnull Index index,
                                                                        @Nonnull IndexScanBounds scanBounds,
                                                                        int commonPrimaryKeyLength,
                                                                        @Nullable byte[] continuation,
                                                                        @Nonnull ScanProperties scanProperties,
                                                                        @Nonnull final IndexOrphanBehavior orphanBehavior) {
        return scanIndexRemoteFetchInternal(index, scanBounds, commonPrimaryKeyLength, continuation, serializer, scanProperties, orphanBehavior);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    protected <M extends Message> RecordCursor<FDBIndexedRecord<M>> scanIndexRemoteFetchInternal(@Nonnull final Index index,
                                                                                                 @Nonnull final IndexScanBounds scanBounds,
                                                                                                 int commonPrimaryKeyLength,
                                                                                                 @Nullable final byte[] continuation,
                                                                                                 @Nonnull RecordSerializer<M> typedSerializer,
                                                                                                 @Nonnull final ScanProperties scanProperties,
                                                                                                 @Nonnull final IndexOrphanBehavior orphanBehavior) {
        // Note that even though it is legal to have 0-len PK, we actually require >0 for remote fetch
        if (commonPrimaryKeyLength <= 0) {
            throw new RecordCoreArgumentException("commonPrimaryKeyLength has to be a positive number", LogMessageKeys.INDEX_NAME, index.getName());
        }
        if (!isIndexScannable(index)) {
            throw new ScanNonReadableIndexException("Cannot scan non-readable index",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        }
        if (!scanBounds.getScanType().equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreArgumentException("Index remote fetch can only be used with VALUE index scan.",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        }
        if (!getContext().getAPIVersion().isAtLeast(APIVersion.API_VERSION_7_1)) {
            throw new UnsupportedMethodException("Index Remote Fetch can only be used with API_VERSION of at least 7.1.");
        }

        IndexMaintainer indexMaintainer = getIndexMaintainer(index);
        // Get the cursor with the index entries and the records from FDB
        RecordCursor<FDBIndexedRawRecord> indexEntries = indexMaintainer.scanRemoteFetch(scanBounds, continuation, scanProperties, commonPrimaryKeyLength);
        // Parse the index entries and payload and build records
        RecordCursor<FDBIndexedRecord<M>> indexedRecordCursor = indexEntriesToIndexRecords(scanProperties, orphanBehavior, indexEntries, typedSerializer);

        return context.instrument(FDBStoreTimer.Events.SCAN_REMOTE_FETCH_ENTRY, indexedRecordCursor);
    }

    @VisibleForTesting
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    <M extends Message> RecordCursor<FDBIndexedRecord<M>> indexEntriesToIndexRecords(@Nonnull final ScanProperties scanProperties,
                                                                                     @Nonnull final IndexOrphanBehavior orphanBehavior,
                                                                                     @Nonnull final RecordCursor<FDBIndexedRawRecord> indexEntries,
                                                                                     @Nonnull RecordSerializer<M> typedSerializer) {
        ByteScanLimiter byteScanLimiter = scanProperties.getExecuteProperties().getState().getByteScanLimiter();
        RecordCursor<FDBIndexedRecord<M>> indexedRecordCursor = indexEntries.mapPipelined(indexedRawRecord -> {
            CompletableFuture<FDBIndexedRecord<M>> indexedRecordFuture = buildSingleRecordInternal(indexedRawRecord, typedSerializer, byteScanLimiter);
            return indexedRecordFuture.thenApply(record -> {
                if (record == null) {
                    // This is the case that there were no record parts in the RawRecord, and so the index entry is attached to
                    // an empty record and returned
                    return handleOrphanEntry(indexedRawRecord.getIndexEntry(), orphanBehavior);
                } else {
                    return record;
                }
            });
        }, 1);

        if (orphanBehavior == IndexOrphanBehavior.SKIP) {
            indexedRecordCursor = indexedRecordCursor.filter(Objects::nonNull);
        }
        return indexedRecordCursor;
    }

    @Override
    @Nonnull
    public CompletableFuture<FDBIndexedRecord<Message>> buildSingleRecord(@Nonnull FDBIndexedRawRecord indexedRawRecord) {
        return buildSingleRecordInternal(indexedRawRecord, serializer, null);
    }

    protected <M extends Message> CompletableFuture<FDBIndexedRecord<M>> buildSingleRecordInternal(@Nonnull FDBIndexedRawRecord indexedRawRecord,
                                                                                                 @Nonnull RecordSerializer<M> typedSerializer,
                                                                                                 @Nullable final ByteScanLimiter byteScanLimiter) {
        SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        // Use the raw record entries to reconstruct the original raw record (include all splits and version, if applicable)
        FDBRawRecord fdbRawRecord = reconstructSingleRecord(recordsSubspace(), sizeInfo, indexedRawRecord.getRawRecord(), useOldVersionFormat());
        if (fdbRawRecord == null) {
            return CompletableFuture.completedFuture(null);
        }
        Optional<CompletableFuture<FDBRecordVersion>> versionFutureOptional = Optional.empty();
        // The version future will be ignored in case the record already has a version
        if (useOldVersionFormat() && !fdbRawRecord.hasVersion()) {
            versionFutureOptional = loadRecordVersionAsync(indexedRawRecord.getIndexEntry().getPrimaryKey());
        }
        if (byteScanLimiter != null) {
            // Only count the bytes of the RawRecord - the index entry bytes are accounted for by the KeyValueCursor
            byteScanLimiter.registerScannedBytes((long)sizeInfo.getKeySize() + (long)sizeInfo.getValueSize());
        }

        // Deserialize the raw record
        CompletableFuture<FDBStoredRecord<M>> storedRecord = deserializeRecord(typedSerializer, fdbRawRecord, metaDataProvider.getRecordMetaData(), versionFutureOptional);
        return storedRecord.thenApply(rec -> new FDBIndexedRecord<>(indexedRawRecord.getIndexEntry(), rec));
    }

    private <M extends Message> FDBIndexedRecord<M> handleOrphanEntry(final IndexEntry indexEntry, final IndexOrphanBehavior orphanBehavior) {
        switch (orphanBehavior) {
            case SKIP:
                return null;
            case RETURN:
                return new FDBIndexedRecord<>(indexEntry, null);
            case ERROR:
                if (getTimer() != null) {
                    getTimer().increment(FDBStoreTimer.Counts.BAD_INDEX_ENTRY);
                }
                throw new RecordCoreStorageException("record not found for prefetched index entry").addLogInfo(
                        LogMessageKeys.INDEX_NAME, indexEntry.getIndex().getName(),
                        LogMessageKeys.PRIMARY_KEY, indexEntry.getPrimaryKey(),
                        LogMessageKeys.INDEX_KEY, indexEntry.getKey(),
                        getSubspaceProvider().logKey(), getSubspaceProvider().toString(getContext()));
            default:
                throw new RecordCoreException("Unexpected index orphan behavior: " + orphanBehavior);
        }
    }

    /**
     * Create and return an instance of {@link FDBRawRecord} from a given {@link MappedKeyValue}. The given
     * parameter represents a series of record splits (and optional version), that the method will "unsplit" and reconstruct
     * into a single raw record.
     * @param recordSubspace the subspace for the record to allow the record keys to be identified from the splits
     * @param sizeInfo Size Info to collect metrics
     * @param mappedResult the record splits, packed into a KeyValueAndMappedReqAndResult
     * @param oldVersionFormat whether to use the old version record format when reading the records
     * @return an instance of {@link FDBRawRecord} reconstructed from the given record splits, null if no record entries found
     */
    @Nullable
    @SuppressWarnings("PMD.CloseResource")
    private FDBRawRecord reconstructSingleRecord(final Subspace recordSubspace, final SplitHelper.SizeInfo sizeInfo,
                                                 final MappedKeyValue mappedResult, final boolean oldVersionFormat) {
        List<KeyValue> scannedRange = mappedResult.getRangeResult();
        if ((scannedRange == null) || scannedRange.isEmpty()) {
            return null;
        }

        ListCursor<KeyValue> rangeCursor = new ListCursor<>(scannedRange, null);
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final RecordCursor<FDBRawRecord> rawRecords;
        if (metaData.isSplitLongRecords()) {
            // Note that we always set "reverse" to false since regardless of the index scan direction, the MappedKeyValue is in non-reverse order
            // Setting the limit manager to UNTRACKED since the KeyValueCursor already counts records, and we don't want the unsplitter to double-count
            rawRecords = new SplitHelper.KeyValueUnsplitter(context, recordSubspace, rangeCursor, oldVersionFormat,
                    sizeInfo, false, CursorLimitManager.UNTRACKED);
        } else {
            if (omitUnsplitRecordSuffix) {
                rawRecords = rangeCursor.map(kv -> {
                    sizeInfo.set(kv);
                    Tuple primaryKey = SplitHelper.unpackKey(recordSubspace, kv);
                    return new FDBRawRecord(primaryKey, kv.getValue(), null, sizeInfo);
                });
            } else {
                // Note that we always set "reverse" to false since regardless of the index scan direction, the MappedKeyValue is in non-reverse order
                // Setting the limit manager to UNTRACKED since the KeyValueCursor already counts records, and we don't want the unsplitter to double-count
                rawRecords = new SplitHelper.KeyValueUnsplitter(context, recordSubspace, rangeCursor, oldVersionFormat,
                        sizeInfo, false, CursorLimitManager.UNTRACKED);
            }
        }
        // Everything is synchronous, just get the only value from the cursor
        return rawRecords.getNext().get();
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull TupleRange range,
                                                                                 @Nullable byte[] continuation,
                                                                                 @Nonnull ScanProperties scanProperties) {
        if (!index.isUnique()) {
            return RecordCursor.empty(getExecutor());
        }
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
    public CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable Tuple remainPrimaryKey) {
        return scanUniquenessViolations(index, valueKey).forEachAsync(uniquenessViolation -> {
            if (remainPrimaryKey == null || !remainPrimaryKey.equals(uniquenessViolation.getPrimaryKey())) {
                return deleteRecordAsync(uniquenessViolation.getPrimaryKey()).thenApply(ignore -> null);
            } else {
                // The uniqueness violation entry of the remained primary key will be removed as part of
                // removeUniquenessViolationsAsync when deleting the second to last record that contains the value key.
                return AsyncUtil.DONE;
            }
        }, getPipelineSize(PipelineOperation.RESOLVE_UNIQUENESS));
    }

    @API(API.Status.INTERNAL)
    public void addIndexUniquenessCommitCheck(@Nonnull Index index, @Nonnull CompletableFuture<Void> check) {
        IndexUniquenessCommitCheck commitCheck = new IndexUniquenessCommitCheck(index, check);
        getRecordContext().addCommitCheck(commitCheck);
    }

    /**
     * Return a future that is ready when all uniqueness commit checks for the given index have completed.
     * This is needed because the uniqueness checks happen in the background, and if we rebuild an index in a
     * single transaction, we need to make sure that those checks have completed prior to trying to mark the
     * index as readable.
     *
     * @param index the index to collect commit checks for
     * @return a future that will complete when all outstanding uniqueness checks for the index have completed
     */
    @Nonnull
    @VisibleForTesting
    CompletableFuture<Void> whenAllIndexUniquenessCommitChecks(@Nonnull Index index) {
        List<FDBRecordContext.CommitCheckAsync> indexUniquenessChecks = getRecordContext().getCommitChecks(commitCheck -> {
            if (commitCheck instanceof IndexUniquenessCommitCheck) {
                return ((IndexUniquenessCommitCheck)commitCheck).getIndex().equals(index);
            } else {
                return false;
            }
        });
        return AsyncUtil.whenAll(indexUniquenessChecks.stream()
                .map(FDBRecordContext.CommitCheckAsync::checkAsync)
                .collect(Collectors.toList()));
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> dryRunDeleteRecordAsync(@Nonnull final Tuple primaryKey) {
        return deleteTypedRecord(serializer, primaryKey, true);
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> deleteRecordAsync(@Nonnull final Tuple primaryKey) {
        return deleteTypedRecord(serializer, primaryKey, false);
    }

    @Nonnull
    protected <M extends Message> CompletableFuture<Boolean> deleteTypedRecord(@Nonnull RecordSerializer<M> typedSerializer,
                                                                               @Nonnull Tuple primaryKey, boolean isDryRun) {
        if (isDryRun) {
            return loadTypedRecord(typedSerializer, primaryKey, false).thenCompose(oldRecord -> oldRecord == null ? AsyncUtil.READY_FALSE : AsyncUtil.READY_TRUE);
        }
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
                byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
                if (oldHasIncompleteVersion) {
                    context.removeVersionMutation(versionKey);
                } else if (metaData.isStoreRecordVersions()) {
                    ensureContextActive().clear(versionKey);
                }
            }
            CompletableFuture<Void> updateIndexesFuture = updateSecondaryIndexes(oldRecord, null);
            if (oldHasIncompleteVersion) {
                return updateIndexesFuture.thenApply(vignore -> {
                    byte[] versionKey = getSubspace().pack(recordVersionKey(primaryKey));
                    context.removeLocalVersion(versionKey);
                    return true;
                });
            } else {
                return updateIndexesFuture.thenApply(vignore -> true);
            }
        });
        return context.instrument(FDBStoreTimer.Events.DELETE_RECORD, result);
    }

    /**
     * Delete the record store at the given {@link KeySpacePath}. This behaves like
     * {@link #deleteStore(FDBRecordContext, Subspace)} on the record store saved
     * at {@link KeySpacePath#toSubspace(FDBRecordContext)}.
     *
     * @param context the transactional context in which to delete the record store
     * @param path the path to the record store
     * @see #deleteStore(FDBRecordContext, Subspace)
     */
    public static void deleteStore(FDBRecordContext context, KeySpacePath path) {
        final Subspace subspace = path.toSubspace(context);
        deleteStore(context, subspace);
    }

    /**
     * Delete the record store at the given {@link Subspace}. In addition to the store's
     * data this will delete the store's header and therefore will remove any evidence that
     * the store existed.
     *
     * <p>
     * This method does not read the underlying record store, so it does not validate
     * that a record store exists in the given subspace. As it might be the case that
     * this record store has a cacheable store state (see {@link #setStateCacheability(boolean)}),
     * this method resets the database's
     * {@linkplain FDBRecordContext#getMetaDataVersionStamp(IsolationLevel) meta-data version-stamp}.
     * As a result, calling this method may cause other clients to invalidate their caches needlessly.
     * </p>
     *
     * @param context the transactional context in which to delete the record store
     * @param subspace the subspace containing the record store
     */
    @SuppressWarnings("PMD.CloseResource")
    public static void deleteStore(FDBRecordContext context, Subspace subspace) {
        // In theory, we only need to set the meta-data version stamp if the record store's
        // meta-data is cacheable, but we can't know that from here.
        context.setMetaDataVersionStamp();
        context.setDirtyStoreState(true);
        context.clear(subspace.range());
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void deleteAllRecords() {
        preloadCache.invalidateAll();

        // Clear out all data except for the store header key and the index state space.
        // Those two subspaces are determined by the configuration of the record store rather then
        // the records.
        Range indexStateRange = indexStateSubspace().range();
        context.clear(new Range(recordsSubspace().getKey(), indexStateRange.begin));
        context.clear(new Range(indexStateRange.end, getSubspace().range().end));
    }

    @Override
    public CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull QueryComponent component) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(ignore -> deleteRecordsWhereAsync(component));
        }

        preloadCache.invalidateAll();
        recordStoreStateRef.get().beginRead();
        boolean async = false;
        try {
            CompletableFuture<Void> future = new RecordsWhereDeleter(component).run();
            async = true;
            return future.whenComplete((ignore, err) -> recordStoreStateRef.get().endRead());
        } finally {
            if (!async) {
                recordStoreStateRef.get().endRead();
            }
        }
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

        @Nonnull final QueryComponent component;
        @Nullable final QueryComponent typelessComponent;
        @Nonnull final QueryToKeyMatcher matcher;
        @Nullable final QueryToKeyMatcher indexMatcher;

        @Nonnull final Collection<RecordType> allRecordTypes;
        @Nonnull final Collection<Index> allIndexes;

        @Nonnull final List<IndexMaintainer> indexMaintainers;

        @Nullable final Key.Evaluated evaluated;
        @Nullable final Key.Evaluated indexEvaluated;

        public RecordsWhereDeleter(@Nonnull QueryComponent component) {
            this.component = component;

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
                throw recordCoreException("record type version of deleteRecordsWhere can only be used when all record types have a type prefix");
            }

            matcher = new QueryToKeyMatcher(component);
            recordMetaData = getRecordMetaData();
            if (recordTypeKeyComparison == null) {
                indexMatcher = matcher;
                allRecordTypes = recordMetaData.getRecordTypes().values();
                allIndexes = recordMetaData.getAllIndexes();
                recordType = null;
                typelessComponent = component;
            } else {
                recordType = recordMetaData.getRecordType(recordTypeKeyComparison.getName());
                if (remainingComponent == null) {
                    indexMatcher = null;
                } else {
                    indexMatcher = new QueryToKeyMatcher(remainingComponent);
                }
                allRecordTypes = Collections.singletonList(recordType);
                final List<Index> recordTypeIndexes = recordType.getAllIndexes();
                allIndexes = new LinkedHashSet<>(recordTypeIndexes);
                for (SyntheticRecordType<?> syntheticType : recordMetaData.getSyntheticRecordTypes().values()) {
                    if (syntheticType.getConstituents().stream().anyMatch(c -> c.getRecordType().equals(recordType))) {
                        allIndexes.addAll(syntheticType.getAllIndexes());
                    }
                }
                typelessComponent = remainingComponent;
            }

            indexMaintainers = allIndexes.stream()
                    .filter(index -> !isIndexDisabled(index))
                    .map(FDBRecordStore.this::getIndexMaintainer)
                    .collect(Collectors.toList());

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
                    throw recordCoreException("Primary key prefixes don't align",
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
                    throw recordCoreException("Record count key prefix doesn't align",
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
                boolean canDelete = canDeleteWhereForIndex(index);
                if (!canDelete) {
                    throw new Query.InvalidExpressionException("deleteRecordsWhere not supported by index " +
                            index.state.index.getName());
                }
            }
        }

        private boolean canDeleteWhereForIndex(@Nonnull final IndexMaintainer indexMaintainer) {
            final Index index = indexMaintainer.state.index;
            final Collection<RecordType> recordTypesForIndex = recordMetaData.recordTypesForIndex(index);
            boolean containsStoredTypes = false;
            boolean containsUnnestedTypes = false;
            boolean containsJoinedTypes = false;
            for (RecordType indexRecordType : recordTypesForIndex) {
                if (indexRecordType.isSynthetic()) {
                    if (indexRecordType instanceof UnnestedRecordType) {
                        containsUnnestedTypes = true;
                    } else if (indexRecordType instanceof JoinedRecordType) {
                        containsJoinedTypes = true;
                    } else {
                        return false;
                    }
                } else {
                    containsStoredTypes = true;
                }
            }
            if (containsStoredTypes && !containsUnnestedTypes && !containsJoinedTypes) {
                return canDeleteWhereForIndexOnStoredTypes(indexMaintainer);
            } else if (containsUnnestedTypes && !containsStoredTypes && !containsJoinedTypes) {
                return canDeleteWhereForIndexOnUnnestedTypes(indexMaintainer);
            } else if (containsJoinedTypes && !containsStoredTypes && !containsUnnestedTypes) {
                return canDeleteWhereForIndexOnJoinedTypes(indexMaintainer);
            } else {
                // we currently do not support mixing synthetic and non-synthetic record types
                return false;
            }
        }

        private boolean canDeleteWhereForIndexOnStoredTypes(@Nonnull final IndexMaintainer indexMaintainer) {
            final Index index = indexMaintainer.state.index;
            final Collection<RecordType> recordTypesForIndex = recordMetaData.recordTypesForIndex(index);

            if (recordType == null || (Key.Expressions.hasRecordTypePrefix(index.getRootExpression()))) {
                return indexMaintainer.canDeleteWhere(matcher, evaluated);
            } else if (recordTypesForIndex.size() > 1) {
                throw recordCoreException("Index " + index.getName() +
                        " applies to more record types than just " + recordType.getName());
            } else if (indexMatcher == null) {
                return true;
            } else {
                return indexMaintainer.canDeleteWhere(indexMatcher, indexEvaluated);
            }
        }

        private boolean canDeleteWhereForIndexOnJoinedTypes(@Nonnull final IndexMaintainer indexMaintainer) {
            final Index index = indexMaintainer.state.index;
            final Collection<RecordType> recordTypesForIndex = recordMetaData.recordTypesForIndex(index);

            if (Key.Expressions.hasRecordTypePrefix(index.getRootExpression())) {
                // If the index has a record type prefix, we need to reject the deletion, as the referenced
                // record type key is different for the synthetic type and the requested type
                return false;
            }
            if (recordType != null) {
                // We currently do not support deleteWhere by type. In theory, it could be supported, but it requires a
                // bit more thought and testing
                return false;
            }
            if (typelessComponent == null) {
                // this should only happen if it is just a request to delete all records of a type. We should never
                // get here, thanks to the check above that `recordType != null`
                return false;
            }
            JoinedRecordType joinedRecordType = null;
            for (RecordType indexRecordType : recordTypesForIndex) {
                // currently do not support multitype indexes with joined types
                if (!(indexRecordType instanceof JoinedRecordType) || joinedRecordType != null) {
                    // this should only be called when all of them are JoinedRecordTypes
                    return false;
                }
                joinedRecordType = (JoinedRecordType)indexRecordType;
            }

            // Since we don't allow deleteRecordsWhere by a record type, what we are trying to do is delete all
            // the index entries where any constituent matches the queryToKeyMatcher.
            // Doing this in the general case is tricky, so we restrict the index to situations where:
            // 1. There are exactly 2 constituents
            // 2. neither is an outer join
            // 3. The join condition also matches the query, ensuring that if either constituent matches, both do
            // 4. The index has a prefix that matches the query component for one of the constituents
            // Thus if you delete everything that starts with that prefix it will delete every entry where constituentA
            // matches the component. Since constituentB and constituentA have the same value for the component, this
            // guarantees that there aren't entries in the index where you need to delete the entry because a record of
            // the type for constiuentB was deleted.
            if (joinedRecordType != null &&
                    joinedRecordType.getConstituents().size() == 2 &&
                    !joinedRecordType.getConstituents().get(0).isOuterJoined() &&
                    !joinedRecordType.getConstituents().get(1).isOuterJoined()) {
                final QueryToKeyMatcher queryToKeyMatcher = new QueryToKeyMatcher(typelessComponent);
                boolean foundJoin = false;
                for (final JoinedRecordType.Join join : joinedRecordType.getJoins()) {
                    if (!join.getLeft().getName().equals(join.getRight().getName()) &&
                            queryToKeyMatcher.matchesSatisfyingQuery(join.getLeftExpression()).getType() == QueryToKeyMatcher.MatchType.EQUALITY &&
                            queryToKeyMatcher.matchesSatisfyingQuery(join.getRightExpression()).getType() == QueryToKeyMatcher.MatchType.EQUALITY) {
                        foundJoin = true;
                        break;
                    }
                }
                if (!foundJoin) {
                    return false;
                }

                for (final JoinedRecordType.JoinConstituent constituent : joinedRecordType.getConstituents()) {
                    final boolean canDeleteWhere = canDeleteWhereOnConstituent(indexMaintainer, constituent.getName());
                    if (canDeleteWhere) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean canDeleteWhereForIndexOnUnnestedTypes(@Nonnull final IndexMaintainer indexMaintainer) {
            final Index index = indexMaintainer.state.index;
            final Collection<RecordType> recordTypesForIndex = recordMetaData.recordTypesForIndex(index);

            if (Key.Expressions.hasRecordTypePrefix(index.getRootExpression())) {
                // If the index has a record type prefix, we need to reject the deletion, as the referenced
                // record type key is different for the synthetic type and the requested type
                return false;
            }
            // Synthetic types nest stored records under named constituents. Find a constituent corresponding
            // to the appropriate record type(s) under which the original predicate can be nested to find the
            // set of index entries corresponding to the deleted stored records
            String constituentName = null;
            for (RecordType indexRecordType : recordTypesForIndex) {
                final SyntheticRecordType<?> syntheticRecordType = (SyntheticRecordType<?>)indexRecordType;
                if (syntheticRecordType instanceof UnnestedRecordType) {
                    UnnestedRecordType unnestedRecordType = (UnnestedRecordType)syntheticRecordType;
                    UnnestedRecordType.NestedConstituent parent = unnestedRecordType.getParentConstituent();
                    if (recordType != null && !recordType.equals(parent.getRecordType())) {
                        // If the delete is limited to a single type, only process indexes on unnested records of
                        // precisely that type. In theory, we could use the constituent name as a predicate here,
                        // but if the different record types have different constituent names, then they can't
                        // be used to form a prefix.
                        return false;
                    }
                    if (constituentName != null && !constituentName.equals(parent.getName())) {
                        return false;
                    }
                    constituentName = parent.getName();
                } else {
                    // this should only ever be called when they are all unnested
                    return false;
                }
            }
            if (constituentName == null) {
                return false;
            }
            return canDeleteWhereOnConstituent(indexMaintainer, constituentName);
        }

        private boolean canDeleteWhereOnConstituent(final @Nonnull IndexMaintainer indexMaintainer, final String constituentName) {
            if (typelessComponent == null) {
                // The only predicate was the one on type. If we get here, all records should be synthesized from the
                // stored type being deleted, so return true (which will clear the index)
                return true;
            }
            final QueryComponent syntheticQueryComponent;
            if (typelessComponent instanceof AndComponent) {
                final List<QueryComponent> originalQueryComponents = ((AndComponent)typelessComponent).getChildren();
                List<QueryComponent> syntheticQueryComponents = new ArrayList<>(originalQueryComponents.size());
                for (QueryComponent c : originalQueryComponents) {
                    syntheticQueryComponents.add(Query.field(constituentName).matches(c));
                }
                syntheticQueryComponent = Query.and(syntheticQueryComponents);
            } else {
                syntheticQueryComponent = Query.field(constituentName).matches(typelessComponent);
            }
            final QueryToKeyMatcher syntheticMatcher = new QueryToKeyMatcher(syntheticQueryComponent);
            return indexMaintainer.canDeleteWhere(syntheticMatcher, indexEvaluated);
        }

        @SuppressWarnings("PMD.CloseResource")
        private CompletableFuture<Void> run() {
            if (evaluated == null) {
                // no record types
                LOGGER.warn("Tried to delete prefix with no record types");
                return AsyncUtil.DONE;
            }

            final Transaction tr = ensureContextActive();

            final Tuple prefix = evaluated.toTuple();
            final Range recordRange = recordsSubspace().subspace(prefix).range();
            context.clear(recordRange);
            if (useOldVersionFormat() && getRecordMetaData().isStoreRecordVersions()) {
                final Range versionRange = getSubspace().subspace(Tuple.from(RECORD_VERSION_KEY).addAll(prefix)).range();
                context.clear(versionRange);
            }

            final KeyExpression recordCountKey = getRecordMetaData().getRecordCountKey();
            if (recordCountKey != null) {
                if (prefix.size() == recordCountKey.getColumnSize()) {
                    // Delete a single record used for counting
                    context.clear(getSubspace().pack(Tuple.from(RECORD_COUNT_KEY).addAll(prefix)));
                } else {
                    // Delete multiple records used for counting
                    context.clear(getSubspace().subspace(Tuple.from(RECORD_COUNT_KEY)).subspace(prefix).range());
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
    @Nonnull
    public CompletableFuture<Long> estimateStoreSizeAsync() {
        final long startTime = System.nanoTime();
        return getSubspaceAsync().thenCompose(subspace -> estimateSize(subspace.range(), startTime));
    }

    @Override
    @Nonnull
    public CompletableFuture<Long> estimateRecordsSizeAsync(@Nonnull TupleRange range) {
        final long startTime = System.nanoTime();
        return getSubspaceAsync().thenCompose(ignore -> estimateSize(range.toRange(recordsSubspace()), startTime));
    }

    private CompletableFuture<Long> estimateSize(@Nonnull Range range, long startTimeNanos) {
        final CompletableFuture<Long> sizeFuture = ensureContextActive().getEstimatedRangeSizeBytes(range);
        return instrument(FDBStoreTimer.Events.ESTIMATE_SIZE, sizeFuture, startTimeNanos);
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value,
                                                          @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        if (getRecordMetaData().getRecordCountKey() != null) {
            if (key.getColumnSize() != value.size()) {
                throw recordCoreException("key and value are not the same size");
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
        return evaluateAggregateFunction(Collections.emptyList(), IndexFunctionHelper.count(key),
                TupleRange.allOf(value.toTuple()), IsolationLevel.SNAPSHOT, indexQueryabilityFilter)
                .thenApply(tuple -> tuple.getLong(0));
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName,
                                                                       @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        // A COUNT index on this record type.
        IndexAggregateFunction aggregateFunction = IndexFunctionHelper.count(EmptyKeyExpression.EMPTY);
        Optional<IndexMaintainer> indexMaintainer = IndexFunctionHelper.indexMaintainerForAggregateFunction(this,
                aggregateFunction, Collections.singletonList(recordTypeName), indexQueryabilityFilter);
        if (indexMaintainer.isPresent()) {
            return indexMaintainer.get().evaluateAggregateFunction(aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT)
                    .thenApply(tuple -> tuple.getLong(0));
        }
        // A universal COUNT index by record type.
        // In fact, any COUNT index by record type that applied to this record type would work, no matter what other
        // types it applied to.
        aggregateFunction = IndexFunctionHelper.count(Key.Expressions.recordType());
        indexMaintainer = IndexFunctionHelper.indexMaintainerForAggregateFunction(this,
                aggregateFunction, Collections.emptyList(), indexQueryabilityFilter);
        if (indexMaintainer.isPresent()) {
            RecordType recordType = getRecordMetaData().getRecordType(recordTypeName);
            return indexMaintainer.get().evaluateAggregateFunction(aggregateFunction, TupleRange.allOf(recordType.getRecordTypeKeyTuple()), IsolationLevel.SNAPSHOT)
                    .thenApply(tuple -> tuple.getLong(0));
        }
        throw recordCoreException("Require a COUNT index on " + recordTypeName);
    }

    static byte[] encodeRecordCount(long count) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(count).array();
    }

    public static long decodeRecordCount(@Nullable byte[] bytes) {
        return bytes == null ? 0 : ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> evaluateIndexRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                                @Nonnull IndexRecordFunction<T> function,
                                                                @Nonnull FDBRecord<Message> rec) {
        return evaluateTypedIndexRecordFunction(evaluationContext, function, rec);
    }

    @Nonnull
    protected <T, M extends Message> CompletableFuture<T> evaluateTypedIndexRecordFunction(@Nonnull EvaluationContext evaluationContext,
                                                                                           @Nonnull IndexRecordFunction<T> indexRecordFunction,
                                                                                           @Nonnull FDBRecord<M> rec) {
        return IndexFunctionHelper.indexMaintainerForRecordFunction(this, indexRecordFunction, rec)
                .orElseThrow(() -> recordCoreException("Record function " + indexRecordFunction +
                                                       " requires appropriate index on " + rec.getRecordType().getName()))
            .evaluateRecordFunction(evaluationContext, indexRecordFunction, rec);
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull EvaluationContext evaluationContext,
                                                          @Nonnull StoreRecordFunction<T> function,
                                                          @Nonnull FDBRecord<Message> rec) {
        return evaluateTypedStoreFunction(evaluationContext, function, rec);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <T, M extends Message> CompletableFuture<T> evaluateTypedStoreFunction(@Nonnull EvaluationContext evaluationContext,
                                                                                  @Nonnull StoreRecordFunction<T> function,
                                                                                  @Nonnull FDBRecord<M> rec) {
        if (FunctionNames.VERSION.equals(function.getName())) {
            if (rec.hasVersion() && rec.getVersion().isComplete()) {
                return CompletableFuture.completedFuture((T) rec.getVersion());
            }
            return (CompletableFuture<T>) loadRecordVersionAsync(rec.getPrimaryKey()).orElse(CompletableFuture.completedFuture(null));
        } else {
            throw recordCoreException("Unknown store function " + function.getName());
        }
    }

    @Override
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel,
                                                              @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        return IndexFunctionHelper.indexMaintainerForAggregateFunction(this,
                        aggregateFunction, recordTypeNames, indexQueryabilityFilter)
                .orElseThrow(() ->
                        new AggregateFunctionNotSupportedException("Aggregate function requires appropriate index",
                                LogMessageKeys.FUNCTION, aggregateFunction,
                                subspaceProvider.logKey(), subspaceProvider.toString(context)))
                .evaluateAggregateFunction(aggregateFunction, range, isolationLevel);
    }

    @Override
    @Nonnull
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        final RecordQueryPlanner planner = new RecordQueryPlanner(getRecordMetaData(), getRecordStoreState());
        return planner.plan(query, parameterRelationshipGraph);
    }

    @Override
    @Nonnull
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph,
                                     @Nonnull RecordQueryPlannerConfiguration plannerConfiguration) {
        final RecordQueryPlanner planner = new RecordQueryPlanner(getRecordMetaData(), getRecordStoreState());
        planner.setConfiguration(plannerConfiguration);
        return planner.plan(query, parameterRelationshipGraph);
    }

    /**
     * Utility method to be used to specify that new indexes should be {@link IndexState#WRITE_ONLY}
     * if an index is not on new record types and there are too many records to build in-line. This method
     * can be used by an implementor of {@link UserVersionChecker#needRebuildIndex(Index, long, boolean)}, which
     * has more details on its usage. This was the default behavior prior to Record Layer version 3.0.
     *
     * @param recordCount the number of records in the store
     * @param indexOnNewRecordTypes whether the index is defined on entirely new record types
     * @return {@link IndexState#READABLE} if the index should be built in-line when the store is opened or
     *      {@link IndexState#WRITE_ONLY} otherwise
     * @see UserVersionChecker#needRebuildIndex(Index, long, boolean)
     */
    @Nonnull
    public static IndexState writeOnlyIfTooManyRecordsForRebuild(long recordCount, boolean indexOnNewRecordTypes) {
        return readableIfNewTypeOrFewRecordsForRebuild(recordCount, indexOnNewRecordTypes, IndexState.WRITE_ONLY);
    }

    /**
     * Utility method to be used to specify that new indexes should be {@link IndexState#DISABLED}
     * if an index is not on new record types and there are too many records to build in-line. This method
     * can be used by an implementor of {@link UserVersionChecker#needRebuildIndex(Index, long, boolean)}, which
     * has more details on its usage. This is also used by the default implementation of that method, and this
     * is used by default if no {@link UserVersionChecker} is supplied to an {@link FDBRecordStore}.
     *
     * @param recordCount the number of records in the store
     * @param indexOnNewRecordTypes whether the index is defined on entirely new record types
     * @return {@link IndexState#READABLE} if the index should be built in-line when the store is opened or
     *      {@link IndexState#DISABLED} otherwise
     * @see UserVersionChecker#needRebuildIndex(Index, long, boolean)
     */
    @Nonnull
    public static IndexState disabledIfTooManyRecordsForRebuild(long recordCount, boolean indexOnNewRecordTypes) {
        return readableIfNewTypeOrFewRecordsForRebuild(recordCount, indexOnNewRecordTypes, IndexState.DISABLED);
    }

    @Nonnull
    private static IndexState readableIfNewTypeOrFewRecordsForRebuild(long recordCount,
                                                                      boolean indexOnNewRecordTypes,
                                                                      @Nonnull IndexState defaultState) {
        if (indexOnNewRecordTypes || recordCount <= MAX_RECORDS_FOR_REBUILD) {
            return IndexState.READABLE;
        } else {
            return defaultState;
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
            if (recordStoreStateRef.get() == null) {
                recordStoreStateRef.compareAndSet(null, storeInfo.getRecordStoreState().toMutable());
            }
            return recordStoreStateRef.get().getStoreHeader();
        });
        if (!MoreAsyncUtil.isCompletedNormally(metaDataPreloadFuture)) {
            storeHeaderFuture = metaDataPreloadFuture.thenCombine(storeHeaderFuture, (vignore, storeHeader) -> storeHeader);
        }
        if (!MoreAsyncUtil.isCompletedNormally(subspacePreloadFuture)) {
            storeHeaderFuture = subspacePreloadFuture.thenCombine(storeHeaderFuture, (vignore, storeHeader) -> storeHeader);
        }
        CompletableFuture<Boolean> result = storeHeaderFuture.thenCompose(storeHeader -> checkVersion(storeHeader, userVersionChecker));
        return context.instrument(FDBStoreTimer.Events.CHECK_VERSION, result).thenApply(versionChanged -> {
            if (versionChanged) {
                this.versionChanged = true;
            }
            return versionChanged;
        });
    }

    @Nonnull
    private CompletableFuture<Boolean> checkVersion(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader, @Nullable UserVersionChecker userVersionChecker) {
        RecordMetaDataProto.DataStoreInfo.Builder info = storeHeader.toBuilder();
        if (info.hasFormatVersion() && info.getFormatVersion() >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
            // If the store is already using a format version greater than or equal to the version
            // where the unsplit records are now written with an extra suffix, use the value for
            // that stored from the database. (Note that this depends on the property that calling "get"
            // on an unset boolean field results in getting back "false".)
            omitUnsplitRecordSuffix = info.getOmitUnsplitRecordSuffix();
        }
        final boolean[] dirty = new boolean[1];
        final boolean newStore = isNewStoreHeader(storeHeader);
        if (Math.max(storeHeader.getFormatVersion(), formatVersion) >= CACHEABLE_STATE_FORMAT_VERSION
                && (stateCacheabilityOnOpen.isUpdateExistingStores() || newStore)) {
            boolean cacheable = stateCacheabilityOnOpen.isCacheable();
            if (info.getCacheable() != cacheable) {
                if (newStore) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("setting initial store state cacheability",
                                LogMessageKeys.OLD, info.getCacheable(),
                                LogMessageKeys.NEW, cacheable,
                                subspaceProvider.logKey(), subspaceProvider.toString(context)));
                    }
                } else {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(KeyValueLogMessage.of("updating store state cacheability",
                                LogMessageKeys.OLD, info.getCacheable(),
                                LogMessageKeys.NEW, cacheable,
                                subspaceProvider.logKey(), subspaceProvider.toString(context)));
                    }
                }
                info.setCacheable(cacheable);
                dirty[0] = true;
            }
        }
        final CompletableFuture<Void> checkedUserVersion = checkUserVersion(userVersionChecker, storeHeader, info, dirty);
        final CompletableFuture<Void> checkedRebuild = checkedUserVersion.thenCompose(vignore -> checkPossiblyRebuild(userVersionChecker, info, dirty));
        return checkedRebuild.thenCompose(vignore -> {
            if (dirty[0]) {
                return updateStoreHeaderAsync(ignore -> info).thenApply(vignore2 -> true);
            } else {
                return AsyncUtil.READY_FALSE;
            }
        }).thenCompose(this::removeReplacedIndexesIfChanged);
    }

    @SuppressWarnings("squid:S3776") // cognitive complexity is high, candidate for refactoring
    private CompletableFuture<Void> checkUserVersion(@Nullable UserVersionChecker userVersionChecker,
                                                     @Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader,
                                                     @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info, @Nonnull boolean[] dirty) {
        if (userVersionChecker == null) {
            return AsyncUtil.DONE;
        }
        final boolean newStore = isNewStoreHeader(storeHeader);
        final int oldUserVersion = newStore ? -1 : info.getUserVersion();
        return userVersionChecker.checkUserVersion(storeHeader, metaDataProvider)
                .thenApply(newUserVersion -> {
                    userVersion = newUserVersion;
                    if (newUserVersion != oldUserVersion) {
                        if (oldUserVersion > newUserVersion) {
                            if (LOGGER.isErrorEnabled()) {
                                LOGGER.error(KeyValueLogMessage.of("stale user version",
                                                LogMessageKeys.STORED_VERSION, oldUserVersion,
                                                LogMessageKeys.LOCAL_VERSION, newUserVersion,
                                                subspaceProvider.logKey(), subspaceProvider.toString(context)));
                            }
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
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(KeyValueLogMessage.of("changing user version",
                                        LogMessageKeys.OLD_VERSION, oldUserVersion,
                                        LogMessageKeys.NEW_VERSION, newUserVersion,
                                        subspaceProvider.logKey(), subspaceProvider.toString(context)));
                            }
                        }
                    }
                    return null;
                });
    }

    private static boolean isNewStoreHeader(@Nonnull RecordMetaDataProto.DataStoreInfoOrBuilder storeInfo) {
        return storeInfo.getFormatVersion() == 0;
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
                throw new RecordCoreStorageException("Error reading version", ex)
                        .addLogInfo(subspaceProvider.logKey(), subspaceProvider.toString(context));
            }
        }
        checkStoreHeaderInternal(info, getContext(), getSubspaceProvider(), existenceCheck);
        return info;
    }

    @API(API.Status.INTERNAL)
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
        final Tuple firstKey = subspace.unpack(firstKeyValue.getKey());
        if (TupleHelpers.equals(firstKey, Tuple.from(STORE_INFO_KEY))) {
            return true;
        } else if (existenceCheck == StoreExistenceCheck.NONE) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(KeyValueLogMessage.of("Record store has no info but is not empty",
                        subspaceProvider.logKey(), subspaceProvider.toString(context),
                        LogMessageKeys.KEY, firstKey));
            }
            return false;
        } else {
            final FDBRecordStoreKeyspace keyspace = determineRecordStoreKeyspace(firstKey, subspaceProvider, context);
            if (FDBRecordStoreKeyspace.INDEX_STATE_SPACE.equals(keyspace) || FDBRecordStoreKeyspace.INDEX_RANGE_SPACE.equals(keyspace) || FDBRecordStoreKeyspace.INDEX_BUILD_SPACE.equals(keyspace)) {
                // Allow list of acceptable key ranges for the first key. This may need to be updated as more keyspaces are added.
                // Includes: INDEX_STATE_SPACE, INDEX_RANGE_SPACE, and INDEX_BUILD_SPACE as those contain only meta-data about the state of the
                // index or index build but no "user data"
                // Excludes: anything with records or data about records, i.e., RECORD (as it contains records), INDEX and INDEX_SECONDARY space (as
                // they contains data from indexes), RECORD_COUNT (as that is/was effectively an index), INDEX_UNIQUENESS_VIOLATIONS_SPACE (as it
                // contains data that should be consistent with the index), and RECORD_VERSION_SPACE (as it contains data that is effectively tied
                // to the records). In a record store where the only corruption is the lack of a store header, then if the store has no records,
                // INDEX_UNIQUENESS_VIOLATIONS_SPACE and RECORD_VERSION_SPACE should be empty as well, but this isn't validated. In theory, if the
                // RECORD_COUNT keyspace was zero, that would be consistent, so it would be "safe" to only warn then as well.
                if (existenceCheck == StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.of("Record store has no info or records but is not empty",
                                subspaceProvider.logKey(), subspaceProvider.toString(context),
                                LogMessageKeys.KEY, firstKey));
                    }
                    return false;
                } else {
                    throw noInfoAndNotEmptyException("Record store has no info or records but is not empty", firstKey, subspaceProvider, context);
                }
            } else {
                throw noInfoAndNotEmptyException("Record store has no info but is not empty", firstKey, subspaceProvider, context);
            }
        }
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    @Nonnull
    private static FDBRecordStoreKeyspace determineRecordStoreKeyspace(@Nonnull Tuple firstKey, @Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordContext context) {
        if (firstKey.isEmpty()) {
            // This shouldn't happen because of how the range read is performed
            throw new RecordCoreException("First key in record store is empty",
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                    LogMessageKeys.KEY, firstKey);
        }
        try {
            return FDBRecordStoreKeyspace.fromKey(firstKey.get(0));
        } catch (RecordCoreException e) {
            // PMD doesn't like this line because it can't tell it's actually the same exception being rethrown
            throw new RecordStoreNoInfoAndNotEmptyException("Record store has no info but is not empty with an unknown keyspace", e)
                    .addLogInfo(subspaceProvider.logKey(), subspaceProvider.toString(context))
                    .addLogInfo(LogMessageKeys.KEY, firstKey);
        }
    }

    @Nonnull
    private static RecordStoreNoInfoAndNotEmptyException noInfoAndNotEmptyException(@Nonnull String staticMessage,
                                                                                    @Nonnull Tuple firstKey,
                                                                                    @Nonnull SubspaceProvider subspaceProvider,
                                                                                    @Nonnull FDBRecordContext context) {
        return new RecordStoreNoInfoAndNotEmptyException(staticMessage,
                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                    LogMessageKeys.KEY, firstKey);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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

    /**
     * Schedule a pre-commit hook to look for replaced indexes. This method delays executing the replaced-indexes
     * check logic until right before the transaction commits, and it will only schedule the check at most once.
     * This is to prevent multiple instances of that logic running at the same time. Because
     * {@link #removeReplacedIndexes()} needs to both read and (potentially) update the index state information, running
     * that method concurrently with itself or with other index state updates can result in errors. So, unless it
     * can be guaranteed via other means that that method is the only method removing replacement indexes, this
     * method should be preferred over calling the method directly.
     *
     * @param changed whether the index state information has changed
     * @return whether the commit check was scheduled
     * @see #removeReplacedIndexes()
     * @see com.apple.foundationdb.record.metadata.IndexOptions#REPLACED_BY_OPTION_PREFIX
     */
    private boolean addRemoveReplacedIndexesCommitCheckIfChanged(boolean changed) {
        if (changed) {
            final String commitCheckName = "removeReplacedIndexes_" + ByteArrayUtil2.toHexString(getSubspace().pack());
            getRecordContext().getOrCreateCommitCheck(commitCheckName, name -> this::removeReplacedIndexes);
        }
        return changed;
    }

    /**
     * Remove any replaced indexes if the index state information has changed. This executes right away (if the
     * index states have, in fact, changed). Note that it is unsafe to call {@link #removeReplacedIndexes()} twice
     * concurrently on the same record store, so this method should only be called if it can be guaranteed to be
     * the only thing calling {@link #removeReplacedIndexes()} at that time. If this cannot be guaranteed, then
     * calling {@link #addRemoveReplacedIndexesCommitCheckIfChanged(boolean)} will ensure that the check is eventually
     * run once and therefore protects against concurrent accesses.
     *
     * @param changed whether index state information has changed
     * @return a future that will return whether {@link #removeReplacedIndexes()} was called
     * @see #removeReplacedIndexes()
     * @see #addRemoveReplacedIndexesCommitCheckIfChanged(boolean)
     * @see com.apple.foundationdb.record.metadata.IndexOptions#REPLACED_BY_OPTION_PREFIX
     */
    @Nonnull
    private CompletableFuture<Boolean> removeReplacedIndexesIfChanged(boolean changed) {
        if (changed) {
            return removeReplacedIndexes().thenApply(vignore -> true);
        } else {
            return AsyncUtil.READY_FALSE;
        }
    }

    @Nonnull
    private CompletableFuture<Void> removeReplacedIndexes() {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> removeReplacedIndexes());
        }

        // Look for any indexes that can be removed because they have been replaced with new indexes
        beginRecordStoreStateRead();
        final RecordMetaData metaData = getRecordMetaData();
        final List<Index> indexesToRemove = new ArrayList<>();
        try {
            for (Index index : metaData.getAllIndexes()) {
                final List<String> replacedByNames = index.getReplacedByIndexNames();
                if (!replacedByNames.isEmpty()) {
                    // Check if all of the replaced by index names are readable
                    if (replacedByNames.stream()
                            .allMatch(replacedByName -> metaData.hasIndex(replacedByName) && isIndexReadable(replacedByName))) {
                        indexesToRemove.add(index);
                    }
                }
            }
        } finally {
            endRecordStoreStateRead();
        }

        // If there are no indexes to remove, terminate now
        if (indexesToRemove.isEmpty()) {
            return AsyncUtil.DONE;
        }

        // Mark all the replaced indexes as DISABLED. This also deletes their data
        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            final List<CompletableFuture<Boolean>> indexRemoveFutures = new ArrayList<>(indexesToRemove.size());
            for (Index index : indexesToRemove) {
                indexRemoveFutures.add(markIndexDisabled(index));
            }
            CompletableFuture<Void> future = AsyncUtil.whenAll(indexRemoveFutures)
                    .whenComplete((vignore, errIgnore) -> endRecordStoreStateWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
            }
        }
    }

    private void beginRecordStoreStateRead() {
        // When the record store state is being updated multiple times, this function (and its implicit retry loop at
        // the atomic reference level) will retry the update on the new record store state, so the operation always
        // does what's expected (i.e., update the "in flight reads" value while leaving the record store state otherwise
        // in tact).
        recordStoreStateRef.updateAndGet(state -> {
            state.beginRead();
            return state;
        });
    }

    private void endRecordStoreStateRead() {
        recordStoreStateRef.updateAndGet(state -> {
            state.endRead();
            return state;
        });
    }

    private void beginRecordStoreStateWrite() {
        recordStoreStateRef.updateAndGet(state -> {
            state.beginWrite();
            return state;
        });
    }

    private void endRecordStoreStateWrite() {
        recordStoreStateRef.updateAndGet(state -> {
            state.endWrite();
            return state;
        });
    }

    @Nonnull
    private CompletableFuture<RecordMetaDataProto.DataStoreInfo> loadStoreHeaderAsync(@Nonnull StoreExistenceCheck existenceCheck, @Nonnull IsolationLevel isolationLevel) {
        return readStoreFirstKey(context, getSubspace(), isolationLevel).thenApply(keyValue -> checkAndParseStoreHeader(keyValue, existenceCheck));
    }

    @VisibleForTesting
    protected void saveStoreHeader(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader) {
        if (recordStoreStateRef.get() == null) {
            throw uninitializedStoreException("cannot update store header on an uninitialized store");
        }
        beginRecordStoreStateWrite();
        try {
            context.setDirtyStoreState(true);
            synchronized (this) {
                recordStoreStateRef.updateAndGet(state -> {
                    state.setStoreHeader(storeHeader);
                    return state;
                });
                ensureContextActive().set(getSubspace().pack(STORE_INFO_KEY), storeHeader.toByteArray());
            }
        } finally {
            endRecordStoreStateWrite();
        }
    }

    @Nonnull
    private CompletableFuture<Void> updateStoreHeaderAsync(@Nonnull UnaryOperator<RecordMetaDataProto.DataStoreInfo.Builder> storeHeaderMutator) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> updateStoreHeaderAsync(storeHeaderMutator));
        }
        AtomicReference<RecordMetaDataProto.DataStoreInfo> oldStoreHeaderRef = new AtomicReference<>();
        AtomicReference<RecordMetaDataProto.DataStoreInfo> newStoreHeaderRef = new AtomicReference<>();
        beginRecordStoreStateWrite();
        try {
            context.setDirtyStoreState(true);
            synchronized (this) {
                recordStoreStateRef.updateAndGet(state -> {
                    RecordMetaDataProto.DataStoreInfo oldStoreHeader = state.getStoreHeader();
                    oldStoreHeaderRef.set(oldStoreHeader);
                    RecordMetaDataProto.DataStoreInfo.Builder storeHeaderBuilder = oldStoreHeader.toBuilder();
                    storeHeaderBuilder = storeHeaderMutator.apply(storeHeaderBuilder);
                    storeHeaderBuilder.setLastUpdateTime(System.currentTimeMillis());
                    RecordMetaDataProto.DataStoreInfo newStoreHeader = storeHeaderBuilder.build();
                    newStoreHeaderRef.set(newStoreHeader);
                    state.setStoreHeader(newStoreHeader);
                    return state;
                });
                ensureContextActive().set(getSubspace().pack(STORE_INFO_KEY), newStoreHeaderRef.get().toByteArray());
            }
        } finally {
            endRecordStoreStateWrite();
        }

        RecordMetaDataProto.DataStoreInfo oldStoreHeader = oldStoreHeaderRef.get();
        RecordMetaDataProto.DataStoreInfo newStoreHeader = newStoreHeaderRef.get();

        // Update the meta-data version-stamp key as appropriate.
        if (oldStoreHeader.getCacheable()) {
            // The old store header had a cacheable store header, so update the database's meta-data version-stamp
            // so that anything that has the old cached value knows to invalidate its cache.
            context.setMetaDataVersionStamp();
            return AsyncUtil.DONE;
        } else if (newStoreHeader.getCacheable()) {
            // The old header did not have a cacheable store header, but the new header does. As long as someone
            // has set the meta-data version stamp ever, there is no need to update it here.
            return context.getMetaDataVersionStampAsync(IsolationLevel.SNAPSHOT).thenAccept(metaDataVersionStamp -> {
                if (metaDataVersionStamp == null) {
                    context.setMetaDataVersionStamp();
                }
            });
        } else {
            // Neither header was cacheable meta-data, so there is no need to update the key.
            return AsyncUtil.DONE;
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
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Void> rebuildAllIndexes() {
        // Note that index states are *not* cleared, as rebuilding the indexes resets each state
        context.clear(getSubspace().range(Tuple.from(INDEX_KEY)));
        context.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY)));
        context.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY)));
        context.clear(getSubspace().range(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY)));
        List<CompletableFuture<Void>> work = new LinkedList<>();
        addRebuildRecordCountsJob(work);
        return rebuildIndexes(getRecordMetaData().getIndexesToBuildSince(-1), Collections.emptyMap(), work, RebuildIndexReason.REBUILD_ALL, null);
    }

    /**
     * Get unbuilt indexes for this store that should be built. This will return any index that is defined on this
     * record store where the state is not {@link IndexState#READABLE} (i.e., the index cannot be queried) that
     * does not have index options set indicating that this index should not be built. For example, if the index
     * has {@linkplain Index#getReplacedByIndexNames() replacement indexes} defined, then the index will be excluded
     * from the return result because the new indexes replacing it should be built in its stead.
     *
     * @return a map linking each unbuilt index that should be built to the list of record types on which it is
     *     defined
     * @see Index#getReplacedByIndexNames()
     */
    @Nonnull
    public Map<Index, List<RecordType>> getIndexesToBuild() {
        if (recordStoreStateRef.get() == null) {
            throw uninitializedStoreException("cannot get indexes to build on uninitialized store");
        }
        final Map<Index, List<RecordType>> indexesToBuild = getRecordMetaData().getIndexesToBuildSince(-1);
        beginRecordStoreStateRead();
        try {
            indexesToBuild.keySet().removeIf(this::isIndexReadable);
            return indexesToBuild;
        } finally {
            endRecordStoreStateRead();
        }
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
        return markIndexWriteOnly(index)
                .thenRun(() -> clearIndexData(index));
    }

    /**
     * Enum controlling how the store state cacheability flag should be changed when the store is opened.
     * By default, the store state data is not cacheable and must be re-read every time to ensure transactional
     * consistency between the store's meta-data and the rest of the data. To enable the transactional cache,
     * the store state cacheability flag in the store header must be set. This enum allows the store state
     * cacheability flag to be automatically set when the store is opened (that is, either during an
     * explicit call to {@link #checkVersion(UserVersionChecker, StoreExistenceCheck)} or during
     * {@link Builder#createOrOpen()} or its variants).
     *
     * <p>
     * Note that some care must be taken when changing this value. In particular, if there are multiple
     * processes trying to concurrently access the same stores, and some are configured with {@link #CACHEABLE}
     * while others are configured with {@link #NOT_CACHEABLE}, then different instances can end up stepping
     * on each other, switching the value of the flag every time. It is therefore advised that to switch between
     * {@link #NOT_CACHEABLE} and {@link #CACHEABLE} (unless one can take an outage in order to change this value
     * in one deployment) to first roll out a change from {@link #NOT_CACHEABLE} to {@link #DEFAULT}, and then to
     * roll out a second change from {@link #DEFAULT} to {@link #CACHEABLE}.
     * </p>
     *
     * @see #setStateCacheabilityAsync(boolean)
     * @see com.apple.foundationdb.record.provider.foundationdb.storestate.MetaDataVersionStampStoreStateCache
     */
    public enum StateCacheabilityOnOpen {
        /**
         * By default, the state cacheability is not changed when the store is opened. New stores are
         * not given a cacheable store state.
         */
        DEFAULT(false, false),
        /**
         * On existing stores, the store state cacheability flag is not changed. New stores will begin
         * with a cacheable store state.
         */
        CACHEABLE_IF_NEW(false, true),
        /**
         * Update the store state cacheability flag to {@code false} during {@link #checkVersion(UserVersionChecker, StoreExistenceCheck)}.
         */
        NOT_CACHEABLE(true, false),
        /**
         * Update the store state cacheability flag to {@code true} during {@link #checkVersion(UserVersionChecker, StoreExistenceCheck)}.
         */
        CACHEABLE(true, true),
        ;

        private final boolean updateExistingStores;
        private final boolean cacheable;

        StateCacheabilityOnOpen(boolean updateExistingStores, boolean cacheable) {
            this.updateExistingStores = updateExistingStores;
            this.cacheable = cacheable;
        }

        /**
         * Whether to update existing stores. If this is {@code false}, the value of the store
         * state cacheability flag will be left as-is on any store that has already been created.
         *
         * @return whether to update stores that have already been created
         */
        public boolean isUpdateExistingStores() {
            return updateExistingStores;
        }

        /**
         * Whether the store state should be marked as cacheable.
         *
         * @return the desired value of the store state cacheability flag
         */
        public boolean isCacheable() {
            return cacheable;
        }
    }

    /**
     * Determines what happens to the store state cacheability flag when the store is opened.
     * During {@link #checkVersion(UserVersionChecker, StoreExistenceCheck)} or
     * {@link Builder#createOrOpen()} or its alternatives, the value of this enum will determine
     * how the store cacheability should be updated, if at all.
     *
     * @return how to update the state cacheability during store opening
     * @see StateCacheabilityOnOpen
     * @see #setStateCacheabilityAsync(boolean)
     */
    @Nonnull
    public StateCacheabilityOnOpen getStateCacheabilityOnOpen() {
        return stateCacheabilityOnOpen;
    }

    /**
     * Set whether the store state is cacheable. In particular, this flag determines whether the
     * {@linkplain FDBRecordContext#getMetaDataVersionStampAsync(IsolationLevel) meta-data version-stamp} key
     * is changed whenever the {@link RecordStoreState} is changed. By default, record store state information is
     * <em>not</em> cacheable. This is because for deployments in which there are many record stores sharing the same
     * cluster, updating the meta-data version key every time any store changes its state could lead to performance
     * problems, so (at least for now), this is a feature the user must opt-in to on each record store.
     *
     * @param cacheable whether the meta-data version-stamp should be invalidated upon store state change
     * @return a future that will complete to {@code true} if the store state's cacheability has changed
     */
    @Nonnull
    public CompletableFuture<Boolean> setStateCacheabilityAsync(boolean cacheable) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> setStateCacheabilityAsync(cacheable));
        }
        if (formatVersion < CACHEABLE_STATE_FORMAT_VERSION) {
            throw recordCoreException("cannot mark record store state cacheable at format version " + formatVersion);
        }
        if (isStateCacheableInternal() == cacheable) {
            return AsyncUtil.READY_FALSE;
        } else {
            return updateStoreHeaderAsync(headerBuilder -> headerBuilder.setCacheable(cacheable))
                    .thenApply(ignore -> true);
        }
    }

    /**
     * Set whether the store state is cacheable. This operation might block if the record store state has
     * not yet been loaded. Use {@link #setStateCacheabilityAsync(boolean)} in asynchronous contexts.
     *
     * @param cacheable whether this store's state should be cacheable
     * @return whether the record store state cacheability has changed
     * @see #setStateCacheabilityAsync(boolean)
     */
    public boolean setStateCacheability(boolean cacheable) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_SET_STATE_CACHEABILITY, setStateCacheabilityAsync(cacheable));
    }

    private boolean isStateCacheableInternal() {
        if (recordStoreStateRef.get() == null) {
            throw uninitializedStoreException("cannot check record store state cacheability on uninitialized store");
        }
        return recordStoreStateRef.get().getStoreHeader().getCacheable();
    }

    private void validateCanAccessHeaderUserFields() {
        if (formatVersion < HEADER_USER_FIELDS_FORMAT_VERSION) {
            throw recordCoreException("cannot access header user fields at current format version",
                    LogMessageKeys.FORMAT_VERSION, formatVersion);
        }
    }

    /**
     * Get the value of a user-settable field from the store header. Each of these fields are written into
     * this record store's store header. This is loaded automatically by the record store as part of
     * {@link Builder#createOrOpenAsync()} or one of its variants. This means that reading this
     * information from the header does not require any additional communication with database assuming that
     * the record store has already been initialized. As a result, this is not a blocking call.
     *
     * <p>
     * Using this feature also requires that the record store be on format version {@link #HEADER_USER_FIELDS_FORMAT_VERSION}
     * or higher. There is no change to the on-disk format version from the previous format version except that
     * extra fields in the store header were added. No data migration is necessary to upgrade to that version if
     * an existing store is on the previous format version, but the new version is used to prevent the user from serializing
     * data and then not being able to read it from instances running an older version of the Record Layer.
     * </p>
     *
     * @param userField the name of the user-settable field to read
     * @return the value of that field in the header or {@code null} if it is unset
     * @see #setHeaderUserFieldAsync(String, ByteString)
     */
    @Nullable
    public ByteString getHeaderUserField(@Nonnull String userField) {
        validateCanAccessHeaderUserFields();
        if (recordStoreStateRef.get() == null) {
            throw uninitializedStoreException("cannot get field from header on uninitialized store");
        }
        beginRecordStoreStateRead();
        try {
            RecordMetaDataProto.DataStoreInfo header = recordStoreStateRef.get().getStoreHeader();
            for (RecordMetaDataProto.DataStoreInfo.UserFieldEntry userFieldEntry : header.getUserFieldList()) {
                if (userFieldEntry.getKey().equals(userField)) {
                    return userFieldEntry.getValue();
                }
            }
            // Not found. Return null.
            return null;
        } finally {
            endRecordStoreStateRead();
        }
    }

    /**
     * Set the value of a user-settable field in the store header. The store header contains meta-data that is then
     * used by the Record Layer to determine information including what meta-data version was used the last time
     * the record store was accessed. It is therefore loaded every time the record store is opened by the
     * {@link Builder#createOrOpenAsync()} or one of its variants. The user may also elect
     * to set fields to custom values that might be meaningful for their application. For example, if the user wishes
     * to migrate from one record type to another, the user might include a field with a value that indicates whether
     * that migration has completed.
     *
     * <p>
     * Note that there are a few potential pitfalls that adopters of this API should be aware of:
     * </p>
     * <ul>
     *     <li>
     *         Because all operations to a single record store must read the store header, every time the
     *         value is updated, any concurrent operations to the store will fail with a
     *         {@link com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionConflictException FDBStoreTransactionConflictException}.
     *         Therefore, this should only ever be used for data that are mutated at a very low rate.
     *     </li>
     *     <li>
     *         As this data must be read every time the record store is created (which is at least once per
     *         transaction), the value should not be too large. There is not at the moment a hard limit applied, but
     *         a good rule of thumb may be to keep the total size from all user fields under a kilobyte.
     *     </li>
     * </ul>
     *
     * <p>
     * The value of these fields are simple byte arrays, so the user is free to choose their own serialization
     * format for the value included in this field. One recommended strategy is for the user to use Protobuf to
     * serialize a message with possibly multiple keys and values, each with meaning to the user, and then to
     * write it to a single user field (or a small number if appropriate). This decreases the total size of
     * the user fields when compared to something like storing one user-field for each field (as the Protobuf
     * serialization format is more compact), and it allows the user to follow standard Protobuf evolution
     * guidelines as these fields evolve.
     * </p>
     *
     * <p>
     * Once set, the value of these fields can be retrieved by calling {@link #getHeaderUserField(String)}. They
     * can be cleared by calling {@link #clearHeaderUserFieldAsync(String)}. Within a given transaction, updates
     * to the header fields should be visible through {@code getHeaderUserField()} (that is, the header user fields
     * support read-your-writes within a transaction), but the context associated with this store must be committed
     * for other transactions to see the update.
     * </p>
     *
     * <p>
     * Using this feature also requires that the record store be on format version {@link #HEADER_USER_FIELDS_FORMAT_VERSION}
     * or higher. There is no change to the on-disk format version from the previous format version except that
     * extra fields in the store header were added. No data migration is necessary to upgrade to that version if
     * an existing store is on the previous format version, but the new version is used to prevent the user from serializing
     * data and then not being able to read it from instances running an older version of the Record Layer.
     * </p>
     *
     * @param userField the name of the user-settable field to write
     * @param value the value to set the field to
     * @return a future that will be ready when setting the field has completed
     * @see #getHeaderUserField(String)
     */
    @Nonnull
    public CompletableFuture<Void> setHeaderUserFieldAsync(@Nonnull String userField, @Nonnull ByteString value) {
        return updateStoreHeaderAsync(storeHeaderBuilder -> {
            validateCanAccessHeaderUserFields();
            boolean found = false;
            for (RecordMetaDataProto.DataStoreInfo.UserFieldEntry.Builder userFieldEntryBuilder : storeHeaderBuilder.getUserFieldBuilderList()) {
                if (userFieldEntryBuilder.getKey().equals(userField)) {
                    userFieldEntryBuilder.setValue(value);
                    found = true;
                }
            }
            if (!found) {
                storeHeaderBuilder.addUserFieldBuilder().setKey(userField).setValue(value);
            }
            return storeHeaderBuilder;
        });
    }

    /**
     * Set the value of a user-settable field in the store header. The provided byte array will be wrapped
     * in a {@link ByteString} and then passed to {@link #setHeaderUserFieldAsync(String, ByteString)}. See that
     * function for more details and for a list of caveats on using this function.
     *
     * @param userField the name of the user-settable field to write
     * @param value the value to set the field to
     * @return a future that will be ready when setting the field has completed
     * @see #setHeaderUserFieldAsync(String, ByteString)
     */
    @Nonnull
    public CompletableFuture<Void> setHeaderUserFieldAsync(@Nonnull String userField, @Nonnull byte[] value) {
        return setHeaderUserFieldAsync(userField, ByteString.copyFrom(value));
    }

    /**
     * Set the value of a user-settable field in the store header. This is a blocking version of
     * {@link #setHeaderUserFieldAsync(String, ByteString)}. In most circumstances, this function should not be blocking,
     * but it might if either the store header has not yet been loaded or in some circumstances if the meta-data
     * is cacheable. It should therefore generally be avoided in asynchronous contexts to be safe.
     *
     * @param userField the name of the user-settable field to write
     * @param value the value to set the field to
     * @see #setHeaderUserFieldAsync(String, ByteString)
     * @see #setStateCacheabilityAsync(boolean)
     */
    public void setHeaderUserField(@Nonnull String userField, @Nonnull ByteString value) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_EDIT_HEADER_USER_FIELD, setHeaderUserFieldAsync(userField, value));
    }

    /**
     * Set the value of a user-settable field in the store header. This is a blocking version of
     * {@link #setHeaderUserFieldAsync(String, byte[])}. In most circumstances, this function should not be blocking,
     * but it might if either the store header has not yet been loaded or in some circumstances if the meta-data
     * is cacheable. It should therefore generally be avoided in asynchronous contexts to be safe.
     *
     * @param userField the name of the user-settable field to write
     * @param value the value to set the field to
     * @see #setHeaderUserFieldAsync(String, ByteString)
     * @see #setStateCacheabilityAsync(boolean)
     */
    public void setHeaderUserField(@Nonnull String userField, @Nonnull byte[] value) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_EDIT_HEADER_USER_FIELD, setHeaderUserFieldAsync(userField, value));
    }

    /**
     * Clear the value of a user-settable field in the store header. This removes a field from the store header
     * after it has been set by {@link #setHeaderUserFieldAsync(String, ByteString)}. This has the same caveats
     * as that function. In particular, whenever this is called, all concurrent operations to the same record store
     * will also fail with an
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionConflictException FDBStoreTransactionConflictException}.
     * As a result, one should avoid clearing these fields too often.
     *
     * @param userField the name of the user-settable field to clear
     * @return a future that will be ready when the field has bean cleared
     * @see #setHeaderUserFieldAsync(String, ByteString)
     */
    @Nonnull
    public CompletableFuture<Void> clearHeaderUserFieldAsync(@Nonnull String userField) {
        return updateStoreHeaderAsync(storeHeaderBuilder -> {
            validateCanAccessHeaderUserFields();
            for (int i = storeHeaderBuilder.getUserFieldCount() - 1; i >= 0; i--) {
                RecordMetaDataProto.DataStoreInfo.UserFieldEntryOrBuilder userFieldEntry = storeHeaderBuilder.getUserFieldOrBuilder(i);
                if (userFieldEntry.getKey().equals(userField)) {
                    storeHeaderBuilder.removeUserField(i);
                }
            }
            return storeHeaderBuilder;
        });
    }

    /**
     * Clear the value of a user-settable field in the store header. This is a blocking version of
     * {@link #clearHeaderUserFieldAsync(String)}. In most circumstances, this function should not be blocking,
     * but it might if either the store header has not yet been loaded or in some circumstances if the meta-data
     * is cacheable. It should therefore generally be avoided in asynchronous contexts to be safe.
     *
     * @param userField the name of user-settable field to clear
     * @see #clearHeaderUserFieldAsync(String)
     * @see #setStateCacheabilityAsync(boolean)
     */
    public void clearHeaderUserField(@Nonnull String userField) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_EDIT_HEADER_USER_FIELD, clearHeaderUserFieldAsync(userField));
    }

    // Actually (1) writes the index state to the database and (2) updates the cached state with the new state
    @SuppressWarnings("PMD.CloseResource")
    private void updateIndexState(@Nonnull String indexName, byte[] indexKey, @Nonnull IndexState indexState) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("index state change",
                    LogMessageKeys.INDEX_NAME, indexName,
                    LogMessageKeys.TARGET_INDEX_STATE, indexState.name()
            ));
        }
        if (recordStoreStateRef.get() == null) {
            throw uninitializedStoreException("cannot update index state on an uninitialized store");
        }
        // This is generally called by someone who should already have a write lock, but adding them here
        // defensively shouldn't cause problems.
        beginRecordStoreStateWrite();
        try {
            context.setDirtyStoreState(true);
            if (isStateCacheableInternal()) {
                // The cache contains index state information, so updates to this information must also
                // update the meta-data version stamp or instances might cache state index states.
                context.setMetaDataVersionStamp();
            }
            Transaction tr = context.ensureActive();
            if (IndexState.READABLE.equals(indexState)) {
                tr.clear(indexKey);
            } else {
                tr.set(indexKey, Tuple.from(indexState.code()).pack());
            }
            recordStoreStateRef.updateAndGet(state -> {
                // See beginRecordStoreStateRead() on why setting state is done in updateAndGet().
                state.setState(indexName, indexState);
                return state;
            });
        } finally {
            endRecordStoreStateWrite();
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Boolean> markIndexNotReadable(@Nonnull String indexName, @Nonnull IndexState indexState) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexNotReadable(indexName, indexState));
        }

        addIndexStateReadConflict(indexName);

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            // A read is done before the write in order to avoid having unnecessary
            // updates cause spurious not_committed errors.
            byte[] indexKey = indexStateSubspace().pack(indexName);
            Transaction tr = context.ensureActive();
            CompletableFuture<Boolean> future = tr.get(indexKey).thenCompose(previous -> {
                if (previous == null) {
                    IndexingRangeSet indexRangeSet = IndexingRangeSet.forIndexBuild(this, getRecordMetaData().getIndex(indexName));
                    return indexRangeSet.isEmptyAsync().thenCompose(empty -> {
                        if (empty) {
                            // For readable indexes, we have an optimization where the range set is cleared out
                            // after the index is build to avoid carrying extra meta-data about the index range
                            // set. However, when we mark an index as write-only, we want to preserve the record
                            // that the index was completely built (if the range set was empty, i.e., cleared)
                            return indexRangeSet.insertRangeAsync(null, null);
                        } else {
                            return AsyncUtil.READY_FALSE;
                        }
                    }).thenApply(ignore -> {
                        updateIndexState(indexName, indexKey, indexState);
                        return true;
                    });
                } else if (!Tuple.fromBytes(previous).get(0).equals(indexState.code())) {
                    updateIndexState(indexName, indexKey, indexState);
                    return AsyncUtil.READY_TRUE;
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            }).whenComplete((b, t) -> endRecordStoreStateWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
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
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Optional<Range>> firstUnbuiltRange(@Nonnull Index index) {
        if (!getRecordMetaData().hasIndex(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " does not exist in meta-data.");
        }
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(this, index);
        return rangeSet.firstMissingRangeAsync().thenApply(Optional::ofNullable);
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
     * Marks an index as readable, but if uniqueness violations are found - mark the index
     * as unique pending instead of throwing a {@link RecordIndexUniquenessViolation} exception.
     *
     * @param index the index to mark readable
     * @return a future that will either complete exceptionally if the index can not
     * be made readable/unique-pending or will contain <code>true</code> if the store was modified
     * and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> markIndexReadableOrUniquePending(@Nonnull Index index) {
        return markIndexReadable(index, true);
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
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Boolean> markIndexReadable(@Nonnull Index index) {
        return markIndexReadable(index, false);
    }

    @Nonnull
    private CompletableFuture<Boolean> markIndexReadable(@Nonnull Index index, boolean allowUniquePending) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexReadable(index));
        }

        addIndexStateReadConflict(index.getName());

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            @SuppressWarnings("PMD.CloseResource")
            Transaction tr = ensureContextActive();
            byte[] indexKey = indexStateSubspace().pack(index.getName());
            CompletableFuture<Boolean> future = tr.get(indexKey).thenCompose(previous -> {
                if (previous != null) {
                    return checkAndUpdateBuiltIndexState(index, indexKey, allowUniquePending);
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            }).whenComplete((b, t) -> endRecordStoreStateWrite()).thenApply(this::addRemoveReplacedIndexesCommitCheckIfChanged);
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
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

    private CompletableFuture<Boolean> checkAndUpdateBuiltIndexState(Index index, byte[] indexKey, boolean allowUniquePending) {
        // An extension function to reduce markIndexReadable's complexity
        CompletableFuture<Optional<Range>> builtFuture = firstUnbuiltRange(index);
        CompletableFuture<Optional<RecordIndexUniquenessViolation>> uniquenessFuture;
        if (index.isUnique()) {
            uniquenessFuture = whenAllIndexUniquenessCommitChecks(index)
                    .thenCompose(vignore -> scanUniquenessViolations(index, 1).first());
        } else {
            uniquenessFuture = CompletableFuture.completedFuture(Optional.empty());
        }
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
                if (allowUniquePending) {
                    if (isIndexReadableUniquePending(index)) {
                        return false;   // Unchanged
                    }
                    updateIndexState(index.getName(), indexKey, IndexState.READABLE_UNIQUE_PENDING);
                    // Keeping the index build data until the uniqueness violations are resolved. The build data will be
                    // cleared only at READABLE state.
                    return true;
                }
                RecordIndexUniquenessViolation wrapped = new RecordIndexUniquenessViolation("Uniqueness violation when making index readable",
                        uniquenessViolation.get());
                wrapped.addLogInfo(
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        subspaceProvider.logKey(), subspaceProvider.toString(context));
                throw wrapped;
            } else {
                updateIndexState(index.getName(), indexKey, IndexState.READABLE);
                clearReadableIndexBuildData(index);
                return true;
            }
        });
    }

    private void logExceptionAsWarn(KeyValueLogMessage message, Throwable exception) {
        if (LOGGER.isWarnEnabled()) {
            for (Throwable ex = exception;
                        ex != null;
                        ex = ex.getCause()) {
                if (ex instanceof LoggableException) {
                    message.addKeysAndValues(((LoggableException)ex).getLogInfo());
                }
            }
            LOGGER.warn(message.toString(), exception);
        }
    }

    /**
     * Marks the index with the given name as readable without checking to see if it is
     * ready. This is dangerous to do if one has not first verified that the
     * index is ready to be readable as it can cause half-built indexes to be
     * used within queries and can thus produce inconsistent results.
     * @param indexName the name of the index to mark readable
     * @return a future that will contain <code>true</code> if the store was modified
     * and <code>false</code> otherwise
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Boolean> uncheckedMarkIndexReadable(@Nonnull String indexName) {
        if (recordStoreStateRef.get() == null) {
            return preloadRecordStoreStateAsync().thenCompose(vignore -> uncheckedMarkIndexReadable(indexName));
        }

        addIndexStateReadConflict(indexName);

        beginRecordStoreStateWrite();
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
            }).whenComplete((b, t) -> endRecordStoreStateWrite()).thenApply(this::addRemoveReplacedIndexesCommitCheckIfChanged);
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
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
     * Loads the current state of the record store asynchronously and caches it in memory so that {@link #getRecordStoreState()} requires no i/o.
     * @return a future that will be complete when this store has loaded its record store state
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    protected CompletableFuture<Void> preloadRecordStoreStateAsync() {
        return preloadRecordStoreStateAsync(StoreExistenceCheck.NONE, IsolationLevel.SNAPSHOT, IsolationLevel.SNAPSHOT);
    }

    /**
     * Loads the current state of the record store asynchronously and sets {@code recordStoreStateRef}.
     * @param existenceCheck the action to be taken when the record store already exists (or does not exist yet)
     * @param storeHeaderIsolationLevel the isolation level for loading the store header
     * @param indexStateIsolationLevel the isolation level for loading index state
     * @return a future that will be complete when this store has loaded its record store state
     */
    @Nonnull
    @API(API.Status.INTERNAL)
    protected CompletableFuture<Void> preloadRecordStoreStateAsync(@Nonnull StoreExistenceCheck existenceCheck,
                                                                   @Nonnull IsolationLevel storeHeaderIsolationLevel,
                                                                   @Nonnull IsolationLevel indexStateIsolationLevel) {
        return loadRecordStoreStateAsync(existenceCheck, storeHeaderIsolationLevel, indexStateIsolationLevel)
                .thenAccept(state -> {
                    if (this.recordStoreStateRef.get() == null) {
                        recordStoreStateRef.compareAndSet(null, state.toMutable());
                    }
                });
    }

    /**
     * Loads the current state of the record store within the given subspace asynchronously. This method does not cache the
     * state in the memory.
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
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Map<String, IndexState>> loadIndexStatesAsync(@Nonnull IsolationLevel isolationLevel) {
        Subspace isSubspace = getSubspace().subspace(Tuple.from(INDEX_STATE_SPACE_KEY));
        KeyValueCursor cursor = KeyValueCursor.Builder.withSubspace(isSubspace)
                .setContext(getContext())
                .setRange(TupleRange.ALL)
                .setContinuation(null)
                .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder()
                        .setIsolationLevel(isolationLevel)
                        .setDefaultCursorStreamingMode(CursorStreamingMode.WANT_ALL)
                        .build())
                )
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
    @SuppressWarnings("PMD.CloseResource")
    private void addRecordsReadConflict() {
        if (recordsReadConflict) {
            return;
        }
        recordsReadConflict = true;
        Transaction tr = ensureContextActive();
        byte[] recordKey = getSubspace().pack(Tuple.from(RECORD_KEY));
        tr.addReadConflictRange(recordKey, ByteArrayUtil.strinc(recordKey));
    }

    /**
     * Add a read conflict key so that the transaction will fail if the index state has changed.
     * @param indexName the index to conflict on, if it's state changes
     */
    @SuppressWarnings("PMD.CloseResource")
    private void addIndexStateReadConflict(@Nonnull String indexName) {
        if (!getRecordMetaData().hasIndex(indexName)) {
            throw new MetaDataException("Index " + indexName + " does not exist in meta-data.");
        }
        if (indexStateReadConflicts.contains(indexName)) {
            return;
        } else {
            indexStateReadConflicts.add(indexName);
        }
        Transaction tr = ensureContextActive();
        byte[] indexStateKey = getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, indexName));
        tr.addReadConflictKey(indexStateKey);
    }

    /**
     * Add a read conflict key for the whole record store state.
     */
    @SuppressWarnings("PMD.CloseResource")
    private void addStoreStateReadConflict() {
        if (storeStateReadConflict) {
            return;
        }
        storeStateReadConflict = true;
        Transaction tr = ensureContextActive();
        byte[] indexStateKey = getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY));
        tr.addReadConflictRange(indexStateKey, ByteArrayUtil.strinc(indexStateKey));
    }

    /**
     * Get the state of the index for this record store. This method will not perform
     * any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     *
     * @param index the index to check for the state
     * @return the state of the given index
     * @throws IllegalArgumentException if no index in the metadata has the same name as this index
     */
    @Nonnull
    public IndexState getIndexState(@Nonnull Index index) {
        return getIndexState(index.getName());
    }

    /**
     * Get the state of the index with the given name for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param indexName the name of the index to check for the state
     * @return the state of the given index
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    @Nonnull
    public IndexState getIndexState(@Nonnull String indexName) {
        addIndexStateReadConflict(indexName);
        return getRecordStoreState().getState(indexName);
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
        return getIndexState(indexName).equals(IndexState.READABLE);
    }

    /**
     * Determine if the index is readable-unique-pending for this record store. This method will not perform
     * any queries to the underlying database and instead satisfies the answer based on the
     * in-memory cache of store state. However, if another operation in a different transaction
     * happens concurrently that changes the index's state, operations using the same {@link FDBRecordContext}
     * as this record store will fail to commit due to conflicts.
     * The readable-unique-pending index state may happen after a unique index is built, but duplications are
     * found. The index will be maintained in this mode until the last duplication is resolved, then its state
     * will be changed to READABLE.
     *
     * @param index the index to check for readability
     * @return <code>true</code> if the index is readable-unique-pending and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the same name as this index
     */
    public boolean isIndexReadableUniquePending(@Nonnull Index index) {
        return isIndexReadableUniquePending(index.getName());
    }

    /**
     * Determine if the index with the given name is readable-unique-pending for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     * The readable-unique-pending index state may happen after a unique index is built, but duplications are
     * found. The index will be maintained in this mode until the last duplication is resolved, then its state
     * will be changed to READABLE.
     *
     * @param indexName the name of the index to check for readability
     * @return <code>true</code> if the named index is readable-unique-pending and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexReadableUniquePending(@Nonnull String indexName) {
        return getIndexState(indexName).equals(IndexState.READABLE_UNIQUE_PENDING);
    }

    /**
     * Determine if the index with the given name is scannable - i.e. either {@link #isIndexReadable(Index)} or
     * {@link #isIndexReadableUniquePending(Index)} for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param index the index to check
     * @return <code>true</code> if the named index is scannable and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexScannable(@Nonnull Index index) {
        return isIndexScannable(index.getName());
    }

    /**
     * Determine if the index with the given name is scannable - i.e. either {@link #isIndexReadable(String)} or
     * {@link #isIndexReadableUniquePending(String)} for this record store.
     * This method will not perform any queries to the underlying database and instead
     * satisfies the answer based on the in-memory cache of store state.
     *
     * @param indexName the name of the index to check
     * @return <code>true</code> if the named index is scannable and <code>false</code> otherwise
     * @throws IllegalArgumentException if no index in the metadata has the given name
     */
    public boolean isIndexScannable(@Nonnull String indexName) {
        return getIndexState(indexName).isScannable();
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
        return getIndexState(indexName).equals(IndexState.WRITE_ONLY);
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
        return getIndexState(indexName).equals(IndexState.DISABLED);
    }

    /**
     * Get the progress of building the given index.
     * @param index the index to check the index build state
     * @return a future that completes to the progress of building the given index
     * @see IndexBuildState
     */
    public CompletableFuture<IndexBuildState> getIndexBuildStateAsync(Index index) {
        return IndexBuildState.loadIndexBuildStateAsync(this, index);
    }

    /**
     * Load the indexing type stamp for an index. This stamp contains information about the kind of
     * index build being used to construct a new index. This method is {@link API.Status#INTERNAL}.
     *
     * @param index the index being built
     * @return the indexing type stamp for the index's current build
     * @see #saveIndexingTypeStamp(Index, IndexBuildProto.IndexBuildIndexingStamp)
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public CompletableFuture<IndexBuildProto.IndexBuildIndexingStamp> loadIndexingTypeStampAsync(Index index) {
        byte[] stampKey = OnlineIndexer.indexBuildTypeSubspace(this, index).pack();
        return ensureContextActive().get(stampKey).thenApply(serializedStamp -> {
            if (serializedStamp == null) {
                return null;
            }
            try {
                return IndexBuildProto.IndexBuildIndexingStamp.parseFrom(serializedStamp);
            } catch (InvalidProtocolBufferException ex) {
                RecordCoreException protoEx = new RecordCoreException("invalid indexing type stamp",
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.ACTUAL, ByteArrayUtil2.loggable(serializedStamp));
                protoEx.initCause(ex);
                throw protoEx;
            }
        });
    }

    /**
     * Update the indexing type stamp for the given index. This is used by the {@link OnlineIndexer}
     * to document what kind of indexing procedure is being used to build the given index. This method
     * is {@link API.Status#INTERNAL}.
     *
     * @param index the index being built
     * @param stamp the new value of the index's indexing type stamp
     * @see #loadIndexingTypeStampAsync(Index)
     */
    @API(API.Status.INTERNAL)
    public void saveIndexingTypeStamp(Index index, IndexBuildProto.IndexBuildIndexingStamp stamp) {
        byte[] stampKey = OnlineIndexer.indexBuildTypeSubspace(this, index).pack();
        ensureContextActive().set(stampKey, stamp.toByteArray());
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
    protected CompletableFuture<Void> rebuildIndexes(@Nonnull Map<Index, List<RecordType>> indexes,
                                                     @Nonnull Map<Index, CompletableFuture<IndexState>> newStates,
                                                     @Nonnull List<CompletableFuture<Void>> work,
                                                     @Nonnull RebuildIndexReason reason,
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
                    final StringBuilder errMessageBuilder = new StringBuilder("unable to ");
                    final CompletableFuture<Void> rebuildOrMarkIndexSafely = MoreAsyncUtil.handleOnException(
                            () -> newStates.getOrDefault(index, READY_READABLE).thenCompose(
                                    indexState -> rebuildOrMarkIndex(index, indexState, recordTypes, reason, oldMetaDataVersion, errMessageBuilder)
                            ),
                            exception -> {
                                // If there is any issue, simply mark the index as disabled without blocking checkVersion
                                logExceptionAsWarn(KeyValueLogMessage.build(errMessageBuilder.toString(),
                                        LogMessageKeys.INDEX_NAME, index.getName()
                                ), exception);
                                return markIndexDisabled(index).thenApply(b -> null);
                            });
                    work.add(rebuildOrMarkIndexSafely);
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
        NEW_STORE(FDBStoreTimer.Events.REBUILD_INDEX_NEW_STORE),
        FEW_RECORDS(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS),
        COUNTS_UNKNOWN(FDBStoreTimer.Events.REBUILD_INDEX_COUNTS_UNKNOWN),
        REBUILD_ALL(FDBStoreTimer.Events.REBUILD_INDEX_REBUILD_ALL),
        EXPLICIT(FDBStoreTimer.Events.REBUILD_INDEX_EXPLICIT),
        TEST(FDBStoreTimer.Events.REBUILD_INDEX_TEST);

        public final FDBStoreTimer.Events event;

        RebuildIndexReason(FDBStoreTimer.Events event) {
            this.event = event;
        }
    }

    private boolean areAllRecordTypesSince(@Nullable Collection<RecordType> recordTypes, @Nullable Integer oldMetaDataVersion) {
        return oldMetaDataVersion != null && (oldMetaDataVersion == -1 || (recordTypes != null && recordTypes.stream().allMatch(recordType -> {
            Integer sinceVersion = recordType.getSinceVersion();
            return sinceVersion != null && sinceVersion > oldMetaDataVersion;
        })));
    }

    protected CompletableFuture<Void> rebuildOrMarkIndex(@Nonnull Index index, @Nonnull IndexState indexState,
                                                         @Nullable List<RecordType> recordTypes, @Nonnull RebuildIndexReason reason,
                                                         @Nullable Integer oldMetaDataVersion,
                                                         @Nonnull StringBuilder errMessageBuilder) {
        // Skip index rebuild if the index is on new record types. This may fail because of reusing an index name whose
        // state hasn't been cleared.
        if (indexState != IndexState.DISABLED && areAllRecordTypesSince(recordTypes, oldMetaDataVersion)) {
            errMessageBuilder.append("rebuild index with no records");
            return rebuildIndexWithNoRecord(index, reason);
        }

        switch (indexState) {
            case WRITE_ONLY:
                errMessageBuilder.append("clear and mark index write only");
                return clearAndMarkIndexWriteOnly(index).thenApply(b -> null);
            case DISABLED:
                errMessageBuilder.append("mark index disabled");
                return markIndexDisabled(index).thenApply(b -> null);
            case READABLE:
            default:
                errMessageBuilder.append("rebuild index");
                return rebuildIndex(index, reason);
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
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(msg.toString());
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(msg.toString());
                }
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
        return rebuildIndex(index, RebuildIndexReason.EXPLICIT);
    }

    @API(API.Status.INTERNAL)
    @Nonnull
    @VisibleForTesting
    @SuppressWarnings({"squid:S2095", "PMD.CloseResource"}) // Resource usage for indexBuilder is too complicated for rules.
    public CompletableFuture<Void> rebuildIndex(@Nonnull final Index index, @Nonnull RebuildIndexReason reason) {
        final boolean newStore = reason == RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index",
                                            LogMessageKeys.INDEX_NAME, index.getName(),
                                            LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                                            LogMessageKeys.REASON, reason.name(),
                                            subspaceProvider.logKey(), subspaceProvider.toString(context),
                                            LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(msg.toString());
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(msg.toString());
                }
            }
        }

        long startTime = System.nanoTime();
        OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(this).setIndex(index).build();
        CompletableFuture<Void> future = indexBuilder.rebuildIndexAsync(this)
                .thenCompose(vignore -> markIndexReadable(index))
                .handle((b, t) -> {
                    if (t != null) {
                        logExceptionAsWarn(KeyValueLogMessage.build("rebuilding index failed",
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                                LogMessageKeys.REASON, reason.name(),
                                subspaceProvider.logKey(), subspaceProvider.toString(context),
                                LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey()), t);
                    }
                    // Only call method that builds in the current transaction, so never any pending work,
                    // so it would work to close before returning future, which would look better to SonarQube.
                    // But this is better if close ever does more.
                    indexBuilder.close();
                    return null;
                });

        return context.instrument(FDBStoreTimer.Events.REBUILD_INDEX,
                context.instrument(reason.event, future, startTime),
                startTime);
    }

    @SuppressWarnings("PMD.GuardLogStatement") // Already is, but around several call.
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

    @SuppressWarnings("PMD.CloseResource")
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
                    LOGGER.info(KeyValueLogMessage.of("migrating record versions to new format",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, formatVersion,
                            subspaceProvider.logKey(), subspaceProvider.toString(context)));
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
            // This can be skipped if the store is new (in which case there is no data), or if the old
            // store did not use the old version format to store record versions
            if (!metaData.isStoreRecordVersions() && !newStore
                    && useOldVersionFormat(oldFormatVersion, omitUnsplitRecordSuffix)) {
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
        final Map<Index, List<RecordType>> indexes = metaData.getIndexesToBuildSince(oldMetaDataVersion);
        if (!indexes.isEmpty()) {
            // If all the new indexes are only for a record type whose primary key has a type prefix, then we can scan less.
            RecordType singleRecordTypeWithPrefixKey = singleRecordTypeWithPrefixKey(indexes);
            final AtomicLong recordCountRef = new AtomicLong(-1);
            final Supplier<CompletableFuture<Long>> lazyRecordCount = getAndRememberFutureLong(recordCountRef,
                    () -> getRecordCountForRebuildIndexes(newStore, rebuildRecordCounts, indexes, singleRecordTypeWithPrefixKey));
            AtomicLong recordsSizeRef = new AtomicLong(-1);
            final Supplier<CompletableFuture<Long>> lazyRecordsSize = getAndRememberFutureLong(recordsSizeRef,
                    () -> getRecordSizeForRebuildIndexes(singleRecordTypeWithPrefixKey));
            if (singleRecordTypeWithPrefixKey == null && formatVersion >= SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION && omitUnsplitRecordSuffix) {
                // Check to see if the unsplit format can be upgraded on an empty store.
                // Only works if singleRecordTypeWithPrefixKey is null as otherwise, the recordCount will not contain
                // all records
                work.add(lazyRecordCount.get().thenAccept(recordCount -> {
                    if (recordCount == 0) {
                        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                            KeyValueLogMessage msg = KeyValueLogMessage.build("upgrading unsplit format on empty store",
                                    LogMessageKeys.NEW_FORMAT_VERSION, formatVersion,
                                    subspaceProvider.logKey(), subspaceProvider.toString(context));
                            if (newStore) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(msg.toString());
                                }
                            } else {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info(msg.toString());
                                }
                            }
                        }
                        omitUnsplitRecordSuffix = formatVersion < SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION;
                        info.clearOmitUnsplitRecordSuffix();
                        addRecordsReadConflict(); // We used snapshot to determine emptiness, and are now acting on it.
                    }
                }));
            }

            Map<Index, CompletableFuture<IndexState>> newStates = getStatesForRebuildIndexes(userVersionChecker, indexes, lazyRecordCount, lazyRecordsSize, newStore, oldMetaDataVersion, oldFormatVersion);
            return rebuildIndexes(indexes, newStates, work, newStore ? RebuildIndexReason.NEW_STORE : RebuildIndexReason.FEW_RECORDS, oldMetaDataVersion).thenRun(() -> {
                // Log after checking all index states
                maybeLogIndexesNeedingRebuilding(newStates, recordCountRef, recordsSizeRef, rebuildRecordCounts, newStore);
                context.increment(FDBStoreTimer.Counts.INDEXES_NEED_REBUILDING, newStates.entrySet().size());
            });
        } else {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenAll(work);
        }
    }

    private static Supplier<CompletableFuture<Long>> getAndRememberFutureLong(@Nonnull AtomicLong ref, @Nonnull Supplier<CompletableFuture<Long>> lazyFuture) {
        return Suppliers.memoize(() -> lazyFuture.get().whenComplete((val, err) -> {
            if (err == null) {
                ref.set(val);
            }
        }));
    }

    /**
     * Get count of records to pass to a {@link UserVersionChecker} to decide whether to build right away. If all of the
     * new indexes are over a single type and that type has a record key prefix, then this count will only be over the
     * record type being indexed. If not, it will be the count of all records of all types, as in that case, the indexer
     * will need to scan the entire store to build each index. If determining the record count would be too costly (such
     * as if there is not an appropriate {@linkplain IndexTypes#COUNT count} index defined), this function may return
     * {@link Long#MAX_VALUE} to indicate that an unknown and unbounded number of records would have to be scanned
     * to build the index.
     *
     * @param newStore {@code true} if this is a brand new store
     * @param rebuildRecordCounts {@code true} if there is a record count key that needs to be rebuilt
     * @param indexes indexes that need to be built
     * @param singleRecordTypeWithPrefixKey either a single record type prefixed by the record type key or {@code null}
     * @return a future that completes to the record count for the version checker
     */
    @Nonnull
    @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.CloseResource"})
    protected CompletableFuture<Long> getRecordCountForRebuildIndexes(boolean newStore, boolean rebuildRecordCounts,
                                                                      @Nonnull Map<Index, List<RecordType>> indexes,
                                                                      @Nullable RecordType singleRecordTypeWithPrefixKey) {
        // Do this with the new indexes in write-only mode to avoid using one of them
        // when evaluating the snapshot record count.
        MutableRecordStoreState writeOnlyState = recordStoreStateRef.get().withWriteOnlyIndexes(indexes.keySet().stream().map(Index::getName).collect(Collectors.toList()));
        if (singleRecordTypeWithPrefixKey != null) {
            // Get a count for just those records, either from a COUNT index on just that type or from a universal COUNT index grouped by record type.
            MutableRecordStoreState saveState = recordStoreStateRef.get();
            try {
                recordStoreStateRef.set(writeOnlyState);
                return getSnapshotRecordCountForRecordType(singleRecordTypeWithPrefixKey.getName());
            } catch (RecordCoreException ex) {
                // No such index; have to use total record count.
            } finally {
                recordStoreStateRef.set(saveState);
            }
        }
        if (!rebuildRecordCounts) {
            MutableRecordStoreState saveState = recordStoreStateRef.get();
            try {
                recordStoreStateRef.set(writeOnlyState);
                // Note the call below can time out if the count index group cardinality is too high. Users can avoid it by
                // setting a custom UserVersionChecker that looks at the size estimate rather than the count, but we should
                // consider checking the size by default or otherwise making the ergonomics around hitting that limitation
                // better in the future
                // See: FDBRecordStoreBase.checkPossiblyRebuild() could take a long time if the record count index is split into many groups (https://github.com/FoundationDB/fdb-record-layer/issues/7)
                return getSnapshotRecordCount();
            } catch (RecordCoreException ex) {
                // Probably this was from the lack of appropriate index on count; treat like rebuildRecordCounts = true.
            } finally {
                recordStoreStateRef.set(saveState);
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
        return records.onNext().thenApply(result -> {
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
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(msg.toString());
                        }
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(msg.toString());
                        }
                    }
                }
                return 0L;
            }
        });
    }

    @Nonnull
    private CompletableFuture<Long> getRecordSizeForRebuildIndexes(@Nullable RecordType singleRecordTypeWithPrefixKey) {
        if (singleRecordTypeWithPrefixKey == null) {
            return estimateRecordsSizeAsync();
        } else {
            return estimateRecordsSizeAsync(TupleRange.allOf(singleRecordTypeWithPrefixKey.getRecordTypeKeyTuple()));
        }
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

    @SuppressWarnings("PMD.CloseResource")
    private void addConvertRecordVersions(@Nonnull List<CompletableFuture<Void>> work) {
        if (useOldVersionFormat()) {
            throw recordCoreException("attempted to convert record versions when still using older format");
        }
        final Subspace legacyVersionSubspace = getLegacyVersionSubspace();

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

    @API(API.Status.INTERNAL)
    @VisibleForTesting
    public Subspace getLegacyVersionSubspace() {
        return getSubspace().subspace(Tuple.from(RECORD_VERSION_KEY));
    }

    @Nonnull
    protected Map<Index, CompletableFuture<IndexState>> getStatesForRebuildIndexes(@Nullable UserVersionChecker userVersionChecker,
                                                                                   @Nonnull Map<Index, List<RecordType>> indexes,
                                                                                   @Nonnull Supplier<CompletableFuture<Long>> lazyRecordCount,
                                                                                   @Nonnull Supplier<CompletableFuture<Long>> lazyRecordsSize,
                                                                                   boolean newStore,
                                                                                   int oldMetaDataVersion,
                                                                                   int oldFormatVersion) {
        Map<Index, CompletableFuture<IndexState>> newStates = new HashMap<>();
        for (Map.Entry<Index, List<RecordType>> entry : indexes.entrySet()) {
            Index index = entry.getKey();
            List<RecordType> recordTypes = entry.getValue();
            boolean indexOnNewRecordTypes = areAllRecordTypesSince(recordTypes, oldMetaDataVersion);
            CompletableFuture<IndexState> stateFuture = userVersionChecker == null ?
                    lazyRecordCount.get().thenApply(recordCount -> FDBRecordStore.disabledIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes)) :
                    userVersionChecker.needRebuildIndex(index, lazyRecordCount, lazyRecordsSize, indexOnNewRecordTypes);
            if (IndexTypes.VERSION.equals(index.getType())
                    && !newStore
                    && oldFormatVersion < SAVE_VERSION_WITH_RECORD_FORMAT_VERSION
                    && !useOldVersionFormat()) {
                stateFuture = stateFuture.thenApply(state -> {
                    if (IndexState.READABLE.equals(state)) {
                        // Do not rebuild any version indexes while the format conversion is going on.
                        // Otherwise, the process moving the versions might race against the index
                        // build and some versions won't be indexed correctly.
                        return IndexState.DISABLED;
                    }
                    return state;
                });
            }
            newStates.put(index, stateFuture);
        }
        return newStates;
    }

    private void maybeLogIndexesNeedingRebuilding(@Nonnull Map<Index, CompletableFuture<IndexState>> newStates,
                                                  @Nonnull AtomicLong recordCountRef,
                                                  @Nonnull AtomicLong recordsSizeRef,
                                                  boolean rebuildRecordCounts,
                                                  boolean newStore) {
        if (LOGGER.isDebugEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("indexes need rebuilding",
                    subspaceProvider.logKey(), subspaceProvider.toString(context));

            // Log the statistics that the user version checker used to determine whether the index could be rebuilt
            // online. For both the record count and the records size estimate, a non-negative value implies that
            // the checker resolved the value
            long recordCount = recordCountRef.get();
            if (recordCount >= 0L) {
                msg.addKeyAndValue(LogMessageKeys.RECORD_COUNT, recordCount == Long.MAX_VALUE ? "unknown" : Long.toString(recordCount));
            }
            long recordsSize = recordsSizeRef.get();
            if (recordsSize >= 0L) {
                msg.addKeyAndValue(LogMessageKeys.RECORDS_SIZE_ESTIMATE, Long.toString(recordsSize));
            }

            if (rebuildRecordCounts) {
                msg.addKeyAndValue(LogMessageKeys.REBUILD_RECORD_COUNTS, "true");
            }
            Map<String, List<String>> stateNames = new HashMap<>();
            for (Map.Entry<Index, CompletableFuture<IndexState>> stateEntry : newStates.entrySet()) {
                final String stateName;
                if (MoreAsyncUtil.isCompletedNormally(stateEntry.getValue())) {
                    stateName = stateEntry.getValue().join().getLogName();
                } else {
                    stateName = "UNKNOWN";
                }
                stateNames.compute(stateName, (key, names) -> {
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
    }

    // Clear the data associated with a given index. This is only safe to do if one is
    // either going to rebuild it or disable it. It is therefore package private.
    // TODO: Better to go through the index maintainer?
    @SuppressWarnings("PMD.CloseResource")
    void clearIndexData(@Nonnull Index index) {
        context.clear(Range.startsWith(indexSubspace(index).pack())); // startsWith to handle ungrouped aggregate indexes
        context.clear(indexSecondarySubspace(index).range());
        IndexingRangeSet.forIndexBuild(this, index).clear();
        if (index.isUnique()) {
            context.clear(indexUniquenessViolationsSubspace(index).range());
        }
        // Under the index build subspace, there are 3 lower level subspaces, the lock space, the scanned records
        // subspace, and the type/stamp subspace. We are not supposed to clear the lock subspace, which is used to
        // run online index jobs which may invoke this method. We should clear:
        // * the scanned records subspace. Which, roughly speaking, counts how many records of this store are covered in
        // index range subspace.
        // * the type/stamp subspace. Which indicates which type of indexing is in progress.
        context.clear(Range.startsWith(OnlineIndexer.indexBuildScannedRecordsSubspace(this, index).pack()));
        context.clear(Range.startsWith(OnlineIndexer.indexBuildTypeSubspace(this, index).pack()));
    }

    @SuppressWarnings("PMD.CloseResource")
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
        context.clear(getSubspace().range(Tuple.from(INDEX_KEY, formerIndex.getSubspaceTupleKey())));
        context.clear(getSubspace().range(Tuple.from(INDEX_SECONDARY_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        context.clear(getSubspace().range(Tuple.from(INDEX_RANGE_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        context.clear(getSubspace().pack(Tuple.from(INDEX_STATE_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        context.clear(getSubspace().range(Tuple.from(INDEX_UNIQUENESS_VIOLATIONS_KEY, formerIndex.getSubspaceTupleKey())));
        if (getTimer() != null) {
            getTimer().recordSinceNanoTime(FDBStoreTimer.Events.REMOVE_FORMER_INDEX, startTime);
        }
    }

    private void clearReadableIndexBuildData(Index index) {
        IndexingRangeSet.forIndexBuild(this, index).clear();
    }

    @SuppressWarnings("PMD.CloseResource")
    public void vacuumReadableIndexesBuildData() {
        Map<Index, IndexState> indexStates = getAllIndexStates(); // also adds state to read conflicts
        for (Map.Entry<Index, IndexState> entry : indexStates.entrySet()) {
            if (entry.getValue().equals(IndexState.READABLE)) {
                clearReadableIndexBuildData(entry.getKey());
            }
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    protected boolean checkPossiblyRebuildRecordCounts(@Nonnull RecordMetaData metaData,
                                                       @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                       @Nonnull List<CompletableFuture<Void>> work,
                                                       int oldFormatVersion) {
        boolean existingStore = oldFormatVersion > 0;
        KeyExpression countKeyExpression = metaData.getRecordCountKey();

        boolean rebuildRecordCounts =
                (existingStore && oldFormatVersion < RECORD_COUNT_ADDED_FORMAT_VERSION)
                || (countKeyExpression != null && formatVersion >= RECORD_COUNT_KEY_ADDED_FORMAT_VERSION &&
                        (!info.hasRecordCountKey() || !KeyExpression.fromProto(info.getRecordCountKey()).equals(countKeyExpression)))
                || (countKeyExpression == null && info.hasRecordCountKey());

        if (rebuildRecordCounts) {
            // We want to clear all record counts.
            if (existingStore) {
                context.clear(getSubspace().range(Tuple.from(RECORD_COUNT_KEY)));
            }

            // Set the new record count key if we have one.
            if (formatVersion >= RECORD_COUNT_KEY_ADDED_FORMAT_VERSION) {
                if (countKeyExpression != null) {
                    info.setRecordCountKey(countKeyExpression.toKeyExpression());
                } else {
                    info.clearRecordCountKey();
                }
            }

            // Add the record rebuild job.
            if (existingStore) {
                addRebuildRecordCountsJob(work);
            }
        }
        return rebuildRecordCounts;
    }

    @SuppressWarnings("PMD.CloseResource")
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
        CompletableFuture<Void> future = records.forEach(rec -> {
            Key.Evaluated subkey = recordCountKey.evaluateSingleton(rec);
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
        return getPrimaryKeyBoundaries(recordsSubspace().pack(low), recordsSubspace().pack(high));
    }

    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public RecordCursor<Tuple> getPrimaryKeyBoundaries(@Nullable TupleRange tupleRange) {
        if (tupleRange == null) {
            tupleRange = TupleRange.ALL;
        }
        Range range = tupleRange.toRange(recordsSubspace());
        return getPrimaryKeyBoundaries(range.begin, range.end);
    }

    @SuppressWarnings("PMD.CloseResource")
    private RecordCursor<Tuple> getPrimaryKeyBoundaries(byte[] rangeStart, byte[] rangeEnd) {
        final Transaction transaction = ensureContextActive();
        CloseableAsyncIterator<byte[]> cursor = context.getDatabase().getLocalityProvider().getBoundaryKeys(transaction, rangeStart, rangeEnd);
        final boolean hasSplitRecordSuffix = hasSplitRecordSuffix();
        DistinctFilterCursorClosure closure = new DistinctFilterCursorClosure();
        return RecordCursor.flatMapPipelined(ignore -> RecordCursor.fromIterator(getExecutor(), cursor),
                (result, ignore) -> RecordCursor.fromIterator(getExecutor(),
                        transaction.snapshot().getRange(result, rangeEnd, 1).iterator()),
                null, DEFAULT_PIPELINE_SIZE)
                .map(keyValue -> {
                    Tuple recordKey = recordsSubspace().unpack(keyValue.getKey());
                    return hasSplitRecordSuffix ? recordKey.popBack() : recordKey;
                })
                // The input stream is expected to be sorted so this filter can work to de-duplicate the data.
                .filter(closure::pred);
    }

    private static class DistinctFilterCursorClosure {
        private Tuple previousKey = null;

        boolean pred(Tuple key) {
            if (key.equals(previousKey)) {
                return false;
            } else {
                previousKey = key;
                return true;
            }
        }
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
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<byte[]> repairRecordKeys(@Nullable byte[] continuation,
                                                      @Nonnull ScanProperties scanProperties,
                                                      final boolean isDryRun) {
        // If the records aren't split to begin with, then there is nothing to do.
        if (getRecordMetaData().isSplitLongRecords()) {
            return CompletableFuture.completedFuture(null);
        }

        if (scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot()) {
            throw new RecordCoreArgumentException("Cannot repair record key split markers at SNAPSHOT isolation level")
                    .addLogInfo(LogMessageKeys.SCAN_PROPERTIES, scanProperties)
                    .addLogInfo(subspaceProvider.logKey(), subspaceProvider.toString(context));
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

    @SuppressWarnings("PMD.CloseResource")
    private void repairRecordKeyIfNecessary(@Nonnull FDBRecordContext context, @Nonnull Subspace recordSubspace,
                                            @Nonnull KeyValue keyValue, final boolean isDryRun) {
        final RecordMetaData metaData = metaDataProvider.getRecordMetaData();
        final Tuple recordKey = recordSubspace.unpack(keyValue.getKey());

        // Ignore version key
        if (metaData.isStoreRecordVersions() && isMaybeVersion(recordKey)) {
            return;
        }

        final Message protoRecord = serializer.deserialize(metaData, recordKey, keyValue.getValue(), getTimer());
        final RecordType recordType = metaData.getRecordTypeForDescriptor(protoRecord.getDescriptorForType());

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
    private RecordCoreException recordCoreException(@Nonnull String msg) {
        return new RecordCoreException(msg,
                subspaceProvider.logKey(), subspaceProvider.toString(context));
    }

    @Nonnull
    private RecordCoreException recordCoreException(@Nonnull String msg, Object... keysAndValues) {
        RecordCoreException err = new RecordCoreException(msg, keysAndValues);
        err.addLogInfo(subspaceProvider.logKey().toString(), subspaceProvider.toString(context));
        return err;
    }

    @Nonnull
    private UninitializedRecordStoreException uninitializedStoreException(@Nonnull String msg) {
        return new UninitializedRecordStoreException(msg,
                subspaceProvider.logKey(), subspaceProvider.toString(context));
    }


    /**
     * For some indexes, there are index maintenance operations that could be either done inline, during a record update
     * operation, or later in the background. The returned control object lets the caller indicate to the index maintenance
     * that he intends to trigger these deferred maintenance in another, possibly background, transaction.
     * This feature is experimental. The default is to perform all the needed index changes inline.
     * @return an {@link IndexDeferredMaintenanceControl} object.
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public synchronized IndexDeferredMaintenanceControl getIndexDeferredMaintenanceControl() {
        if (indexDeferredMaintenanceControl == null) {
            indexDeferredMaintenanceControl = new IndexDeferredMaintenanceControl();
        }
        return indexDeferredMaintenanceControl;
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

        @Nonnull
        private StateCacheabilityOnOpen stateCacheabilityOnOpen = StateCacheabilityOnOpen.DEFAULT;

        @Nonnull
        private PlanSerializationRegistry planSerializationRegistry = DefaultPlanSerializationRegistry.INSTANCE;

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
            this.stateCacheabilityOnOpen = other.stateCacheabilityOnOpen;
            this.planSerializationRegistry = other.planSerializationRegistry;
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
            this.userVersionChecker = store.userVersionChecker;
            this.indexMaintainerRegistry = store.indexMaintainerRegistry;
            this.indexMaintenanceFilter = store.indexMaintenanceFilter;
            this.pipelineSizer = store.pipelineSizer;
            this.storeStateCache = store.storeStateCache;
            this.stateCacheabilityOnOpen = store.stateCacheabilityOnOpen;
            this.planSerializationRegistry = store.planSerializationRegistry;
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
        public StateCacheabilityOnOpen getStateCacheabilityOnOpen() {
            return stateCacheabilityOnOpen;
        }

        @Override
        @Nonnull
        public Builder setStateCacheabilityOnOpen(@Nonnull final StateCacheabilityOnOpen stateCacheabilityOnOpen) {
            this.stateCacheabilityOnOpen = stateCacheabilityOnOpen;
            return this;
        }

        @Nonnull
        public PlanSerializationRegistry getPlanSerializationRegistry() {
            return planSerializationRegistry;
        }

        public void setPlanSerializationRegistry(@Nonnull final PlanSerializationRegistry planSerializationRegistry) {
            this.planSerializationRegistry = planSerializationRegistry;
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
                    serializer, indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer, storeStateCache, stateCacheabilityOnOpen,
                    userVersionChecker, planSerializationRegistry);
        }

        @Override
        @Nonnull
        public CompletableFuture<FDBRecordStore> uncheckedOpenAsync() {
            final CompletableFuture<Long> readVersionFuture = preloadReadVersion();
            final CompletableFuture<Void> preloadMetaData = readVersionFuture.thenCompose(ignore -> preloadMetaData());
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
            final CompletableFuture<Long> readVersionFuture = preloadReadVersion();
            final CompletableFuture<Void> preloadMetaData = readVersionFuture.thenCompose(ignore -> preloadMetaData());
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

        /**
         * Load the read version for instrumentation purposes. This assumes that for many transactions, opening
         * a record store will be the first thing that is done, so this ensures that in most cases, the read version
         * is instrumented (separately from the rest of the store opening method). If the user has already called
         * {@link FDBRecordContext#getReadVersionAsync()} or {@link FDBRecordContext#getReadVersion()}, this will
         * not add spurious instrumentation as the future will be re-used.
         *
         * @return a future that will contain the transaction's read version
         * @see FDBRecordContext#getReadVersionAsync()
         */
        @Nonnull
        private CompletableFuture<Long> preloadReadVersion() {
            if (context == null) {
                throw new RecordCoreException("record context must be supplied");
            }
            return context.getReadVersionAsync();
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
