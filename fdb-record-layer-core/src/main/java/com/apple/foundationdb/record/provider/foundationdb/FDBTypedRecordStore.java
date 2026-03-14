/*
 * FDBTypedRecordStore.java
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
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.MessageBuilderRecordSerializer;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TypedRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A type-safe record store.
 *
 * Takes a serializer that operates on a specific message type and an underlying record store and makes
 * operations for reading and writing records type-safe while only operating on that one type.
 * The record store can contain other record types which can be accessed using a different typed record
 * store with the same underlying untyped record store.
 *
 * @param <M> type used to represent stored records
 * @see FDBRecordStore
 * @see FDBRecordStoreBase
 */
@API(API.Status.UNSTABLE)
public class FDBTypedRecordStore<M extends Message> implements FDBRecordStoreBase<M> {

    @Nonnull
    private final FDBRecordStore untypedStore;
    @Nonnull
    private final RecordSerializer<M> typedSerializer;

    protected FDBTypedRecordStore(@Nonnull FDBRecordStore untypedStore, @Nonnull RecordSerializer<M> typedSerializer) {
        this.untypedStore = untypedStore;
        this.typedSerializer = typedSerializer;
    }

    @Override
    public FDBRecordStore getUntypedRecordStore() {
        return untypedStore;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return untypedStore.getRecordMetaData();
    }

    @Nonnull
    @Override
    public FDBRecordContext getContext() {
        return untypedStore.getContext();
    }

    @Nullable
    @Override
    public SubspaceProvider getSubspaceProvider() {
        return untypedStore.getSubspaceProvider();
    }

    @Nonnull
    @Override
    public RecordStoreState getRecordStoreState() {
        return untypedStore.getRecordStoreState();
    }

    @Nonnull
    @Override
    public RecordSerializer<M> getSerializer() {
        return typedSerializer;
    }


    @Nonnull
    @Override
    public IndexMaintainerFactoryRegistry getIndexMaintainerRegistry() {
        return untypedStore.getIndexMaintainerRegistry();
    }

    @Nonnull
    @Override
    public IndexMaintainer getIndexMaintainer(@Nonnull final Index index) {
        return untypedStore.getIndexMaintainer(index);
    }

    @Override
    public int getIncarnation() {
        return untypedStore.getIncarnation();
    }

    @Override
    public CompletableFuture<Void> updateIncarnation(@Nonnull final IntFunction<Integer> updater) {
        return untypedStore.updateIncarnation(updater);
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull M rec, @Nonnull RecordExistenceCheck existenceCheck, @Nullable FDBRecordVersion version, @Nonnull VersionstampSaveBehavior behavior) {
        return untypedStore.saveTypedRecord(typedSerializer, rec, existenceCheck, version, behavior);
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBStoredRecord<M>> dryRunSaveRecordAsync(@Nonnull M rec, @Nonnull RecordExistenceCheck existenceCheck, @Nullable FDBRecordVersion version, @Nonnull VersionstampSaveBehavior behavior) {
        return untypedStore.saveTypedRecord(typedSerializer, rec, existenceCheck, version, behavior, true, false);
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBStoredRecord<M>> loadRecordInternal(@Nonnull Tuple primaryKey, @Nonnull ExecuteState executeState, boolean snapshot) {
        return untypedStore.loadTypedRecord(typedSerializer, primaryKey, snapshot);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> preloadRecordAsync(@Nonnull Tuple primaryKey) {
        return untypedStore.preloadRecordAsync(primaryKey);
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBSyntheticRecord> loadSyntheticRecord(@Nonnull final Tuple primaryKey, final IndexOrphanBehavior orphanBehavior) {
        throw new RecordCoreException("api unsupported on typed store");
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> recordExistsAsync(@Nonnull Tuple primaryKey, @Nonnull final IsolationLevel isolationLevel) {
        return untypedStore.recordExistsAsync(primaryKey, isolationLevel);
    }

    @Override
    public void addRecordReadConflict(@Nonnull Tuple primaryKey) {
        untypedStore.addRecordReadConflict(primaryKey);
    }

    @Override
    public void addRecordWriteConflict(@Nonnull Tuple primaryKey) {
        untypedStore.addRecordWriteConflict(primaryKey);
    }

    @Nonnull
    @Override
    public RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable Tuple low, @Nullable Tuple high, @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return untypedStore.scanTypedRecords(typedSerializer, low, high, lowEndpoint, highEndpoint, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<Tuple> scanRecordKeys(@Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        return untypedStore.scanRecordKeys(continuation, scanProperties);
    }

    @Nonnull
    @Override
    public CompletableFuture<Integer> countRecords(@Nullable Tuple low, @Nullable Tuple high, @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return untypedStore.countRecords(low, high, lowEndpoint, highEndpoint, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanIndex(@Nonnull Index index, @Nonnull IndexScanBounds scanBounds, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return untypedStore.scanIndex(index, scanBounds, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRemoteFetch(@Nonnull Index index,
                                                                  @Nonnull IndexScanBounds scanBounds,
                                                                  int commonPrimaryKeyLength,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ScanProperties scanProperties,
                                                                  @Nonnull final IndexOrphanBehavior orphanBehavior) {
        return untypedStore.scanIndexRemoteFetchInternal(index, scanBounds, commonPrimaryKeyLength, continuation, typedSerializer, scanProperties, orphanBehavior);
    }

    @Override
    @Nonnull
    public CompletableFuture<FDBIndexedRecord<M>> buildSingleRecord(@Nonnull FDBIndexedRawRecord indexedRawRecord) {
        return untypedStore.buildSingleRecordInternal(indexedRawRecord, typedSerializer, null);
    }

    @Nonnull
    @Override
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(@Nonnull Index index, @Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return untypedStore.scanUniquenessViolations(index, range, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> resolveUniquenessViolation(@Nonnull Index index, @Nonnull Tuple valueKey, @Nullable Tuple primaryKey) {
        return untypedStore.resolveUniquenessViolation(index, valueKey, primaryKey);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> dryRunDeleteRecordAsync(@Nonnull Tuple primaryKey) {
        return untypedStore.deleteTypedRecord(typedSerializer, primaryKey, true);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> deleteRecordAsync(@Nonnull Tuple primaryKey) {
        return untypedStore.deleteTypedRecord(typedSerializer, primaryKey, false);
    }

    @Override
    public void deleteAllRecords() {
        untypedStore.deleteAllRecords();
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> deleteRecordsWhereAsync(@Nonnull QueryComponent component) {
        return untypedStore.deleteRecordsWhereAsync(component);
    }

    @Nonnull
    @Override
    public PipelineSizer getPipelineSizer() {
        return untypedStore.getPipelineSizer();
    }

    @Nonnull
    @Override
    public CompletableFuture<Long> estimateStoreSizeAsync() {
        return untypedStore.estimateStoreSizeAsync();
    }

    @Nonnull
    @Override
    public CompletableFuture<Long> estimateRecordsSizeAsync(@Nonnull TupleRange range) {
        return untypedStore.estimateRecordsSizeAsync(range);
    }

    @Nonnull
    @Override
    public CompletableFuture<Long> getSnapshotRecordCount(@Nonnull KeyExpression key, @Nonnull Key.Evaluated value,
                                                          @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.getSnapshotRecordCount(key, value, indexQueryabilityFilter);
    }

    @Nonnull
    @Override
    public CompletableFuture<Long> getSnapshotRecordCountForRecordType(@Nonnull String recordTypeName,
                                                                       @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.getSnapshotRecordCountForRecordType(recordTypeName, indexQueryabilityFilter);
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> evaluateIndexRecordFunction(@Nonnull EvaluationContext evaluationContext, @Nonnull IndexRecordFunction<T> function, @Nonnull FDBRecord<M> rec) {
        return untypedStore.evaluateTypedIndexRecordFunction(evaluationContext, function, rec);
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> evaluateStoreFunction(@Nonnull EvaluationContext evaluationContext, @Nonnull StoreRecordFunction<T> function, @Nonnull FDBRecord<M> rec) {
        return untypedStore.evaluateTypedStoreFunction(evaluationContext, function, rec);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel,
                                                              @Nonnull IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.evaluateAggregateFunction(recordTypeNames, aggregateFunction, range, isolationLevel,
                indexQueryabilityFilter);
    }

    @Nonnull
    @Override
    public RecordQueryPlan planQuery(@Nonnull final RecordQuery query, @Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
        return untypedStore.planQuery(query, parameterRelationshipGraph);
    }

    @Nonnull
    @Override
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph,
                                     @Nonnull RecordQueryPlannerConfiguration plannerConfiguration) {
        return untypedStore.planQuery(query, parameterRelationshipGraph, plannerConfiguration);
    }

    @Nonnull
    @Override
    public RecordQueryPlan planQuery(@Nonnull RecordQuery query) {
        return planQuery(query, ParameterRelationshipGraph.empty());
    }

    /**
     * A builder for {@link FDBTypedRecordStore}.
     *
     * This is only used when creating both the untyped and typed record stores at the same time.
     * To create a new typed record store from an open record store, use {@link FDBRecordStoreBase#getTypedRecordStore}.
     * @param <M> generated Protobuf class for the record message type
     * @see FDBTypedRecordStore#newBuilder
     */
    public static class Builder<M extends Message> implements BaseBuilder<M, FDBTypedRecordStore<M>> {

        @Nonnull
        private final FDBRecordStore.Builder untypedStoreBuilder;
        @Nullable
        private RecordSerializer<M> typedSerializer;

        protected Builder() {
            this.untypedStoreBuilder = FDBRecordStore.newBuilder();
        }

        protected Builder(Builder<M> other) {
            this.untypedStoreBuilder = other.untypedStoreBuilder.copyBuilder();
            this.typedSerializer = other.typedSerializer;
        }

        protected Builder(FDBTypedRecordStore<M> store) {
            this.untypedStoreBuilder = store.untypedStore.asBuilder();
            this.typedSerializer = store.typedSerializer;
        }

        @Nullable
        @Override
        public RecordSerializer<M> getSerializer() {
            return typedSerializer;
        }

        @Nonnull
        @Override
        public Builder<M> setSerializer(@Nullable RecordSerializer<M> typedSerializer) {
            this.typedSerializer = typedSerializer;
            return this;
        }

        /**
         * Get the serializer that will be used by the underlying record store for non-typed operations such as building indexes.
         * @return untyped serializer
         */
        @Nonnull
        public RecordSerializer<Message> getUntypedSerializer() {
            return untypedStoreBuilder.getSerializer();
        }

        /**
         * Get the serializer that will be used by the underlying record store for non-typed operations such as building indexes.
         * @param serializer untyped serializer
         * @return this builder
         */
        @Nonnull
        public Builder<M> setUntypedSerializer(@Nonnull RecordSerializer<Message> serializer) {
            untypedStoreBuilder.setSerializer(serializer);
            return this;
        }

        @Override
        @Deprecated(forRemoval = true)
        @SuppressWarnings("removal") // this method is deprecated to be removed with parent
        public int getFormatVersion() {
            return untypedStoreBuilder.getFormatVersion();
        }

        @Override
        public FormatVersion getFormatVersionEnum() {
            return untypedStoreBuilder.getFormatVersionEnum();
        }

        @Override
        @Nonnull
        @Deprecated(forRemoval = true)
        @SuppressWarnings("removal") // this method is deprecated to be removed with parent
        public Builder<M> setFormatVersion(int formatVersion) {
            untypedStoreBuilder.setFormatVersion(formatVersion);
            return this;
        }

        @Override
        public BaseBuilder<M, FDBTypedRecordStore<M>> setFormatVersion(final FormatVersion formatVersion) {
            untypedStoreBuilder.setFormatVersion(formatVersion);
            return this;
        }

        @Nullable
        @Override
        public RecordMetaDataProvider getMetaDataProvider() {
            return untypedStoreBuilder.getMetaDataProvider();
        }

        @Nonnull
        @Override
        public Builder<M> setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider) {
            untypedStoreBuilder.setMetaDataProvider(metaDataProvider);
            return this;
        }

        @Nullable
        @Override
        public FDBMetaDataStore getMetaDataStore() {
            return untypedStoreBuilder.getMetaDataStore();
        }

        @Nonnull
        @Override
        public Builder<M> setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore) {
            untypedStoreBuilder.setMetaDataStore(metaDataStore);
            return this;
        }

        @Nullable
        @Override
        public FDBRecordContext getContext() {
            return untypedStoreBuilder.getContext();
        }

        @Nonnull
        @Override
        public Builder<M> setContext(@Nullable FDBRecordContext context) {
            untypedStoreBuilder.setContext(context);
            return this;
        }

        @Nullable
        @Override
        public SubspaceProvider getSubspaceProvider() {
            return untypedStoreBuilder.getSubspaceProvider();
        }

        @Nonnull
        @Override
        public Builder<M> setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider) {
            untypedStoreBuilder.setSubspaceProvider(subspaceProvider);
            return this;
        }

        @Nonnull
        @Override
        public Builder<M> setSubspace(@Nullable Subspace subspace) {
            untypedStoreBuilder.setSubspace(subspace);
            return this;
        }

        @Nonnull
        @Override
        public Builder<M> setKeySpacePath(@Nullable KeySpacePath keySpacePath) {
            untypedStoreBuilder.setKeySpacePath(keySpacePath);
            return this;
        }

        @Nullable
        @Override
        public UserVersionChecker getUserVersionChecker() {
            return untypedStoreBuilder.getUserVersionChecker();
        }

        @Nonnull
        @Override
        public Builder<M> setUserVersionChecker(@Nullable UserVersionChecker userVersionChecker) {
            untypedStoreBuilder.setUserVersionChecker(userVersionChecker);
            return this;
        }

        @Nonnull
        @Override
        public IndexMaintainerFactoryRegistry getIndexMaintainerRegistry() {
            return untypedStoreBuilder.getIndexMaintainerRegistry();
        }

        @Nonnull
        @Override
        public Builder<M> setIndexMaintainerRegistry(@Nonnull IndexMaintainerFactoryRegistry indexMaintainerRegistry) {
            untypedStoreBuilder.setIndexMaintainerRegistry(indexMaintainerRegistry);
            return this;
        }

        @Nonnull
        @Override
        public IndexMaintenanceFilter getIndexMaintenanceFilter() {
            return untypedStoreBuilder.getIndexMaintenanceFilter();
        }

        @Nonnull
        @Override
        public Builder<M> setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            untypedStoreBuilder.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        @Nonnull
        @Override
        public PipelineSizer getPipelineSizer() {
            return untypedStoreBuilder.getPipelineSizer();
        }

        @Nonnull
        @Override
        public Builder<M> setPipelineSizer(@Nonnull PipelineSizer pipelineSizer) {
            untypedStoreBuilder.setPipelineSizer(pipelineSizer);
            return this;
        }

        @Nonnull
        @Override
        public FDBRecordStoreStateCache getStoreStateCache() {
            return untypedStoreBuilder.getStoreStateCache();
        }

        @Nonnull
        @Override
        public Builder<M> setStoreStateCache(@Nonnull FDBRecordStoreStateCache storeStateCache) {
            untypedStoreBuilder.setStoreStateCache(storeStateCache);
            return this;
        }

        @Nonnull
        @Override
        public FDBRecordStore.StateCacheabilityOnOpen getStateCacheabilityOnOpen() {
            return untypedStoreBuilder.getStateCacheabilityOnOpen();
        }

        @Override
        @Nonnull
        public BaseBuilder<M, FDBTypedRecordStore<M>> setStateCacheabilityOnOpen(@Nonnull final FDBRecordStore.StateCacheabilityOnOpen stateCacheabilityOnOpen) {
            untypedStoreBuilder.setStateCacheabilityOnOpen(stateCacheabilityOnOpen);
            return this;
        }

        @Nullable
        @Override
        public String getBypassFullStoreLockReason() {
            return untypedStoreBuilder.getBypassFullStoreLockReason();
        }

        @Nonnull
        @Override
        public Builder<M> setBypassFullStoreLockReason(@Nullable final String reason) {
            untypedStoreBuilder.setBypassFullStoreLockReason(reason);
            return this;
        }

        @Nonnull
        @Override
        public CompletableFuture<FDBTypedRecordStore<M>> uncheckedOpenAsync() {
            return untypedStoreBuilder.uncheckedOpenAsync()
                    .thenApply(untypedStore -> new FDBTypedRecordStore<>(untypedStore, typedSerializer));
        }

        @Nonnull
        @Override
        public CompletableFuture<FDBTypedRecordStore<M>> createOrOpenAsync(@Nonnull StoreExistenceCheck existenceCheck) {
            return untypedStoreBuilder.createOrOpenAsync(existenceCheck)
                    .thenApply(untypedStore -> new FDBTypedRecordStore<>(untypedStore, typedSerializer));
        }

        @Nonnull
        @Override
        public FDBTypedRecordStore<M> build() {
            if (typedSerializer == null) {
                throw new RecordCoreException("typed serializer must be specified");
            }
            if (untypedStoreBuilder.getSerializer() == null) {
                untypedStoreBuilder.setSerializer(typedSerializer.widen());
            }
            return new FDBTypedRecordStore<>(untypedStoreBuilder.build(), typedSerializer);
        }

        @Override
        @Nonnull
        public Builder<M> copyBuilder() {
            return new Builder<>(this);
        }

    }

    /**
     * Create a new typed record store builder.
     *
     * @param <M> generated Protobuf class for the record message type
     * @return an uninitialized builder
     */
    @Nonnull
    public static <M extends Message> Builder<M> newBuilder() {
        return new Builder<>();
    }

    /**
     * Create a new typed record store builder.
     *
     * @param serializer a typed serializer to use
     * @param <M> generated Protobuf class for the record message type
     * @return an uninitialized builder
     */
    @Nonnull
    public static <M extends Message> Builder<M> newBuilder(@Nonnull RecordSerializer<M> serializer) {
        return new Builder<M>().setSerializer(serializer);
    }

    /**
     * Create a new typed record store builder.
     *
     * @param fieldDescriptor field descriptor for the union field used to hold the target record type
     * @param builderSupplier builder for the union message type
     * @param tester predicate to determine whether an instance of the union message has the target record type
     * @param getter accessor to get record message instance from union message instance
     * @param setter access to store record message instance into union message instance
     * @param <M> generated Protobuf class for the record message type
     * @param <U> generated Protobuf class for the union message
     * @param <B> generated Protobuf class for the union message's builder
     * @return a builder using the given functions
     */
    @Nonnull
    public static <M extends Message, U extends Message, B extends Message.Builder> Builder<M> newBuilder(@Nonnull Descriptors.FieldDescriptor fieldDescriptor,
                                                                                                          @Nonnull Supplier<B> builderSupplier,
                                                                                                          @Nonnull Predicate<U> tester,
                                                                                                          @Nonnull Function<U, M> getter,
                                                                                                          @Nonnull BiConsumer<B, M> setter) {
        RecordSerializer<M> typedSerializer = new TypedRecordSerializer<>(fieldDescriptor, builderSupplier, tester, getter, setter);
        RecordSerializer<Message> untypedSerializer = new MessageBuilderRecordSerializer(builderSupplier::get);
        return newBuilder(typedSerializer).setUntypedSerializer(untypedSerializer);
    }

    /**
     * Create a new typed record store builder.
     *
     * <pre><code>
     * static final FDBTypedRecordStore.Builder&lt;MyProto.MyRecord, MyProto.RecordTypeUnion, MyProto.RecordTypeUnion.Builder&gt; BUILDER =
     * FDBTypedRecordStore.newBuilder(
     *         MyProto.getDescriptor(),
     *         MyProto.RecordTypeUnion.getDescriptor().findFieldByNumber(MyProto.RecordTypeUnion._MYRECORD_FIELD_NUMBER),
     *         MyProto.RecordTypeUnion::newBuilder,
     *         MyProto.RecordTypeUnion::hasMyRecord,
     *         MyProto.RecordTypeUnion::getMyRecord,
     *         MyProto.RecordTypeUnion.Builder::setMyRecord)
     *
     * final FDBTypedRecordStore&lt;MyProto.MyRecord, MyProto.RecordTypeUnion, MyProto.RecordTypeUnion.Builder&gt; store =
     *     BUILDER.copyBuilder().setContext(ctx).setSubspace(s).createOrOpen();
     * final MyProto.MyRecord myrec1 = store.loadRecord(pkey).getRecord();
     * </code></pre>
     *
     * @param fileDescriptor file descriptor for all record message types
     * @param fieldDescriptor field descriptor for the union field used to hold the target record type
     * @param builderSupplier builder for the union message type
     * @param tester predicate to determine whether an instance of the union message has the target record type
     * @param getter accessor to get record message instance from union message instance
     * @param setter access to store record message instance into union message instance
     * @param <M> generated Protobuf class for the record message type
     * @param <U> generated Protobuf class for the union message
     * @param <B> generated Protobuf class for the union message's builder
     * @return a builder using the given functions
     */
    @Nonnull
    public static <M extends Message, U extends Message, B extends Message.Builder> Builder<M> newBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                                                                          @Nonnull Descriptors.FieldDescriptor fieldDescriptor,
                                                                                                          @Nonnull Supplier<B> builderSupplier,
                                                                                                          @Nonnull Predicate<U> tester,
                                                                                                          @Nonnull Function<U, M> getter,
                                                                                                          @Nonnull BiConsumer<B, M> setter) {
        RecordMetaData metaData = RecordMetaData.build(fileDescriptor);
        return newBuilder(fieldDescriptor, builderSupplier, tester, getter, setter).setMetaDataProvider(metaData);
    }

    @Nonnull
    public Builder<M> asBuilder() {
        return new Builder<>(this);
    }

}
