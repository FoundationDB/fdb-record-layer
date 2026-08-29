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

import org.jspecify.annotations.Nullable;

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

    private final FDBRecordStore untypedStore;
    private final RecordSerializer<M> typedSerializer;

    protected FDBTypedRecordStore(FDBRecordStore untypedStore, RecordSerializer<M> typedSerializer) {
        this.untypedStore = untypedStore;
        this.typedSerializer = typedSerializer;
    }

    @Override
    public FDBRecordStore getUntypedRecordStore() {
        return untypedStore;
    }

    @Override
    public RecordMetaData getRecordMetaData() {
        return untypedStore.getRecordMetaData();
    }

    @Override
    public FDBRecordContext getContext() {
        return untypedStore.getContext();
    }

    @Nullable
    @Override
    public SubspaceProvider getSubspaceProvider() {
        return untypedStore.getSubspaceProvider();
    }

    @Override
    public RecordStoreState getRecordStoreState() {
        return untypedStore.getRecordStoreState();
    }

    @Override
    public RecordSerializer<M> getSerializer() {
        return typedSerializer;
    }


    @Override
    public IndexMaintainerFactoryRegistry getIndexMaintainerRegistry() {
        return untypedStore.getIndexMaintainerRegistry();
    }

    @Override
    public IndexMaintainer getIndexMaintainer(final Index index) {
        return untypedStore.getIndexMaintainer(index);
    }

    @Override
    public int getIncarnation() {
        return untypedStore.getIncarnation();
    }

    @Override
    public CompletableFuture<Void> updateIncarnation(final IntFunction<Integer> updater) {
        return untypedStore.updateIncarnation(updater);
    }

    @Override
    public CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(M rec, RecordExistenceCheck existenceCheck, @Nullable FDBRecordVersion version, VersionstampSaveBehavior behavior) {
        return untypedStore.saveTypedRecord(typedSerializer, rec, existenceCheck, version, behavior);
    }

    @Override
    public CompletableFuture<FDBStoredRecord<M>> dryRunSaveRecordAsync(M rec, RecordExistenceCheck existenceCheck, @Nullable FDBRecordVersion version, VersionstampSaveBehavior behavior) {
        return untypedStore.saveTypedRecord(typedSerializer, rec, existenceCheck, version, behavior, true, false);
    }

    @Override
    public CompletableFuture<FDBStoredRecord<M>> loadRecordInternal(Tuple primaryKey, ExecuteState executeState, boolean snapshot) {
        return untypedStore.loadTypedRecord(typedSerializer, primaryKey, snapshot);
    }

    @Override
    public CompletableFuture<Void> preloadRecordAsync(Tuple primaryKey) {
        return untypedStore.preloadRecordAsync(primaryKey);
    }

    @Override
    public CompletableFuture<FDBSyntheticRecord> loadSyntheticRecord(final Tuple primaryKey, final IndexOrphanBehavior orphanBehavior) {
        throw new RecordCoreException("api unsupported on typed store");
    }

    @Override
    public CompletableFuture<Boolean> recordExistsAsync(Tuple primaryKey, final IsolationLevel isolationLevel) {
        return untypedStore.recordExistsAsync(primaryKey, isolationLevel);
    }

    @Override
    public void addRecordReadConflict(Tuple primaryKey) {
        untypedStore.addRecordReadConflict(primaryKey);
    }

    @Override
    public void addRecordWriteConflict(Tuple primaryKey) {
        untypedStore.addRecordWriteConflict(primaryKey);
    }

    @Override
    public RecordCursor<FDBStoredRecord<M>> scanRecords(@Nullable Tuple low, @Nullable Tuple high, EndpointType lowEndpoint, EndpointType highEndpoint, @Nullable byte[] continuation, ScanProperties scanProperties) {
        return untypedStore.scanTypedRecords(typedSerializer, low, high, lowEndpoint, highEndpoint, continuation, scanProperties);
    }

    @Override
    public RecordCursor<Tuple> scanRecordKeys(@Nullable final byte[] continuation, final ScanProperties scanProperties) {
        return untypedStore.scanRecordKeys(continuation, scanProperties);
    }

    @Override
    public CompletableFuture<Integer> countRecords(@Nullable Tuple low, @Nullable Tuple high, EndpointType lowEndpoint, EndpointType highEndpoint, @Nullable byte[] continuation, ScanProperties scanProperties) {
        return untypedStore.countRecords(low, high, lowEndpoint, highEndpoint, continuation, scanProperties);
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(Index index, IndexScanBounds scanBounds, @Nullable byte[] continuation, ScanProperties scanProperties) {
        return untypedStore.scanIndex(index, scanBounds, continuation, scanProperties);
    }

    @Override
    public RecordCursor<FDBIndexedRecord<M>> scanIndexRemoteFetch(Index index,
                                                                  IndexScanBounds scanBounds,
                                                                  int commonPrimaryKeyLength,
                                                                  @Nullable byte[] continuation,
                                                                  ScanProperties scanProperties,
                                                                  final IndexOrphanBehavior orphanBehavior) {
        return untypedStore.scanIndexRemoteFetchInternal(index, scanBounds, commonPrimaryKeyLength, continuation, typedSerializer, scanProperties, orphanBehavior);
    }

    @Override
    public CompletableFuture<FDBIndexedRecord<M>> buildSingleRecord(FDBIndexedRawRecord indexedRawRecord) {
        return untypedStore.buildSingleRecordInternal(indexedRawRecord, typedSerializer, null);
    }

    @Override
    public RecordCursor<RecordIndexUniquenessViolation> scanUniquenessViolations(Index index, TupleRange range, @Nullable byte[] continuation, ScanProperties scanProperties) {
        return untypedStore.scanUniquenessViolations(index, range, continuation, scanProperties);
    }

    @Override
    public CompletableFuture<Void> resolveUniquenessViolation(Index index, Tuple valueKey, @Nullable Tuple primaryKey) {
        return untypedStore.resolveUniquenessViolation(index, valueKey, primaryKey);
    }

    @Override
    public CompletableFuture<Boolean> dryRunDeleteRecordAsync(Tuple primaryKey) {
        return untypedStore.deleteTypedRecord(typedSerializer, primaryKey, true);
    }

    @Override
    public CompletableFuture<Boolean> deleteRecordAsync(Tuple primaryKey) {
        return untypedStore.deleteTypedRecord(typedSerializer, primaryKey, false);
    }

    @Override
    public void deleteAllRecords() {
        untypedStore.deleteAllRecords();
    }

    @Override
    public CompletableFuture<Void> deleteRecordsWhereAsync(QueryComponent component) {
        return untypedStore.deleteRecordsWhereAsync(component);
    }

    @Override
    public PipelineSizer getPipelineSizer() {
        return untypedStore.getPipelineSizer();
    }

    @Override
    public CompletableFuture<Long> estimateStoreSizeAsync() {
        return untypedStore.estimateStoreSizeAsync();
    }

    @Override
    public CompletableFuture<Long> estimateRecordsSizeAsync(TupleRange range) {
        return untypedStore.estimateRecordsSizeAsync(range);
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCount(KeyExpression key, Key.Evaluated value,
                                                          IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.getSnapshotRecordCount(key, value, indexQueryabilityFilter);
    }

    @Override
    public CompletableFuture<Long> getSnapshotRecordCountForRecordType(String recordTypeName,
                                                                       IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.getSnapshotRecordCountForRecordType(recordTypeName, indexQueryabilityFilter);
    }

    @Override
    public <T> CompletableFuture<T> evaluateIndexRecordFunction(EvaluationContext evaluationContext, IndexRecordFunction<T> function, FDBRecord<M> rec) {
        return untypedStore.evaluateTypedIndexRecordFunction(evaluationContext, function, rec);
    }

    @Override
    public <T> CompletableFuture<T> evaluateStoreFunction(EvaluationContext evaluationContext, StoreRecordFunction<T> function, FDBRecord<M> rec) {
        return untypedStore.evaluateTypedStoreFunction(evaluationContext, function, rec);
    }

    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(List<String> recordTypeNames,
                                                              IndexAggregateFunction aggregateFunction,
                                                              TupleRange range,
                                                              IsolationLevel isolationLevel,
                                                              IndexQueryabilityFilter indexQueryabilityFilter) {
        return untypedStore.evaluateAggregateFunction(recordTypeNames, aggregateFunction, range, isolationLevel,
                indexQueryabilityFilter);
    }

    @Override
    public RecordQueryPlan planQuery(final RecordQuery query, final ParameterRelationshipGraph parameterRelationshipGraph) {
        return untypedStore.planQuery(query, parameterRelationshipGraph);
    }

    @Override
    public RecordQueryPlan planQuery(RecordQuery query, ParameterRelationshipGraph parameterRelationshipGraph,
                                     RecordQueryPlannerConfiguration plannerConfiguration) {
        return untypedStore.planQuery(query, parameterRelationshipGraph, plannerConfiguration);
    }

    @Override
    public RecordQueryPlan planQuery(RecordQuery query) {
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

        @Override
        public Builder<M> setSerializer(@Nullable RecordSerializer<M> typedSerializer) {
            this.typedSerializer = typedSerializer;
            return this;
        }

        /**
         * Get the serializer that will be used by the underlying record store for non-typed operations such as building indexes.
         * @return untyped serializer
         */
        public RecordSerializer<Message> getUntypedSerializer() {
            return untypedStoreBuilder.getSerializer();
        }

        /**
         * Get the serializer that will be used by the underlying record store for non-typed operations such as building indexes.
         * @param serializer untyped serializer
         * @return this builder
         */
        public Builder<M> setUntypedSerializer(RecordSerializer<Message> serializer) {
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

        @Override
        public Builder<M> setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider) {
            untypedStoreBuilder.setSubspaceProvider(subspaceProvider);
            return this;
        }

        @Override
        public Builder<M> setSubspace(@Nullable Subspace subspace) {
            untypedStoreBuilder.setSubspace(subspace);
            return this;
        }

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

        @Override
        public Builder<M> setUserVersionChecker(@Nullable UserVersionChecker userVersionChecker) {
            untypedStoreBuilder.setUserVersionChecker(userVersionChecker);
            return this;
        }

        @Override
        public IndexMaintainerFactoryRegistry getIndexMaintainerRegistry() {
            return untypedStoreBuilder.getIndexMaintainerRegistry();
        }

        @Override
        public Builder<M> setIndexMaintainerRegistry(IndexMaintainerFactoryRegistry indexMaintainerRegistry) {
            untypedStoreBuilder.setIndexMaintainerRegistry(indexMaintainerRegistry);
            return this;
        }

        @Override
        public IndexMaintenanceFilter getIndexMaintenanceFilter() {
            return untypedStoreBuilder.getIndexMaintenanceFilter();
        }

        @Override
        public Builder<M> setIndexMaintenanceFilter(IndexMaintenanceFilter indexMaintenanceFilter) {
            untypedStoreBuilder.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        @Override
        public PipelineSizer getPipelineSizer() {
            return untypedStoreBuilder.getPipelineSizer();
        }

        @Override
        public Builder<M> setPipelineSizer(PipelineSizer pipelineSizer) {
            untypedStoreBuilder.setPipelineSizer(pipelineSizer);
            return this;
        }

        @Override
        public FDBRecordStoreStateCache getStoreStateCache() {
            return untypedStoreBuilder.getStoreStateCache();
        }

        @Override
        public Builder<M> setStoreStateCache(FDBRecordStoreStateCache storeStateCache) {
            untypedStoreBuilder.setStoreStateCache(storeStateCache);
            return this;
        }

        @Override
        public FDBRecordStore.StateCacheabilityOnOpen getStateCacheabilityOnOpen() {
            return untypedStoreBuilder.getStateCacheabilityOnOpen();
        }

        @Override
        public BaseBuilder<M, FDBTypedRecordStore<M>> setStateCacheabilityOnOpen(final FDBRecordStore.StateCacheabilityOnOpen stateCacheabilityOnOpen) {
            untypedStoreBuilder.setStateCacheabilityOnOpen(stateCacheabilityOnOpen);
            return this;
        }

        @Nullable
        @Override
        public String getBypassFullStoreLockReason() {
            return untypedStoreBuilder.getBypassFullStoreLockReason();
        }

        @Override
        public Builder<M> setBypassFullStoreLockReason(@Nullable final String reason) {
            untypedStoreBuilder.setBypassFullStoreLockReason(reason);
            return this;
        }

        @Override
        public CompletableFuture<FDBTypedRecordStore<M>> uncheckedOpenAsync() {
            return untypedStoreBuilder.uncheckedOpenAsync()
                    .thenApply(untypedStore -> new FDBTypedRecordStore<>(untypedStore, typedSerializer));
        }

        @Override
        public CompletableFuture<FDBTypedRecordStore<M>> createOrOpenAsync(StoreExistenceCheck existenceCheck) {
            return untypedStoreBuilder.createOrOpenAsync(existenceCheck)
                    .thenApply(untypedStore -> new FDBTypedRecordStore<>(untypedStore, typedSerializer));
        }

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
    public static <M extends Message> Builder<M> newBuilder(RecordSerializer<M> serializer) {
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
    public static <M extends Message, U extends Message, B extends Message.Builder> Builder<M> newBuilder(Descriptors.FieldDescriptor fieldDescriptor,
                                                                                                          Supplier<B> builderSupplier,
                                                                                                          Predicate<U> tester,
                                                                                                          Function<U, M> getter,
                                                                                                          BiConsumer<B, M> setter) {
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
    public static <M extends Message, U extends Message, B extends Message.Builder> Builder<M> newBuilder(Descriptors.FileDescriptor fileDescriptor,
                                                                                                          Descriptors.FieldDescriptor fieldDescriptor,
                                                                                                          Supplier<B> builderSupplier,
                                                                                                          Predicate<U> tester,
                                                                                                          Function<U, M> getter,
                                                                                                          BiConsumer<B, M> setter) {
        RecordMetaData metaData = RecordMetaData.build(fileDescriptor);
        return newBuilder(fieldDescriptor, builderSupplier, tester, getter, setter).setMetaDataProvider(metaData);
    }

    public Builder<M> asBuilder() {
        return new Builder<>(this);
    }

}
