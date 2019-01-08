/*
 * FDBRecordStoreBuilder.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for builders for {@link FDBRecordStoreBase}.
 *
 * Methods for getting a record store from the builder:
 * <ul>
 * <li>{@link #createOrOpen}: open an existing record store or create it the first time.</li>
 * <li>{@link #open}: open an existing record store or throw an exception if it has never been created.</li>
 * <li>{@link #create}: create a record store or throw an exception if it has already been created.</li>
 * <li>{@link #build}: return a record store without checking its state in the database. One should almost always
 * call {@link FDBRecordStoreBase#checkVersion} before actually using the record store.</li>
 * </ul>
 *
 * @param <M> type used to represent stored records
 * @param <R> type of record store that will be built
 * @see FDBRecordStore.Builder
 * @see FDBTypedRecordStore.Builder
 */
@API(API.Status.STABLE)
public abstract class FDBRecordStoreBuilder<M extends Message, R extends FDBRecordStoreBase<M>> {
    @Nullable
    protected RecordSerializer<M> serializer;

    protected int formatVersion = FDBRecordStoreBase.DEFAULT_FORMAT_VERSION;

    @Nullable
    protected RecordMetaDataProvider metaDataProvider;

    @Nullable
    protected FDBMetaDataStore metaDataStore;

    @Nullable
    protected FDBRecordContext context;

    @Nullable
    protected SubspaceProvider subspaceProvider;

    @Nullable
    protected FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    
    @Nonnull
    protected IndexMaintainerRegistry indexMaintainerRegistry = IndexMaintainerRegistryImpl.instance();

    @Nonnull
    protected IndexMaintenanceFilter indexMaintenanceFilter = IndexMaintenanceFilter.NORMAL;

    @Nonnull
    protected FDBRecordStoreBase.PipelineSizer pipelineSizer = FDBRecordStoreBase.DEFAULT_PIPELINE_SIZER;

    protected FDBRecordStoreBuilder() {
    }

    public FDBRecordStoreBuilder(@Nonnull FDBRecordStoreBuilder<M, R> other) {
        copyFrom(other);
    }

    protected FDBRecordStoreBuilder(@Nonnull R store) {
        copyFrom(store);
    }

    public void copyFrom(@Nonnull FDBRecordStoreBuilder<M, R> other) {
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
    }

    public void copyFrom(@Nonnull R store) {
        this.serializer = store.serializer;
        this.formatVersion = store.formatVersion;
        this.metaDataProvider = store.metaDataProvider;
        this.context = store.context;
        this.subspaceProvider = store.subspaceProvider;
        this.indexMaintainerRegistry = store.indexMaintainerRegistry;
        this.indexMaintenanceFilter = store.indexMaintenanceFilter;
        this.pipelineSizer = store.pipelineSizer;
    }

    @Nonnull
    public RecordSerializer<M> getSerializer() {
        return serializer;
    }

    /**
     * Set the serializer used to convert records into byte arrays.
     * @param serializer the serializer to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setSerializer(@Nonnull RecordSerializer<M> serializer) {
        this.serializer = serializer;
        return this;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

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
    public FDBRecordStoreBuilder<M, R> setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
        return this;
    }

    @Nullable
    public RecordMetaDataProvider getMetaDataProvider() {
        return metaDataProvider;
    }

    /**
     * Set the provider for the record store's meta-data.
     * If {@link #setMetaDataStore} is also called, the provider will only be used to initialize the meta-data store when it is empty. The record store will be built using the store as its provider.
     * @param metaDataProvider the meta-data source to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
        return this;
    }

    @Nullable
    public FDBMetaDataStore getMetaDataStore() {
        return metaDataStore;
    }

    /**
     * Set the {@link FDBMetaDataStore} to use as the source of meta-data.
     * If {@link #setMetaDataProvider} is also called, it will be used to seed the store.
     * @param metaDataStore the meta-data store to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore) {
        this.metaDataStore = metaDataStore;
        return this;
    }

    @Nullable
    public FDBRecordContext getContext() {
        return context;
    }

    /**
     * Set the record context (transaction) to use for the record store.
     * @param context the record context / transaction to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setContext(@Nullable FDBRecordContext context) {
        this.context = context;
        return this;
    }

    /**
     * Set the subspace to use for the record store.
     * The record store is allowed to use the entire subspace, so it should not overlap any other record store's subspace.
     * It is preferred to use SubspaceProvider with KeySpacePath rather than this because key
     * space path provides more meaningful logs.
     * @param subspace the subspace to use
     * @return this builder
     */
    @Nonnull
    @API(API.Status.UNSTABLE)
    public FDBRecordStoreBuilder<M, R> setSubspace(@Nullable Subspace subspace) {
        this.subspaceProvider = subspace == null ? null : new SubspaceProviderBySubspace(subspace);
        return this;
    }

    /**
     * Set the key space path to use for the record store.
     * The record store is allowed to use the entire subspace, so it should not overlap any other record store's subspace.
     * Note: The context should be set before setting the key space path.
     * @param keySpacePath the key space path to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setKeySpacePath(@Nullable KeySpacePath keySpacePath) {
        if (context == null) {
            throw new RecordCoreException("The context should be set before setting the key space path.");
        }
        this.subspaceProvider = keySpacePath == null ? null : new SubspaceProviderByKeySpacePath(keySpacePath, context);
        return this;
    }

    /**
     * Set the subspace provider from a subspace provider.
     * @param subspaceProvider the subspace provider
     * @return this builder
     */
    public FDBRecordStoreBuilder<M, R> setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider) {
        this.subspaceProvider = subspaceProvider;
        return this;
    }

    @Nullable
    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return userVersionChecker;
    }

    /**
     * Set the {@link FDBRecordStoreBase.UserVersionChecker function} to be used to check the meta-data version of the record store.
     * @param userVersionChecker the checker function to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setUserVersionChecker(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
        this.userVersionChecker = userVersionChecker;
        return this;
    }

    @Nonnull
    public IndexMaintainerRegistry getIndexMaintainerRegistry() {
        return indexMaintainerRegistry;
    }

    /**
     * Set the registry of index maintainers to be used by the record store.
     * @param indexMaintainerRegistry the index registry to use
     * @return this builder
     * @see FDBRecordStoreBase#getIndexMaintainer
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry) {
        this.indexMaintainerRegistry = indexMaintainerRegistry;
        return this;
    }

    @Nonnull
    public IndexMaintenanceFilter getIndexMaintenanceFilter() {
        return indexMaintenanceFilter;
    }

    /**
     * Set the {@link IndexMaintenanceFilter index filter} to be used by the record store.
     * @param indexMaintenanceFilter the index filter to use
     * @return this builder
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
        this.indexMaintenanceFilter = indexMaintenanceFilter;
        return this;
    }

    @Nonnull
    public FDBRecordStoreBase.PipelineSizer getPipelineSizer() {
        return pipelineSizer;
    }

    /**
     * Set the {@link FDBRecordStoreBase.PipelineSizer object} to be used to determine the depth of pipelines run by the record store.
     * @param pipelineSizer the sizer to use
     * @return this builder
     * @see FDBRecordStoreBase#getPipelineSize
     */
    @Nonnull
    public FDBRecordStoreBuilder<M, R> setPipelineSizer(@Nonnull FDBRecordStoreBase.PipelineSizer pipelineSizer) {
        this.pipelineSizer = pipelineSizer;
        return this;
    }

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
    public abstract FDBRecordStoreBuilder<M, R> copyBuilder();

    @Nonnull
    public abstract R build();

    @Nonnull
    protected RecordMetaDataProvider getMetaDataProviderForBuild() {
        if (metaDataStore != null) {
            return metaDataStore;
        } else if (metaDataProvider != null) {
            return metaDataProvider;
        } else {
            throw new RecordCoreException("Neither metaDataStore nor metaDataProvider was set in builder.");
        }
    }

    @Nonnull
    public R uncheckedOpen() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD_STORE_STATE, uncheckedOpenAsync());
    }

    /**
     * Opens a <code>FDBRecordStore</code> instance without calling
     * {@link FDBRecordStoreBase#checkVersion}.
     * @return a future that will contain a store with the appropriate parameters set when ready
     */
    @Nonnull
    public CompletableFuture<R> uncheckedOpenAsync() {
        final CompletableFuture<Void> preloadMetaData = preloadMetaData();
        R recordStore = build();
        final CompletableFuture<Void> subspaceFuture = recordStore.preloadSubspaceAsync();
        final CompletableFuture<Void> loadStoreState = subspaceFuture.thenCompose(vignore -> recordStore.preloadRecordStoreStateAsync());
        return CompletableFuture.allOf(preloadMetaData, loadStoreState).thenApply(vignore -> recordStore);
    }

    private CompletableFuture<Void> preloadMetaData() {
        if (metaDataStore != null) {
            return metaDataStore.preloadMetaData(metaDataProvider);
        } else {
            return AsyncUtil.DONE;
        }
    }

    /**
     * Synchronous version of {@link #createAsync}.
     * @return a newly created record store
     */
    @Nonnull
    public R create() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createAsync());
    }

    /**
     * Synchronous version of {@link #openAsync}.
     * @return an open record store
     */
    @Nonnull
    public R open() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, openAsync());
    }

    /**
     * Synchronous version of {@link #createOrOpenAsync()}.
     * @return an open record store
     */
    @Nonnull
    public R createOrOpen() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createOrOpenAsync());
    }

    /**
     * Synchronous version of {@link #createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck)}.
     * @param existenceCheck whether the store must already exist
     * @return an open record store
     */
    @Nonnull
    public R createOrOpen(@Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION, createOrOpenAsync(existenceCheck));
    }

    /**
     * Opens a new <code>FDBRecordStore</code> instance in the given path with the given meta-data.
     * The store must not have already been written to the specified subspace.
     * @return a future that will contain a store with the appropriate parameters set when ready
     */
    @Nonnull
    public CompletableFuture<R> createAsync() {
        return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS);
    }

    /**
     * Opens an existing <code>FDBRecordStore</code> instance in the given path with the given meta-data.
     * The store must have already been written to the specified subspace.
     * @return a future that will contain a store with the appropriate parameters set when ready
     */
    @Nonnull
    public CompletableFuture<R> openAsync() {
        return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS);
    }

    /**
     * Opens a <code>FDBRecordStore</code> instance in the given path with the given meta-data.
     * @return a future that will contain a store with the appropriate parameters set when ready
     */
    @Nonnull
    public CompletableFuture<R> createOrOpenAsync() {
        return createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);
    }

    /**
     * Opens a <code>FDBRecordStore</code> instance in the given path with the given meta-data.
     * @param existenceCheck whether the store must already exist
     * @return a future that will contain a store with the appropriate parameters set when ready
     */
    @Nonnull
    public CompletableFuture<R> createOrOpenAsync(@Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        // Might be as many as four reads: meta-data store, keyspace path, store index state, store info header.
        // Try to do them as much in parallel as possible.
        final CompletableFuture<Void> preloadMetaData = preloadMetaData();
        R recordStore = build();
        final CompletableFuture<Void> subspaceFuture = recordStore.preloadSubspaceAsync();
        final CompletableFuture<Void> loadStoreState = subspaceFuture.thenCompose(vignore -> recordStore.preloadRecordStoreStateAsync());
        final CompletableFuture<KeyValue> loadStoreInfo = subspaceFuture.thenCompose(vignore -> recordStore.readStoreFirstKey());
        final CompletableFuture<KeyValue> combinedFuture = CompletableFuture.allOf(preloadMetaData, loadStoreState).thenCombine(loadStoreInfo, (v, kv) -> kv);
        final CompletableFuture<Boolean> checkVersion = recordStore.checkVersion(combinedFuture, userVersionChecker, existenceCheck);
        return checkVersion.thenApply(vignore -> recordStore);
    }

}
