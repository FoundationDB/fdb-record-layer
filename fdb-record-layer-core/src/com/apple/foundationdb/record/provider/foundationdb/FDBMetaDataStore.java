/*
 * FDBMetaDataStore.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Serialization of {@link RecordMetaData} into the database.
 *
 * <p>
 * Storing meta-data in the database allows for it to be updated atomically. All clients of the database will see the new version at the same time.
 * If, on the other hand, meta-data is part of the application itself, it is possible that during deployment of a new version of the application some
 * instances will have out-of-date meta-data.
 * </p>
 */
@API(API.Status.MAINTAINED)
public class FDBMetaDataStore extends FDBStoreBase implements RecordMetaDataProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBMetaDataStore.class);

    @Nonnull
    private Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[0];
    @Nullable
    private ExtensionRegistry extensionRegistry;
    @Nullable
    private RecordMetaData recordMetaData;
    @Nullable
    private final MetaDataCache cache;
    @Nullable
    private PendingCacheUpdate pendingCacheUpdate;
    private boolean maintainHistory = true;

    // It is recommended to use {@link #FDBMetaDataStore(FDBRecordContext, KeySpacePath)} instead.
    @API(API.Status.UNSTABLE)
    public FDBMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace,
                            @Nullable MetaDataCache cache) {
        super(context, subspace);
        this.cache = cache;
    }

    public FDBMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
        this(context, new Subspace(path.toTuple()), null);
    }

    // All keys in subspace are taken by SplitHelper.
    // Normally meta-data fits into UNSPLIT_RECORD (0).
    public static final Tuple CURRENT_KEY = Tuple.from((Object)null);
    public static final Tuple HISTORY_KEY_PREFIX = Tuple.from("H");

    // TODO: Previously, meta-data was stored directly in the store's root.
    // This can be removed at some point after existing stores have been updated.
    public static final Tuple OLD_FORMAT_KEY = TupleHelpers.EMPTY;

    /**
     * Set dependencies upon which record descriptors may depend.
     * @param dependencies array of descriptors that record descriptors might depend on
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public void setDependencies(@Nonnull Descriptors.FileDescriptor[] dependencies) {
        this.dependencies = dependencies;
    }

    @Nullable
    protected ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }

    public void setExtensionRegistry(@Nullable ExtensionRegistry extensionRegistry) {
        this.extensionRegistry = extensionRegistry;
    }

    /**
     * Set whether this store keeps a record of older versions.
     * @param maintainHistory {@code true} if this store should maintain a history
     */
    public void setMaintainHistory(boolean maintainHistory) {
        this.maintainHistory = maintainHistory;
    }

    /**
     * Load current meta-data from store and set for <code>getRecordMetaData</code>.
     * @param checkCache {@code true} if the cache should be checked first
     * @param currentVersion the version to load
     * @return a future that completes with the Protobuf form of the meta-data
     */
    public CompletableFuture<RecordMetaDataProto.MetaData> loadAndSetCurrent(boolean checkCache, int currentVersion) {
        final int cachedSerializedVersion;
        if (checkCache && cache != null) {
            byte[] serialized = cache.getCachedSerialized();
            if (serialized != null) {
                RecordMetaDataProto.MetaData metaDataProto = parseMetaDataProto(serialized);
                cachedSerializedVersion = metaDataProto.getVersion();
                if (currentVersion < 0 || currentVersion == cachedSerializedVersion) {
                    recordMetaData = buildMetaData(metaDataProto);
                    addPendingCacheUpdate(recordMetaData);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("Using cached serialized meta-data",
                                subspaceProvider.logKey(), subspaceProvider,
                                LogMessageKeys.VERSION, currentVersion));
                    }
                    return CompletableFuture.completedFuture(metaDataProto);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("Cached serialized meta-data is out-of-date",
                            subspaceProvider.logKey(), subspaceProvider,
                            LogMessageKeys.VERSION, currentVersion,
                            "cachedVersion", cachedSerializedVersion));
                }
            } else {
                cachedSerializedVersion = 0;
            }
        } else {
            cachedSerializedVersion = -1;
        }
        CompletableFuture<RecordMetaDataProto.MetaData> future = loadCurrentSerialized()
                .thenApply(serialized -> {
                    if (serialized == null) {
                        return null;
                    }
                    RecordMetaDataProto.MetaData metaDataProto = parseMetaDataProto(serialized);
                    recordMetaData = buildMetaData(metaDataProto);
                    if (cache != null) {
                        int serializedVersion = recordMetaData.getVersion();
                        if (currentVersion != serializedVersion) {
                            cache.setCurrentVersion(context, serializedVersion);
                        }
                        addPendingCacheUpdate(recordMetaData);
                        if (serializedVersion > cachedSerializedVersion) {
                            addPendingCacheUpdate(serialized);
                        }
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("Loaded meta-data",
                                subspaceProvider.logKey(), subspaceProvider,
                                LogMessageKeys.VERSION, metaDataProto.getVersion()));
                    }
                    return metaDataProto;
                });
        return instrument(FDBStoreTimer.Events.LOAD_META_DATA, future);
    }

    protected CompletableFuture<byte[]> loadCurrentSerialized() {
        return SplitHelper.loadWithSplit(ensureContextActive(), context, getSubspace(), CURRENT_KEY, true, false, null)
                // TODO: Compatibility with old stores that used whole subspace for current meta-data.
                .thenCompose(currentRawRecord -> {
                    if (currentRawRecord != null) {
                        return CompletableFuture.completedFuture(currentRawRecord.getRawRecord());
                    } else {
                        return SplitHelper.loadWithSplit(ensureContextActive(), context, getSubspace(), OLD_FORMAT_KEY, true, false, null)
                                .thenApply(oldRawRecord -> {
                                    if (oldRawRecord != null) {
                                        final byte[] oldBytes = oldRawRecord.getRawRecord();
                                        LOGGER.info(KeyValueLogMessage.of("Upgrading old-format meta-data store",
                                                subspaceProvider.logKey(), subspaceProvider));
                                        ensureContextActive().clear(getSubspace().range(OLD_FORMAT_KEY));
                                        SplitHelper.saveWithSplit(context, getSubspace(), CURRENT_KEY, oldBytes, null);
                                        return oldBytes;
                                    } else {
                                        return (byte[])null;
                                    }
                                });
                    }
                });
    }

    @Nonnull
    protected RecordMetaDataProto.MetaData parseMetaDataProto(@Nonnull byte[] serialized) {
        try {
            return RecordMetaDataProto.MetaData.parseFrom(serialized, getExtensionRegistry());
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("Error parsing meta-data", ex);
        }
    }

    @Nonnull
    protected RecordMetaData buildMetaData(RecordMetaDataProto.MetaData metaDataProto) {
        return new RecordMetaDataBuilder(metaDataProto, dependencies, false).getRecordMetaData();
    }

    public CompletableFuture<RecordMetaData> loadVersion(int version) {
        if (!maintainHistory) {
            throw new RecordCoreException("This store does not maintain a history of older versions");
        }
        return SplitHelper.loadWithSplit(ensureContextActive(), context, getSubspace(), HISTORY_KEY_PREFIX.add(version), true, false, null)
                .thenApply(rawRecord -> (rawRecord == null) ? null : buildMetaData(parseMetaDataProto(rawRecord.getRawRecord())));
    }

    /**
     * Build meta-data to use from Protobuf and save.
     * @param metaDataProto the Protobuf form of the meta-data to save
     * @return a future that completes when the save is done
     */
    public CompletableFuture<Void> saveAndSetCurrent(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        RecordMetaData validatedMetaData = new RecordMetaDataBuilder(metaDataProto, dependencies, false).getRecordMetaData();
        MetaDataValidator validator = new MetaDataValidator(validatedMetaData, IndexMaintainerRegistryImpl.instance());
        validator.validate();

        // Load even if not maintaining history so as to get compatibility upgrade before (over-)writing.
        CompletableFuture<Void> future = loadCurrentSerialized().thenApply(oldSerialized -> {
            if (oldSerialized != null && maintainHistory) {
                RecordMetaDataProto.MetaData oldProto = parseMetaDataProto(oldSerialized);
                int oldVersion = oldProto.getVersion();
                if (metaDataProto.getVersion() <= oldVersion) {
                    LOGGER.warn(KeyValueLogMessage.of("Meta-data version did not increase",
                            subspaceProvider.logKey(), subspaceProvider,
                            "old", oldVersion,
                            "new", metaDataProto.getVersion()));
                    throw new RecordCoreException("meta-data version must increase when history is maintained");
                }
                SplitHelper.saveWithSplit(context, getSubspace(), HISTORY_KEY_PREFIX.add(oldVersion), oldSerialized, null);
            }
            return null;
        });
        future = future.thenApply(vignore -> {
            recordMetaData = validatedMetaData;
            byte[] serialized = metaDataProto.toByteArray();
            SplitHelper.saveWithSplit(context, getSubspace(), CURRENT_KEY, serialized, null);
            if (cache != null) {
                cache.setCurrentVersion(context, recordMetaData.getVersion());
                addPendingCacheUpdate(recordMetaData);
                addPendingCacheUpdate(serialized);
            }
            return null;
        });
        return instrument(FDBStoreTimer.Events.SAVE_META_DATA, future);
    }

    // TODO: FDBMetadataStore should assign index keys based on a counter (https://github.com/FoundationDB/fdb-record-layer/issues/11)
    public CompletableFuture<RecordMetaData> getRecordMetaDataAsync(boolean errorIfMissing) {
        if (recordMetaData != null) {
            return CompletableFuture.completedFuture(recordMetaData);
        }

        final CompletableFuture<Integer> currentVersionFuture = cache == null ? CompletableFuture.completedFuture(-1)
                                                                              : cache.getCurrentVersionAsync(context);

        return instrument(FDBStoreTimer.Events.GET_META_DATA_CACHE_VERSION, currentVersionFuture)
                .thenCompose(currentVersion -> {
                    if (cache != null) {
                        final long startTime = System.nanoTime();
                        recordMetaData = cache.getCachedMetaData();
                        final long endTime = System.nanoTime();
                        if (getTimer() != null) {
                            getTimer().record(FDBStoreTimer.Events.GET_META_DATA_CACHE_ENTRY, endTime - startTime);
                        }
                        if (recordMetaData != null && currentVersion >= 0) {
                            if (currentVersion == recordMetaData.getVersion()) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(KeyValueLogMessage.of("Using cached meta-data",
                                                                   subspaceProvider.logKey(), subspaceProvider,
                                                                   LogMessageKeys.VERSION, currentVersion));
                                }
                                return CompletableFuture.completedFuture(recordMetaData);
                            }
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(KeyValueLogMessage.of("Cached meta-data is out-of-date",
                                                               subspaceProvider.logKey(), subspaceProvider,
                                                               LogMessageKeys.VERSION, currentVersion,
                                                               "cachedVersion", recordMetaData.getVersion()));
                            }
                            recordMetaData = null;
                        }
                    }
                    return loadAndSetCurrent(true, currentVersion).thenApply(ignore -> {
                        if (errorIfMissing && recordMetaData == null) {
                            throw new MissingMetaDataException("Metadata could not be loaded");
                        }
                        return recordMetaData;
                    });
                });
    }

    /**
     * Prepare a meta-data store for use by loading any existing {@link RecordMetaData} or storing an initial
     * seed from the given provider.
     * @param metaDataProvider a provider for a seed meta-data to be used when this store is empty
     * @return a future that is complete when this store is ready for use
     */
    @Nonnull
    public CompletableFuture<Void> preloadMetaData(@Nullable RecordMetaDataProvider metaDataProvider) {
        // Must exist in store if don't have a separate seed meta-data.
        return getRecordMetaDataAsync(metaDataProvider == null)
                .thenCompose(metaData -> {
                    if (metaData == null) {
                        RecordMetaData seedMetaData = metaDataProvider.getRecordMetaData();
                        RecordMetaDataProto.MetaData metaDataProto = seedMetaData.toProto();
                        return saveAndSetCurrent(metaDataProto);
                    } else {
                        return AsyncUtil.DONE;
                    }
                });
    }

    /**
     * Thrown if meta-data was never written to the store.
     */
    @SuppressWarnings("serial")
    public static class MissingMetaDataException extends RecordCoreException {

        public MissingMetaDataException(@Nonnull String msg) {
            super(msg);
        }

    }

    class PendingCacheUpdate implements FDBRecordContext.AfterCommit {
        RecordMetaData metaData;
        byte[] serialized;

        @Override
        public void run() {
            if (metaData != null) {
                cache.setCachedMetaData(metaData);
            }
            if (serialized != null) {
                cache.setCachedSerialized(serialized);
            }
        }
    }

    private PendingCacheUpdate pendingCacheUpdate() {
        if (pendingCacheUpdate == null) {
            pendingCacheUpdate = new PendingCacheUpdate();

        }
        return pendingCacheUpdate;
    }

    private void addPendingCacheUpdate(@Nonnull RecordMetaData metaData) {
        pendingCacheUpdate().metaData = metaData;
    }

    private void addPendingCacheUpdate(@Nonnull byte[] serialized) {
        pendingCacheUpdate().serialized = serialized;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_META_DATA, getRecordMetaDataAsync(true));
    }

    public void saveRecordMetaData(@Nonnull RecordMetaDataProvider metaDataProvider) {
        saveRecordMetaData(metaDataProvider.getRecordMetaData().toProto());
    }

    public void saveRecordMetaData(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_META_DATA, saveAndSetCurrent(metaDataProto));
    }

    @Nullable
    @VisibleForTesting
    public MetaDataCache getCache() {
        return cache;
    }

}
