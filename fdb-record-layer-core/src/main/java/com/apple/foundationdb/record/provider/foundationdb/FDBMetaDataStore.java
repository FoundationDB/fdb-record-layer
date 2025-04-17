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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Serialization of {@link RecordMetaData} into the database.
 *
 * <p>
 * Storing meta-data in the database allows for it to be updated atomically. All clients of the database will see the new version at the same time.
 * If, on the other hand, meta-data is part of the application itself, it is possible that during deployment of a new version of the application some
 * instances will have out-of-date meta-data.
 * </p>
 */
@API(API.Status.UNSTABLE)
public class FDBMetaDataStore extends FDBStoreBase implements RecordMetaDataProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBMetaDataStore.class);
    private static final ExtensionRegistry DEFAULT_EXTENSION_REGISTRY;

    static {
        ExtensionRegistry defaultExtensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(defaultExtensionRegistry);
        DEFAULT_EXTENSION_REGISTRY = defaultExtensionRegistry.getUnmodifiable();
    }

    // All keys in subspace are taken by SplitHelper.
    // Normally meta-data fits into UNSPLIT_RECORD (0).
    public static final Tuple CURRENT_KEY = Tuple.from((Object)null);
    public static final Tuple HISTORY_KEY_PREFIX = Tuple.from("H");

    // TODO: Previously, meta-data was stored directly in the store's root.
    //  This can be removed at some point after existing stores have been updated.
    public static final Tuple OLD_FORMAT_KEY = TupleHelpers.EMPTY;

    @Nonnull
    private Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[0];
    @Nullable
    private Descriptors.FileDescriptor localFileDescriptor;
    @Nullable
    private ExtensionRegistry extensionRegistry = DEFAULT_EXTENSION_REGISTRY;
    @Nonnull
    private MetaDataEvolutionValidator evolutionValidator = MetaDataEvolutionValidator.getDefaultInstance();
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
        this(context, new Subspace(path.toTuple(context)), null);
    }

    /**
     * Set dependencies upon which record descriptors may depend.
     * @param dependencies array of descriptors that record descriptors might depend on
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public void setDependencies(@Nonnull Descriptors.FileDescriptor[] dependencies) {
        this.dependencies = dependencies;
    }

    /**
     * Get the local meta-data file descriptor.
     * See {@link RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor)} for more information.
     * @return the local meta-data file descriptor
     * @see RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor)
     */
    @Nullable
    public Descriptors.FileDescriptor getLocalFileDescriptor() {
        return localFileDescriptor;
    }

    /**
     * Set the local meta-data file descriptor.
     *
     * <p>
     * A record store created off of the meta-data store may not be able to store a record created by a
     * statically-generated proto file because the meta-data and record have mismatched descriptors. Using this method,
     * the meta-data can use the same version of the descriptor as the record.
     * See {@link RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor)} for more information.
     * </p>
     *
     * <p>
     * Note that it is not allowed to (a) change the name of the existing record types or (b) change existing
     * non-{@code NESTED} record types to {@code NESTED} record types in the evolved local file descriptor.
     * </p>
     *
     * @param localFileDescriptor the local descriptor of the meta-data
     * @see RecordMetaDataBuilder#setLocalFileDescriptor(Descriptors.FileDescriptor)
     */
    public void setLocalFileDescriptor(@Nullable Descriptors.FileDescriptor localFileDescriptor) {
        this.localFileDescriptor = localFileDescriptor;
    }

    /**
     * Get the validator used when saving a new version of the meta-data to ensure the new meta-data is a valid
     * evolution of the old meta-data. By default, this is the the {@link MetaDataEvolutionValidator}'s
     * {@linkplain MetaDataEvolutionValidator#getDefaultInstance() default instance}, but the user can tweak
     * the validator's options by supplying a different validator through {@link #setEvolutionValidator(MetaDataEvolutionValidator)}.
     * See {@link #setEvolutionValidator(MetaDataEvolutionValidator)} for more details.
     *
     * @return the validator used to ensure the new meta-data is a valid evolution of the existing meta-data
     * @see #setEvolutionValidator(MetaDataEvolutionValidator)
     * @see MetaDataEvolutionValidator
     */
    @Nonnull
    public MetaDataEvolutionValidator getEvolutionValidator() {
        return evolutionValidator;
    }

    /**
     * Set the validator used when saving a new version of the meta-data to ensure the new meta-data is a valid
     * evolution of the old meta-data. See {@link #getEvolutionValidator()} for more details.
     *
     * <p>
     * Whenever the meta-data store, saves a new version of the meta-data (through, for example, {@link #saveRecordMetaData(RecordMetaDataProvider)}),
     * the existing meta-data from the store is read. The new and existing meta-data are then compared, and an error
     * is thrown if the new meta-data is not a valid evolution of the existing meta-data. See {@link MetaDataEvolutionValidator}
     * for more details on how the meta-data should be evolved.
     * </p>
     *
     * <p>
     * If one sets a {@linkplain #setLocalFileDescriptor(Descriptors.FileDescriptor) local file descriptor}, then this
     * validator is also used to make sure that the local file descriptor is compatible with the file descriptor within
     * the meta-data proto retrieved from the database.
     * </p>
     *
     * @param evolutionValidator the validator used to ensure the new meta-data is a valid evolution of the existing meta-data
     * @see MetaDataEvolutionValidator
     */
    public void setEvolutionValidator(@Nonnull MetaDataEvolutionValidator evolutionValidator) {
        this.evolutionValidator = evolutionValidator;
    }

    /**
     * Get the extension registry used when deserializing meta-data proto messages. By default, the extension registry
     * will have only the extensions specified in "{@code record_metadata_options.proto}". Note that this
     * differs from the {@linkplain ExtensionRegistry#getEmptyRegistry() empty extension registry}.
     *
     * <p>
     * When the meta-data proto is built into a {@link RecordMetaData} object, extension options on fields
     * specifying primary key and index information are ignored. However, options specifying each record's
     * type are still processed, including information specifying the meta-data's union descriptor. As a result,
     * if the provided registry does not at least register the {@code (record).usage} extension option, the
     * {@code RecordMetaData} may fail to build with an error message indicating that no union descriptor was specified.
     * </p>
     *
     * @return the extension registry used when parsing meta-data
     */
    @Nullable
    protected ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }

    /**
     * Set the extension registry used when deserializing meta-data proto messages. Note that if using Protobuf
     * version 2, a {@code null} extension registry serves as a synonym for the
     * {@linkplain ExtensionRegistry#getEmptyRegistry() empty extension registry}, but in Protobuf version 3,
     * a {@code null} extension registry can result in {@link NullPointerException}s when the meta-data proto is
     * deserialized. Therefore, users who upgrade from proto2 to proto3 may need to adjust the registry they pass
     * to the meta-data store through this function.
     *
     * @param extensionRegistry the extension registry used when parsing meta-data
     */
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
                    recordMetaData = buildMetaData(metaDataProto, false);
                    addPendingCacheUpdate(recordMetaData);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("Using cached serialized meta-data",
                                subspaceProvider.logKey(), subspaceProvider.toString(context),
                                LogMessageKeys.VERSION, currentVersion));
                    }
                    return CompletableFuture.completedFuture(metaDataProto);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("Cached serialized meta-data is out-of-date",
                                    subspaceProvider.logKey(), subspaceProvider.toString(context),
                                    LogMessageKeys.VERSION, currentVersion,
                                    LogMessageKeys.CACHED_VERSION, cachedSerializedVersion));
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
                    recordMetaData = buildMetaData(metaDataProto, false);
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
                                subspaceProvider.logKey(), subspaceProvider.toString(context),
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
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info(KeyValueLogMessage.of("Upgrading old-format meta-data store",
                                                    subspaceProvider.logKey(), subspaceProvider.toString(context)));
                                        }
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
    protected RecordMetaDataBuilder createMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        return createMetaDataBuilder(metaDataProto, true);
    }

    @Nonnull
    protected RecordMetaDataBuilder createMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto, boolean useLocalFileDescriptor) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .addDependencies(dependencies)
                .setEvolutionValidator(evolutionValidator);
        if (useLocalFileDescriptor && localFileDescriptor != null) {
            builder.setLocalFileDescriptor(localFileDescriptor);
        }
        return builder.setRecords(metaDataProto);
    }

    @Nonnull
    protected RecordMetaData buildMetaData(@Nonnull RecordMetaDataProto.MetaData metaDataProto, boolean validate, boolean useLocalFileDescriptor) {
        return createMetaDataBuilder(metaDataProto, useLocalFileDescriptor).build(validate);
    }

    @Nonnull
    protected RecordMetaData buildMetaData(@Nonnull RecordMetaDataProto.MetaData metaDataProto, boolean validate) {
        return buildMetaData(metaDataProto, validate, true);
    }

    public CompletableFuture<RecordMetaData> loadVersion(int version) {
        if (!maintainHistory) {
            throw new RecordCoreException("This store does not maintain a history of older versions");
        }
        return SplitHelper.loadWithSplit(ensureContextActive(), context, getSubspace(), HISTORY_KEY_PREFIX.add(version), true, false, null)
                .thenApply(rawRecord -> (rawRecord == null) ? null : buildMetaData(parseMetaDataProto(rawRecord.getRawRecord()), false));
    }

    /**
     * Build meta-data to use from Protobuf and save.
     * @param metaDataProto the Protobuf form of the meta-data to save
     * @return a future that completes when the save is done
     */
    @Nonnull
    public CompletableFuture<Void> saveAndSetCurrent(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        RecordMetaData validatedMetaData = buildMetaData(metaDataProto, true);

        // Load even if not maintaining history so as to get compatibility upgrade before (over-)writing.
        CompletableFuture<Void> future = loadCurrentSerialized().thenApply(oldSerialized -> {
            if (oldSerialized != null) {
                RecordMetaDataProto.MetaData oldProto = parseMetaDataProto(oldSerialized);
                int oldVersion = oldProto.getVersion();
                if (metaDataProto.getVersion() <= oldVersion) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.of("Meta-data version did not increase",
                                        subspaceProvider.logKey(), subspaceProvider.toString(context),
                                        LogMessageKeys.OLD, oldVersion,
                                        LogMessageKeys.NEW, metaDataProto.getVersion()));
                    }
                    throw new MetaDataException("meta-data version must increase");
                }
                // Build the meta-data, but don't use the local file descriptor as this should validate the original descriptors
                // against each other.
                RecordMetaData oldMetaData = buildMetaData(oldProto, true, false);
                RecordMetaData newMetaData = buildMetaData(metaDataProto, true, false);
                evolutionValidator.validate(oldMetaData, newMetaData);
                if (maintainHistory) {
                    SplitHelper.saveWithSplit(context, getSubspace(), HISTORY_KEY_PREFIX.add(oldVersion), oldSerialized, null);
                }
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
                                                                   subspaceProvider.logKey(), subspaceProvider.toString(context),
                                                                   LogMessageKeys.VERSION, currentVersion));
                                }
                                return CompletableFuture.completedFuture(recordMetaData);
                            }
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(KeyValueLogMessage.of("Cached meta-data is out-of-date",
                                                subspaceProvider.logKey(), subspaceProvider.toString(context),
                                                LogMessageKeys.VERSION, currentVersion,
                                                LogMessageKeys.CACHED_VERSION, recordMetaData.getVersion()));
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

    /**
     * Save the record meta-data into the meta-data store.
     *
     * <p>
     * If the given records descriptor is missing a union message, this method will automatically add one to the descriptor
     * before saving the meta-data. If the meta-data store is currently empty, a default union descriptor will be added to
     * the meta-data based on the non-{@code NESTED} record types. Otherwise, it will update the records descriptor of
     * the currently stored meta-data (see {@link #updateRecords(Descriptors.FileDescriptor)}). The new union descriptor
     * will include any type in existing union and any new record type in the new file descriptor. This method will
     * process extension options.
     * </p>
     *
     * @param fileDescriptor the file descriptor of the record meta-data
     * @see MetaDataProtoEditor#addDefaultUnionIfMissing(Descriptors.FileDescriptor)
     */
    @API(API.Status.UNSTABLE)
    public void saveRecordMetaData(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_META_DATA, saveRecordMetaDataAsync(fileDescriptor));
    }

    /**
     * Save the record meta-data into the meta-data store.
     *
     * @param metaDataProvider the meta-data provider
     */
    public void saveRecordMetaData(@Nonnull RecordMetaDataProvider metaDataProvider) {
        saveRecordMetaData(metaDataProvider.getRecordMetaData().toProto());
    }

    /**
     * Save the record meta-data into the meta-data store.
     *
     * @param metaDataProto the serialized record meta-data
     */
    public void saveRecordMetaData(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_META_DATA, saveAndSetCurrent(metaDataProto));
    }

    /**
     * Save the record meta-data into the meta-data store.
     *
     * <p>
     * If the given records descriptor is missing a union message, this method will automatically add one to the descriptor
     * before saving the meta-data. If the meta-data store is currently empty, a default union descriptor will be added to
     * the meta-data based on the non-{@code NESTED} record types. Otherwise, it will update the records descriptor of
     * the currently stored meta-data (see {@link #updateRecords(Descriptors.FileDescriptor)}). The new union descriptor
     * will include any type in existing union and any new record type in the new file descriptor. This method will
     * process extension options. Also the records descriptor of the meta-data will change while generating the union.
     * </p>
     *
     * @param fileDescriptor the file descriptor of the record meta-data
     * @return a future when save is completed
     * @see MetaDataProtoEditor#addDefaultUnionIfMissing(Descriptors.FileDescriptor)
     */
    @API(API.Status.UNSTABLE)
    public CompletableFuture<Void> saveRecordMetaDataAsync(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        return getRecordMetaDataAsync(false).thenCompose(metaData -> {
            if (metaData == null ) {
                return saveAndSetCurrent(RecordMetaData.build(MetaDataProtoEditor.addDefaultUnionIfMissing(fileDescriptor)).toProto());
            } else {
                return updateRecordsAsync(fileDescriptor);
            }
        });
    }


    @Nonnull
    private CompletableFuture<RecordMetaDataProto.MetaData> loadCurrentProto() {
        return loadCurrentSerialized().thenApply(serialized -> {
            if (serialized == null) {
                return null;
            }
            return parseMetaDataProto(serialized);
        });
    }

    /**
     * Add a new index to the record meta-data.
     * @param recordType the name of the record type
     * @param indexName the name of the new index
     * @param fieldName the field to be indexed
     */
    public void addIndex(@Nonnull String recordType, @Nonnull String indexName, @Nonnull String fieldName) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, addIndexAsync(recordType, indexName, fieldName));
    }

    /**
     * Add a new index to the record meta-data.
     * @param recordType the name of the record type
     * @param indexName the name of the new index
     * @param indexExpression the root expression of the index
     */
    public void addIndex(@Nonnull String recordType, @Nonnull String indexName, @Nonnull KeyExpression indexExpression) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, addIndexAsync(recordType, indexName, indexExpression));
    }

    /**
     * Add a new index to the record meta-data.
     * @param recordType the name of the record type
     * @param index the new index to be added
     */
    public void addIndex(@Nonnull String recordType, @Nonnull Index index) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, addIndexAsync(recordType, index));
    }

    /**
     * Add a new index to the record meta-data asynchronously.
     * @param recordType the name of the record type
     * @param indexName the name of the new index
     * @param fieldName the field to be indexed
     * @return a future that completes when the index is added
     */
    @Nonnull
    public CompletableFuture<Void> addIndexAsync(@Nonnull String recordType, @Nonnull String indexName, @Nonnull String fieldName) {
        return addIndexAsync(recordType, new Index(indexName, fieldName));
    }

    /**
     * Add a new index to the record meta-data asynchronously.
     * @param recordType the name of the record type
     * @param indexName the name of the new index
     * @param indexExpression the root expression of the index
     * @return a future that completes when the index is added
     */
    @Nonnull
    public CompletableFuture<Void> addIndexAsync(@Nonnull String recordType, @Nonnull String indexName,
                                                 @Nonnull KeyExpression indexExpression) {
        return addIndexAsync(recordType, new Index(indexName, indexExpression));
    }

    /**
     * Add a new index to the record meta-data asynchronously.
     * @param recordType the name of the record type
     * @param index the index to be added
     * @return a future that completes when the index is added
     */
    @Nonnull
    public CompletableFuture<Void> addIndexAsync(@Nonnull String recordType, @Nonnull Index index) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            recordMetaDataBuilder.addIndex(recordType, index);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Add a new index to the record meta-data that contains multiple record types.
     * If the list is null or empty, the resulting index will include all record types.
     * If the list has one element it will just be a normal single record type index.
     * @param recordTypes a list of record types that the index will include
     * @param index the index to be added
     */
    public void addMultiTypeIndex(@Nullable List<String> recordTypes, @Nonnull Index index) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, addMultiTypeIndexAsync(recordTypes, index));
    }

    /**
     * Add a new index to the record meta-data that contains multiple record types asynchronously.
     * If the list is null or empty, the resulting index will include all record types.
     * If the list has one element it will just be a normal single record type index.
     * @param recordTypes a list of record types that the index will include
     * @param index the index to be added
     * @return a future that completes when the index is added
     */
    @Nonnull
    public CompletableFuture<Void> addMultiTypeIndexAsync(@Nullable List<String> recordTypes, @Nonnull Index index) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            List<RecordTypeBuilder> recordTypeBuilders = new ArrayList<>();
            if (recordTypes != null) {
                for (String type : recordTypes) {
                    recordTypeBuilders.add(recordMetaDataBuilder.getRecordType(type));
                }
            }
            recordMetaDataBuilder.addMultiTypeIndex(recordTypeBuilders, index);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Add a new index on all record types.
     * @param index the index to be added
     */
    public void addUniversalIndex(@Nonnull Index index) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, addUniversalIndexAsync(index));
    }

    /**
     * Add a new index on all record types.
     * @param index the index to be added
     * @return a future that completes when the index is added
     */
    @Nonnull
    public CompletableFuture<Void> addUniversalIndexAsync(@Nonnull Index index) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            recordMetaDataBuilder.addUniversalIndex(index);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Remove the given index from the record meta-data.
     * @param indexName the name of the index to be removed
     */
    public void dropIndex(@Nonnull String indexName) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_DROP_INDEX, dropIndexAsync(indexName));
    }

    /**
     * Remove the given index from the record meta-data asynchronously.
     * @param indexName the name of the index to be removed
     * @return a future that is complete when the index is dropped
     */
    @Nonnull
    public CompletableFuture<Void> dropIndexAsync(@Nonnull String indexName) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            recordMetaDataBuilder.removeIndex(indexName);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Update the meta-data records descriptor.
     * This adds any new record types to the meta-data and replaces all of the current descriptors with the new descriptors.
     *
     * <p>
     *  It is important to note that if a local file descriptor is set using {@link #setLocalFileDescriptor(Descriptors.FileDescriptor)},
     *  the local file descriptor must be at least as evolved as the records descriptor passed to this method.
     *  Otherwise, {@code updateRecords} will fail.
     * </p>
     *
     * <p>
     * If the given file descriptor is missing a union message, this method will add one before updating the meta-data.
     * The generated union descriptor is constructed by adding any non-{@code NESTED} types in the file descriptor to the
     * union descriptor from the currently stored meta-data. A new field is not added if a field of the given type already
     * exists, and the order of any existing fields is preserved. Note that types are identified by name, so renaming
     * top-level message types may result in validation errors when trying to update the record descriptor. Also the records
     * descriptor of the meta-data will change while generating the union.
     * </p>
     *
     * @param recordsDescriptor the new recordsDescriptor
     */
    @API(API.Status.UNSTABLE)
    public void updateRecords(@Nonnull Descriptors.FileDescriptor recordsDescriptor) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_UPDATE_RECORDS_DESCRIPTOR, updateRecordsAsync(recordsDescriptor));
    }

    /**
     * Update the meta-data records descriptor asynchronously.
     * This adds any new record types to the meta-data and replaces all of the current descriptors with the new descriptors.
     *
     * <p>
     *  It is important to note that if a local file descriptor is set using {@link #setLocalFileDescriptor(Descriptors.FileDescriptor)},
     *  the local file descriptor must be at least as evolved as the records descriptor passed to this method.
     *  Otherwise, {@code updateRecordsAsync} will fail.
     * </p>
     *
     * <p>
     * If the given file descriptor is missing a union message, this method will add one before updating the meta-data.
     * The generated union descriptor is constructed by adding any non-{@code NESTED} types in the file descriptor to the
     * union descriptor from the currently stored meta-data. A new field is not added if a field of the given type already
     * exists, and the order of any existing fields is preserved. Note that types are identified by name, so renaming
     * top-level message types may result in validation errors when trying to update the record descriptor.
     * </p>
     *
     * @param recordsDescriptor the new recordsDescriptor
     * @return a future that completes when the records descriptor is updated
     */
    @Nonnull
    @API(API.Status.UNSTABLE)
    public CompletableFuture<Void> updateRecordsAsync(@Nonnull Descriptors.FileDescriptor recordsDescriptor) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            // Update the records without using its local file descriptor. Let saveAndSetCurrent use the local file descriptor when saving the meta-data.
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto, false);

            // If union is missing, first add a default union.
            recordMetaDataBuilder.updateRecords(MetaDataProtoEditor.addDefaultUnionIfMissing(recordsDescriptor, recordMetaDataBuilder.getUnionDescriptor()));
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Mutate the stored meta-data proto using a mutation callback.
     *
     * <p>
     * This method applies the given mutation callback on the stored meta-data and then saves it back to the store.
     * Callers might need to provide an appropriate {@link MetaDataEvolutionValidator} to the meta-data store, depending on the
     * changes that the callback performs.
     * </p>
     *
     * <p>
     * Also, callers must be cautious about modifying the stored records descriptor using mutation callbacks, as this will make the
     * meta-data store the ultimate (and sole) source of truth for the record definitions and not just the in-database
     * copy of a {@code .proto} file under source control.
     * </p>
     *
     * @param mutateMetaDataProto a callback that mutates the meta-data proto
     */
    public void mutateMetaData(@Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> mutateMetaDataProto) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_MUTATE_METADATA, mutateMetaDataAsync(mutateMetaDataProto));
    }

    /**
     * Mutate the stored meta-data proto and record meta-data builder using mutation callbacks.
     *
     * <p>
     * This method applies the given mutation callbacks on the stored meta-data and then saves it back to the store. Note the
     * order that the callbacks are executed. {@code mutateMetaDataProto} is called first on the meta-data proto and then the
     * second {@code mutateRecordMetaDataBuilder} is executed on the meta-data builder.
     * Callers might need to provide an appropriate {@link MetaDataEvolutionValidator} to the meta-data store, depending on the
     * changes that the callbacks perform.
     * </p>
     *
     * <p>
     * Also, callers must be cautious about modifying the stored records descriptor using mutation callbacks, as this will make the
     * meta-data store the ultimate (and sole) source of truth for the record definitions and not just the in-database
     * copy of a {@code .proto} file under source control.
     * </p>
     *
     * @param mutateMetaDataProto a callback that mutates the meta-data proto
     * @param mutateRecordMetaDataBuilder a callback that mutates the record meta-data builder after the meta-data proto is mutated
     */
    public void mutateMetaData(@Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> mutateMetaDataProto,
                               @Nullable Consumer<RecordMetaDataBuilder> mutateRecordMetaDataBuilder) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_MUTATE_METADATA, mutateMetaDataAsync(mutateMetaDataProto, mutateRecordMetaDataBuilder));
    }

    /**
     * Mutate the stored meta-data proto using a mutation callback asynchronously.
     *
     * <p>
     * This method applies the given mutation callback on the stored meta-data and then saves it back to the store.
     * Callers might need to provide an appropriate {@link MetaDataEvolutionValidator} to the meta-data store, depending on the
     * changes that the callback performs.
     * </p>
     *
     * <p>
     * Also, callers must be cautious about modifying the stored records descriptor using mutation callbacks, as this will make the
     * meta-data store the ultimate (and sole) source of truth for the record definitions and not just the in-database
     * copy of a {@code .proto} file under source control.
     * </p>
     *
     * @param mutateMetaDataProto a callback that mutates the meta-data proto
     * @return a future that completes when the meta-data is mutated
     */
    @Nonnull
    public CompletableFuture<Void> mutateMetaDataAsync(@Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> mutateMetaDataProto) {
        return mutateMetaDataAsync(mutateMetaDataProto, null);
    }

    /**
     * Mutate the stored meta-data proto and record meta-data builder using mutation callbacks asynchronously.
     *
     * <p>
     * This method applies the given mutation callbacks on the stored meta-data and then saves it back to the store. Note the
     * order that the callbacks are executed. {@code mutateMetaDataProto} is called first on the meta-data proto and then the
     * second {@code mutateRecordMetaDataBuilder} is executed on the meta-data builder.
     * Callers might need to provide an appropriate {@link MetaDataEvolutionValidator} to the meta-data store, depending on the
     * changes that the callbacks perform.
     * </p>
     *
     * <p>
     * Also, callers must be cautious about modifying the stored records descriptor using mutation callbacks, as this will make the
     * meta-data store the ultimate (and sole) source of truth for the record definitions and not just the in-database
     * copy of a {@code .proto} file under source control.
     * </p>
     *
     * @param mutateMetaDataProto a callback that mutates the meta-data proto
     * @param mutateRecordMetaDataBuilder a callback that mutates the record meta-data builder after the meta-data proto is mutated
     * @return a future that completes when the meta-data is mutated
     */
    @Nonnull
    public CompletableFuture<Void> mutateMetaDataAsync(@Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> mutateMetaDataProto,
                                                       @Nullable Consumer<RecordMetaDataBuilder> mutateRecordMetaDataBuilder) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataProto.MetaData.Builder metaDataBuilder = metaDataProto.toBuilder();
            mutateMetaDataProto.accept(metaDataBuilder);
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataBuilder.build());
            recordMetaDataBuilder.setVersion(metaDataProto.getVersion() + 1);
            if (mutateRecordMetaDataBuilder != null) {
                mutateRecordMetaDataBuilder.accept(recordMetaDataBuilder);
            }
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Update whether record versions should be stored in the meta-data.
     *
     * @param storeRecordVersions whether record versions should be stored
     */
    public void updateStoreRecordVersions(boolean storeRecordVersions) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_UPDATE_STORE_RECORD_VERSIONS, updateStoreRecordVersionsAsync(storeRecordVersions));
    }

    /**
     * Update whether record versions should be stored in the meta-data asynchronously.
     *
     * @param storeRecordVersions whether record versions should be stored
     * @return a future that completes when {@code storeRecordVersions} is updated
     */
    @Nonnull
    public CompletableFuture<Void> updateStoreRecordVersionsAsync(boolean storeRecordVersions) {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            recordMetaDataBuilder.setStoreRecordVersions(storeRecordVersions);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    /**
     * Enable splitting long records.
     *
     * <p>
     * Note that enabling splitting long records could result in data corruption if the record store was initially created with a format version older than
     * {@link FormatVersion#SAVE_UNSPLIT_WITH_SUFFIX}.
     * Hence the default evolution validator will fail if one enables it. To enable this, first build a custom evolution validator that {@link MetaDataEvolutionValidator#allowsUnsplitToSplit()}
     * and use {@link #setEvolutionValidator(MetaDataEvolutionValidator)} to set the evolution validator used by this store.
     * For more details, see {@link MetaDataEvolutionValidator#allowsUnsplitToSplit()}.
     * </p>
     */
    public void enableSplitLongRecords() {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ENABLE_SPLIT_LONG_RECORDS, enableSplitLongRecordsAsync());
    }

    /**
     * Enable splitting long records asynchronously.
     *
     * <p>
     * Note that enabling splitting long records could result in data corruption if the record store was initially created with a format version older than
     * {@link FormatVersion#SAVE_UNSPLIT_WITH_SUFFIX}.
     * Hence the default evolution validator will fail if one enables it. To enable this, first build a custom evolution validator that {@link MetaDataEvolutionValidator#allowsUnsplitToSplit()}
     * and use {@link #setEvolutionValidator(MetaDataEvolutionValidator)} to set the evolution validator used by this store.
     * For more details, see {@link MetaDataEvolutionValidator#allowsUnsplitToSplit()}.
     * </p>
     *
     * @return a future that completes when {@code splitLongRecords} is set
     */
    @Nonnull
    public CompletableFuture<Void> enableSplitLongRecordsAsync() {
        return loadCurrentProto().thenCompose(metaDataProto -> {
            RecordMetaDataBuilder recordMetaDataBuilder = createMetaDataBuilder(metaDataProto);
            recordMetaDataBuilder.setSplitLongRecords(true);
            return saveAndSetCurrent(recordMetaDataBuilder.getRecordMetaData().toProto());
        });
    }

    @Nullable
    @VisibleForTesting
    public MetaDataCache getCache() {
        return cache;
    }

}
