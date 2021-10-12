/*
 * IndexingCommon.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shared data structure to be used (only) by the Indexing* modules.
 */

@API(API.Status.INTERNAL)
public class IndexingCommon {
    private final UUID uuid = UUID.randomUUID();

    @Nonnull private final FDBDatabaseRunner runner;
    @Nullable private SynchronizedSessionRunner synchronizedSessionRunner = null;

    @Nonnull private final FDBRecordStore.Builder recordStoreBuilder;
    @Nonnull private final AtomicLong totalRecordsScanned;

    private final boolean useSynchronizedSession;
    private final boolean trackProgress;
    private final long leaseLengthMillis;

    @Nonnull public OnlineIndexer.Config config; // this item may be modified on the fly
    @Nullable private final Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader;
    private int configLoaderInvocationCount = 0;

    @Nonnull private Collection<RecordType> allRecordTypes;
    @Nonnull private final List<IndexContext> targetIndexContexts;
    /**
     * Constant indicating that there should be no limit to some usually limited operation.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    public static class IndexContext {
        @Nonnull public final Index index;
        @Nonnull public final Collection<RecordType> recordTypes;
        public final boolean isSynthetic;

        IndexContext(@Nonnull Index index,
                     @Nonnull Collection<RecordType> recordTypes,
                     boolean isSynthetic) {
            this.index = index;
            this.recordTypes = recordTypes;
            this.isSynthetic = isSynthetic;
        }
    }

    IndexingCommon(@Nonnull FDBDatabaseRunner runner,
                   @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                   @Nonnull List<Index> targetIndexes,
                   @Nullable Collection<RecordType> allRecordTypes,
                   @Nullable Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader,
                   @Nonnull OnlineIndexer.Config config,
                   boolean trackProgress,
                   boolean useSynchronizedSession,
                   long leaseLengthMillis) {
        this.useSynchronizedSession = useSynchronizedSession;
        this.runner = runner;
        this.configLoader = configLoader;
        this.config = config;
        this.trackProgress = trackProgress;
        this.recordStoreBuilder = recordStoreBuilder;
        this.leaseLengthMillis = leaseLengthMillis;

        this.totalRecordsScanned = new AtomicLong(0);
        this.targetIndexContexts = new ArrayList<>(targetIndexes.size());
        this.allRecordTypes = new HashSet<>();

        fillTargetIndexers(targetIndexes, allRecordTypes);
    }

    private void fillTargetIndexers(@Nonnull List<Index> targetIndexes, @Nullable Collection<RecordType> recordTypes) {
        boolean presetTypes = false;
        if (recordTypes != null) {
            if (targetIndexes.size() > 1) {
                throw new IndexingBase.ValidationException("Can't use preset record types with multi target indexing");
            }
            presetTypes = true;
        }
        if (recordStoreBuilder.getMetaDataProvider() == null) {
            throw new MetaDataException("record store builder must include metadata");
        }
        final RecordMetaData metaData = recordStoreBuilder.getMetaDataProvider().getRecordMetaData();
        for (Index targetIndex: targetIndexes) {
            Collection<RecordType> types;
            if (presetTypes) {
                types = recordTypes;
            } else {
                types = metaData.recordTypesForIndex(targetIndex);
            }
            boolean isSynthetic = false;
            if (types.stream().anyMatch(RecordType::isSynthetic)) {
                types = new SyntheticRecordPlanner(metaData, new RecordStoreState(null, null))
                        .storedRecordTypesForIndex(targetIndex, types);
                isSynthetic = true;
            }
            targetIndexContexts.add(new IndexContext(targetIndex, types, isSynthetic));
            allRecordTypes.addAll(types);
        }
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean isUseSynchronizedSession() {
        return useSynchronizedSession;
    }

    public List<Object> indexLogMessageKeyValues() {
        return indexLogMessageKeyValues(null);
    }

    public List<Object> indexLogMessageKeyValues(@Nullable String transactionName) {
        List<Object> keyValues = new ArrayList<>() ;

        logIf(transactionName != null, keyValues,
                LogMessageKeys.TRANSACTION_NAME, transactionName);

        logIf(true, keyValues,
                LogMessageKeys.TARGET_INDEX_NAME, getTargetIndexesNames(),
                LogMessageKeys.RECORDS_SCANNED, totalRecordsScanned.get(),
                LogMessageKeys.INDEXER_ID, uuid);

        return keyValues;
    }

    @SuppressWarnings("varargs")
    private void logIf(boolean condition, List<Object> list, @Nonnull Object... a) {
        if (condition) {
            list.addAll(Arrays.asList(a));
        }
    }

    @Nonnull
    public FDBDatabaseRunner getRunner() {
        return synchronizedSessionRunner == null ? runner : synchronizedSessionRunner;
    }

    @Nonnull
    public Index getIndex() {
        if (isMultiTarget()) {
            // backward compatibility safeguard - modules that do not support multi targets (yet) will continue calling
            // this function, which verifies a very lonely target index
            throw new IndexingBase.ValidationException("Multi target index exist, but an operation that assumes a single index was called");
        }
        return targetIndexContexts.get(0).index;
    }

    @Nonnull
    public Index getPrimaryIndex() {
        return targetIndexContexts.get(0).index;
    }

    @Nonnull
    public Collection<RecordType> getAllRecordTypes() {
        return allRecordTypes;
    }

    @Nonnull
    public List<IndexContext> getTargetIndexContexts() {
        return targetIndexContexts;
    }

    @Nonnull
    public List<Index> getTargetIndexes() {
        return targetIndexContexts.stream().map(targetIndexContext -> targetIndexContext.index).collect(Collectors.toList());
    }

    @Nonnull
    public List<String> getTargetIndexesNames() {
        return getTargetIndexes().stream().map(Index::getName).collect(Collectors.toList());
    }

    boolean isMultiTarget() {
        return targetIndexContexts.size() > 1;
    }

    public boolean isTrackProgress() {
        return trackProgress;
    }

    @Nonnull
    public FDBRecordStore.Builder getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    @Nullable
    public SynchronizedSessionRunner getSynchronizedSessionRunner() {
        return synchronizedSessionRunner;
    }

    public void setSynchronizedSessionRunner(@Nullable final SynchronizedSessionRunner synchronizedSessionRunner) {
        this.synchronizedSessionRunner = synchronizedSessionRunner;
    }

    @Nonnull
    public AtomicLong getTotalRecordsScanned() {
        return totalRecordsScanned;
    }

    public int getConfigLoaderInvocationCount() {
        return configLoaderInvocationCount;
    }

    public long getLeaseLengthMillis() {
        return leaseLengthMillis;
    }

    public boolean loadConfig() {
        if (configLoader == null) {
            return false;
        }
        configLoaderInvocationCount++;
        config = configLoader.apply(config);
        return true;
    }

    public void close() {
        runner.close();
        if (synchronizedSessionRunner != null) {
            synchronizedSessionRunner.close();
        }
    }
}
