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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Shared data structure to be used (only) by the Indexing* modules.
 */

@API(API.Status.INTERNAL)
public class IndexingCommon {
    private final UUID uuid = UUID.randomUUID();

    @Nonnull private final FDBDatabaseRunner runner;
    @Nullable private SynchronizedSessionRunner synchronizedSessionRunner = null;

    @Nonnull private final FDBRecordStore.Builder recordStoreBuilder;
    @Nonnull private final Index index;
    @Nonnull private final AtomicLong totalRecordsScanned;

    private final boolean useSynchronizedSession;
    private final boolean syntheticIndex;
    private final boolean trackProgress;
    private final long leaseLengthMillis;

    @Nonnull public OnlineIndexer.Config config; // this item may be modified on the fly
    @Nullable private final Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader;
    private int configLoaderInvocationCount = 0;

    @Nonnull public Collection<RecordType> recordTypes;

    /**
     * Constant indicating that there should be no limit to some usually limited operation.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    IndexingCommon(@Nonnull FDBDatabaseRunner runner,
                   @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                   @Nonnull Index index, @Nonnull Collection<RecordType> recordTypes,
                   @Nonnull Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader, @Nonnull OnlineIndexer.Config config,
                   boolean syntheticIndex,
                   boolean trackProgress,
                   boolean useSynchronizedSession,
                   long leaseLengthMillis) {
        this.useSynchronizedSession = useSynchronizedSession;
        this.runner = runner;
        this.index = index;
        this.recordTypes = recordTypes;
        this.configLoader = configLoader;
        this.config = config;
        this.syntheticIndex = syntheticIndex;
        this.trackProgress = trackProgress;
        this.recordStoreBuilder = recordStoreBuilder;
        this.leaseLengthMillis = leaseLengthMillis;

        this.totalRecordsScanned = new AtomicLong(0);
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
        if (transactionName == null) {
            return Arrays.asList(
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    LogMessageKeys.RECORDS_SCANNED, totalRecordsScanned.get(),
                    LogMessageKeys.INDEXER_ID, uuid);
        } else {
            return Arrays.asList(
                    LogMessageKeys.TRANSACTION_NAME, transactionName,
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    LogMessageKeys.RECORDS_SCANNED, totalRecordsScanned.get(),
                    LogMessageKeys.INDEXER_ID, uuid);
        }
    }

    @Nonnull
    public FDBDatabaseRunner getRunner() {
        return synchronizedSessionRunner == null ? runner : synchronizedSessionRunner;
    }

    @Nonnull
    public Index getIndex() {
        return index;
    }

    public boolean isSyntheticIndex() {
        return syntheticIndex;
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

    @Nullable
    public SyntheticRecordFromStoredRecordPlan getSyntheticPlan(FDBRecordStore store) {
        if (!syntheticIndex) {
            return null;
        }
        final SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(store.getRecordMetaData(), store.getRecordStoreState().withWriteOnlyIndexes(Collections.singletonList(index.getName())));
        return syntheticPlanner.forIndex(index);
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
