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
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.tuple.Tuple;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Shared data structure to be used (only) by the Indexing* modules.
 */

@API(API.Status.INTERNAL)
public class IndexingCommon {
    private final UUID indexerId = UUID.randomUUID();

    private final FDBDatabaseRunner runner;

    private final FDBRecordStore.Builder recordStoreBuilder;
    private final AtomicLong totalRecordsScanned;
    private final boolean trackProgress;

    OnlineIndexOperationConfig config; // this item may be modified on the fly
    @Nullable private final Function<OnlineIndexOperationConfig, OnlineIndexOperationConfig> configLoader;
    private int configLoaderInvocationCount = 0;

    private Collection<RecordType> allRecordTypes;
    private final List<IndexContext> targetIndexContexts;
    private List<Index> queuedIndexes = Collections.emptyList();
    /**
     * Constant indicating that there should be no limit to some usually limited operation.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    /**
     * Index and record types for indexing.
     */
    // TODO: Make a Java record when available.
    public static class IndexContext {
        public final Index index;
        public final Collection<RecordType> recordTypes;
        public final boolean isSynthetic;

        IndexContext(Index index,
                     Collection<RecordType> recordTypes,
                     boolean isSynthetic) {
            this.index = index;
            this.recordTypes = recordTypes;
            this.isSynthetic = isSynthetic;
        }
    }

    IndexingCommon(FDBDatabaseRunner runner,
                   FDBRecordStore.Builder recordStoreBuilder,
                   List<Index> targetIndexes,
                   @Nullable Collection<RecordType> allRecordTypes,
                   @Nullable UnaryOperator<OnlineIndexOperationConfig> configLoader,
                   OnlineIndexOperationConfig config,
                   boolean trackProgress) {
        this.runner = runner;
        this.configLoader = configLoader;
        this.config = config;
        this.trackProgress = trackProgress;
        this.recordStoreBuilder = recordStoreBuilder;

        this.totalRecordsScanned = new AtomicLong(0);
        this.targetIndexContexts = new ArrayList<>(targetIndexes.size());
        this.allRecordTypes = new HashSet<>();

        fillTargetIndexers(targetIndexes, allRecordTypes);
    }

    private void fillTargetIndexers(List<Index> targetIndexes, @Nullable Collection<RecordType> recordTypes) {
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
                types = SyntheticRecordPlanner.storedRecordTypesForIndex(metaData, targetIndex, types);
                isSynthetic = true;
            }
            targetIndexContexts.add(new IndexContext(targetIndex, types, isSynthetic));
            allRecordTypes.addAll(types);
        }
    }

    public UUID getIndexerId() {
        return indexerId;
    }

    public List<Object> indexLogMessageKeyValues() {
        return indexLogMessageKeyValues(null);
    }

    public List<Object> indexLogMessageKeyValues(@Nullable String transactionName) {
        return indexLogMessageKeyValues(transactionName, null);
    }

    public List<Object> indexLogMessageKeyValues(@Nullable String transactionName, @Nullable List<Object> moreKeyValues) {
        List<Object> keyValues = new ArrayList<>() ;

        logIf(transactionName != null, keyValues,
                LogMessageKeys.TRANSACTION_NAME, transactionName);

        logIf(true, keyValues,
                LogMessageKeys.TARGET_INDEX_NAME, getTargetIndexesNames(),
                LogMessageKeys.RECORDS_SCANNED, totalRecordsScanned.get(),
                LogMessageKeys.INDEXER_ID, indexerId);

        SubspaceProvider subspaceProvider = getRecordStoreBuilder().getSubspaceProvider();
        if (subspaceProvider != null) {
            keyValues.add(subspaceProvider.logKey());
            keyValues.add(subspaceProvider);
        }

        if (moreKeyValues != null && !moreKeyValues.isEmpty()) {
            keyValues.addAll(moreKeyValues);
        }

        return keyValues;
    }

    @SuppressWarnings("varargs")
    private void logIf(boolean condition, List<Object> list, Object... a) {
        if (condition) {
            list.addAll(Arrays.asList(a));
        }
    }

    public FDBDatabaseRunner getRunner() {
        return runner;
    }

    IndexContext getIndexContext() {
        if (isMultiTarget()) {
            // backward compatibility safeguard - modules that do not support multi targets (yet) will continue calling
            // this function, which verifies a very lonely target index
            throw new IndexingBase.ValidationException("Multi target index exist, but an operation that assumes a single index was called");
        }
        return targetIndexContexts.get(0);
    }

    public Index getIndex() {
        return getIndexContext().index;
    }

    public Index getPrimaryIndex() {
        return targetIndexContexts.get(0).index;
    }

    public Collection<RecordType> getAllRecordTypes() {
        return allRecordTypes;
    }

    @Nullable
    public TupleRange computeRecordsRange() {
        Tuple low = null;
        Tuple high = null;
        for (RecordType recordType : getAllRecordTypes()) {
            if (!recordType.primaryKeyHasRecordTypePrefix() || recordType.isSynthetic()) {
                // If any of the types to build for does not have a prefix, give up.
                return null;
            }
            Tuple prefix = recordType.getRecordTypeKeyTuple();
            if (low == null) {
                low = high = prefix;
            } else if (low.compareTo(prefix) > 0) {
                low = prefix;
            } else if (high.compareTo(prefix) < 0) {
                high = prefix;
            }
        }
        return low == null ? null : TupleRange.betweenInclusive(low, high);
    }

    public List<IndexContext> getTargetIndexContexts() {
        return targetIndexContexts;
    }

    public List<Index> getTargetIndexes() {
        return targetIndexContexts.stream().map(targetIndexContext -> targetIndexContext.index).toList();
    }

    /**
     * Return a cached list of indexes that are in {@link IndexState#WRITE_ONLY_WITH_QUEUE} state. These
     * indexes may require special handling (queue drain) during the indexing process.
     * @return list of indexes
     */
    public List<Index> getQueuedIndexes() {
        return queuedIndexes;
    }

    /**
     * Cache a list of indexes that are in {@link IndexState#WRITE_ONLY_WITH_QUEUE} state. These
     * indexes may require special handling (queue drain) during the indexing process.
     */
    public void setQueuedIndexes(List<Index> queuedIndexes) {
        this.queuedIndexes = queuedIndexes;
    }

    public List<String> getTargetIndexesNames() {
        return getTargetIndexes().stream().map(Index::getName).toList();
    }

    boolean isMultiTarget() {
        return targetIndexContexts.size() > 1;
    }

    public boolean isTrackProgress() {
        return trackProgress;
    }

    public FDBRecordStore.Builder getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    public AtomicLong getTotalRecordsScanned() {
        return totalRecordsScanned;
    }

    public int getConfigLoaderInvocationCount() {
        return configLoaderInvocationCount;
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
    }
}
