/*
 * LuceneRecordContextProperties.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.PendingWriteQueue;
import com.apple.foundationdb.record.provider.common.SerializationKeyManager;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyKey;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The list of {@link RecordLayerPropertyKey} for configuration of the lucene indexing for a {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}.
 */
public final class LuceneRecordContextProperties {
    /**
     * A defined {@link RecordLayerPropertyKey} for Boolean type to control whether the compression of lucene index is enabled.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_INDEX_COMPRESSION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.compressionEnabled", true);

    /**
     * A defined {@link RecordLayerPropertyKey} for Boolean type to control whether the encryption of lucene index is enabled.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_INDEX_ENCRYPTION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.encryptionEnabled", false);

    public static final RecordLayerPropertyKey<SerializationKeyManager> LUCENE_INDEX_KEY_MANAGER = new RecordLayerPropertyKey<>("com.apple.foundationdb.record.lucene.keyManager", null, SerializationKeyManager.class);

    /**
     * Whether {@code StoredField} and {@code FieldInfo} are also encoded, allowing compression and encryption.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_FIELD_PROTOBUF_PREFIX_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.fieldProtobufPrefixEnabled", false);

    /**
     * An {@link ExecutorService} to use for parallel execution in {@link LuceneRecordCursor}.
     */
    public static final RecordLayerPropertyKey<ExecutorService> LUCENE_EXECUTOR_SERVICE = new RecordLayerPropertyKey<>("com.apple.foundationdb.record.lucene.executorService", null, ExecutorService.class);

    /**
     * A defined {@link RecordLayerPropertyKey} for Integertype to control the number of spellcheck suggestions to look up.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_SPELLCHECK_SEARCH_UPPER_LIMIT = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.spellcheckSearchLimitation", 5);

    /**
     * Maximum segment size to produce during normal merging for ordinary full-text search with Lucene.
     */
    public static final RecordLayerPropertyKey<Double> LUCENE_MERGE_MAX_SIZE = RecordLayerPropertyKey.doublePropertyKey("com.apple.foundationdb.record.lucene.mergeMaxSize", 5.0);

    /**
     * Count of segments after which to merge for ordinary full-text search with Lucene.
     */
    public static final RecordLayerPropertyKey<Double> LUCENE_MERGE_SEGMENTS_PER_TIER = RecordLayerPropertyKey.doublePropertyKey("com.apple.foundationdb.record.lucene.mergeSegmentsPerTier", 10.0);

    /**
     * This controls whether Lucene indexes' directories (and their directories for auto-complete) should be merged based on probability to reduce multiple merges per transaction.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_MULTIPLE_MERGE_OPTIMIZATION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.multipleMerge.optimizationEnabled", true);

    /**
     * This controls the page size to scan the basic Lucene index.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_INDEX_CURSOR_PAGE_SIZE = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.cursor.pageSize", 201);

    /**
     * This controls the number of threads used when opening segments in parallel.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_OPEN_PARALLELISM = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.open.parallelism", 16);

    /**
     * During merge, commit the agile context right after this time quota is reached. Milliseconds units.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_AGILE_COMMIT_TIME_QUOTA = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.agile.time.quota", 4000);

    /**
     * During merge, commit the agile context right after write size exceeds this value. Bytes units.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_AGILE_COMMIT_SIZE_QUOTA = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.agile.size.quota", 900_000);
    /**
     * If set to true, disable the agility context feature and force every merge to be performed in a single transaction.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_AGILE_DISABLE_AGILITY_CONTEXT = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.agile.disabled", false);
    /**
     * Number of documents to move from a partition when its size exceeds {@link com.apple.foundationdb.record.lucene.LuceneIndexOptions#INDEX_PARTITION_HIGH_WATERMARK}.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_REPARTITION_DOCUMENT_COUNT = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.repartition.document.count", 16);
    /**
     * Maximum number of documents to move during a re-balancing run.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.repartition.max.document.count", 1000);
    /**
     * Lucene file lock time window in milliseconds. If a file lock is older (or younger) than this value, the lock will be considered invalid.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_FILE_LOCK_TIME_WINDOW_MILLISECONDS = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.file.lock.window.milliseconds", (int)(TimeUnit.SECONDS.toMillis(20)));
    /**
     * Use concurrent merge scheduler. The default is now to assume deferred operations when available, and hence
     * assume that merge will not be performed during IO but in a background process.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_USE_CONCURRENT_MERGE_SCHEDULER = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.useConcurrentMergeScheduler", false);
    /**
     * Use "default transaction priority" during merge. The default of this prop is {@code true} because merge over multiple transactions may be preventing user operations.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_USE_DEFAULT_PRIORITY_DURING_MERGE = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.useDefaultPriorityDuringMerge", true);
    /**
     * Lucene block cache maximum size. At most these many blocks will be stored in cache.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_BLOCK_CACHE_MAXIMUM_SIZE = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.block.cache.size", FDBDirectory.DEFAULT_BLOCK_CACHE_MAXIMUM_SIZE);
    /**
     * Lucene async to sync behavior: Whether to use the legacy async to sync calls or the non-exception-mapping behavior.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_USE_LEGACY_ASYNC_TO_SYNC = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.exception.mapping.enabled", true);
    /**
     * Lucene max number of pending writes that can be replayed for a query.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_MAX_PENDING_WRITES_REPLAYED_FOR_QUERY = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.max.pending.writes.replay", PendingWriteQueue.MAX_PENDING_ENTRIES_TO_REPLAY);
}
