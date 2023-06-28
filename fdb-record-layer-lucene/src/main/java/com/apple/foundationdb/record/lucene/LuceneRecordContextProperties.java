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

import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyKey;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;

import java.util.concurrent.ExecutorService;
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
     * This controls whether Lucene indexes' directories (and their directories for auto-complete) should be merged based on probability to reduce multiple merges per transaction.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_MULTIPLE_MERGE_OPTIMIZATION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.multipleMerge.optimizationEnabled", true);

    /**
     * This controls the page size to scan the basic Lucene index.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_INDEX_CURSOR_PAGE_SIZE = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.cursor.pageSize", 201);

    /**
     * If enabled, allow checkIntegrity during indexing.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_INDEX_CHECK_INTEGRITY_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.checkIntegrity", true);
}
