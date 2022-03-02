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

import com.apple.foundationdb.record.ScanProperties;
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
    public static final RecordLayerPropertyKey<Boolean> LUCENE_INDEX_COMPRESSION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.compressionEnabled", false);

    /**
     * A defined {@link RecordLayerPropertyKey} for Boolean type to control whether the encryption of lucene index is enabled.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Boolean> LUCENE_INDEX_ENCRYPTION_ENABLED = RecordLayerPropertyKey.booleanPropertyKey("com.apple.foundationdb.record.lucene.encryptionEnabled", false);

    /**
     * A defined {@link RecordLayerPropertyKey} for Integer type to control the number of suggestions to look up.
     * The number to use is the minimum of this value and the one configured by the {@link ScanProperties}. So this one is an upper limit.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.autoCompleteSearchLimitation", 10);

    /**
     * A defined {@link RecordLayerPropertyKey} for Long type to control the default weight for each new suggestion when indexing.
     * It is used as a key to get the property value from {@link RecordLayerPropertyStorage#getPropertyValue(RecordLayerPropertyKey)}.
     * Call {@link RecordLayerPropertyKey#buildValue(Supplier)} with a supplier if you want to override this property with a value other than default.
     */
    public static final RecordLayerPropertyKey<Long> LUCENE_AUTO_COMPLETE_DEFAULT_WEIGHT = RecordLayerPropertyKey.longPropertyKey("com.apple.foundationdb.record.lucene.autoCompleteDefaultWeight", 100L);

    /**
     * Upper limitation of text size that is acceptable for auto-complete indexing.
     * Text larger than this limitation is ignored.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.autoCompleteTextSizeUpperLimitation", 32_766);

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
     * Maximum number of segments to be merged at a time for ordinary full-text search with Lucene, during forceMerge for forceMergeDeletes.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_MERGE_MAX_NUMBER = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.mergeMaxNum", 2);

    /**
     * Maximum segment size to produce during normal merging for auto-complete search with Lucene.
     */
    public static final RecordLayerPropertyKey<Double> LUCENE_AUTO_COMPLETE_MERGE_MAX_SIZE = RecordLayerPropertyKey.doublePropertyKey("com.apple.foundationdb.record.lucene.autoCompleteMergeMaxSize", 5.0);

    /**
     * Maximum number of segments to be merged at a time for auto-complete search with Lucene, during forceMerge for forceMergeDeletes.
     */
    public static final RecordLayerPropertyKey<Integer> LUCENE_AUTO_COMPLETE_MERGE_MAX_NUMBER = RecordLayerPropertyKey.integerPropertyKey("com.apple.foundationdb.record.lucene.autoCompleteMergeMaxNum", 2);
}
