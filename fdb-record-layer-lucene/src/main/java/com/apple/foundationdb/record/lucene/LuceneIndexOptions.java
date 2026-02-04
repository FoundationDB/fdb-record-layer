/*
 * LuceneIndexOptions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Options for use with Lucene indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexOptions {
    /**
     * The delimiter by which the key and value of one key-value pair within the option's value is split.
     */
    public static final String DELIMITER_BETWEEN_KEY_AND_VALUE = ":";
    /**
     * The delimiter by which the elements are split if the option's value has multiple elements or key-value pairs.
     */
    public static final String DELIMITER_BETWEEN_ELEMENTS = ",";

    /**
     * Whether a Lucene's EdgeNGramTokenFilter or a regular NGramTokenFilter to use for the ngram analyzer.
     */
    public static final String NGRAM_TOKEN_EDGES_ONLY = "ngramTokenEdgesOnly";
    /**
     * The name of the Lucene analyzer to use for full-text search.
     */
    public static final String LUCENE_ANALYZER_NAME_OPTION = "luceneAnalyzerName";
    /**
     * The override mapping from fields to Lucene analyzers, if they want to use different analyzer than the default one.
     * The format of the value should be "fieldName1:analyzerName1,fieldName2:analyzerName2...", with key-value pairs split by "," and each key and value split by ":".
     */
    public static final String LUCENE_ANALYZER_NAME_PER_FIELD_OPTION = "luceneAnalyzerNamePerField";
    /**
     * The name of the Lucene analyzer to use for auto-complete query.
     */
    public static final String AUTO_COMPLETE_ANALYZER_NAME_OPTION = "autoCompleteAnalyzerName";
    /**
     * The override mapping from fields to Lucene analyzers for auto-complete query, if they want to use different analyzers than the default one.
     * The format of the value should be "fieldName1:analyzerName1,fieldName2:analyzerName2...", with key-value pairs split by "," and each key and value split by ":".
     */
    public static final String AUTO_COMPLETE_ANALYZER_NAME_PER_FIELD_OPTION = "autoCompleteAnalyzerNamePerField";
    /**
     * The name of the synonym set to use in Lucene.
     */
    public static final String TEXT_SYNONYM_SET_NAME_OPTION = "textSynonymSetName";
    public static final String PRIMARY_KEY_SERIALIZATION_FORMAT = "primaryKeySerializationFormat";
    /**
     * Whether a separate B-tree index gives a direct mapping of live documents to segment by record primary key.
     * Boolean string ({@code true} or {@code false}).
     */
    public static final String PRIMARY_KEY_SEGMENT_INDEX_ENABLED = "primaryKeySegmentIndexEnabled";
    /**
     * Whether to use the StoredFields format that stores data in K/V (TRUE) pairs or in the standard Lucene file (FALSE).
     */
    public static final String OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED = "optimizedStoredFieldsFormatEnabled";

    /**
     * Option to designate a record field as the timestamp by which the corresponding document would be partitioned in
     * Lucene.
     */
    public static final String INDEX_PARTITION_BY_FIELD_NAME = "partitionFieldName";

    /**
     * Option to set high watermark size for a lucene partition, beyond which the partition would be split, or a new
     * partition would be created.
     */
    public static final String INDEX_PARTITION_HIGH_WATERMARK = "partitionHighWatermark";

    /**
     * Option to set low watermark size for a lucene partition, below which the partition would be a candidate for
     * merge with another adjacent partition.
     */
    public static final String INDEX_PARTITION_LOW_WATERMARK = "partitionLowWatermark";
    /**
     * Whether a separate B-tree index gives a direct mapping of live documents to segment by record primary key, that
     * utilizes an optimized stored fields to manage merges.
     * <p>
     * Boolean string ({@code true} or {@code false}).
     * </p>
     * <p>
     * This is incompatible with {@link #PRIMARY_KEY_SEGMENT_INDEX_ENABLED}, and thus only one can be enabled on an
     * index.
     * This implies {@link #OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED}, and thus
     * {@code OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED} cannot be set (to {@code true} or {@code false}) if this is
     * enabled.
     * </p>
     */
    public static final String PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED = "primaryKeySegmentIndexV2Enabled";
    /**
     * Enable pending write queue during an ongoing merge.
     * During an ongoing merge the index is locked, which causes record updates attempts to fail. When this option is set to true,
     * record updates during merge will be written to a queue and applied when the merge is done.
     * When such queue exists, queries on the index will first apply the items in the pending queue (without commiting),
     * which will keep consistency.
     */
    public static final String ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE = "enablePendingWriteQueueDuringMerge";

    private LuceneIndexOptions() {
    }

    /**
     * Parse an option's value to a key-value map.
     * @param optionValue the raw string for option's value
     * @return the map
     */
    public static Map<String, String> parseKeyValuePairOptionValue(@Nonnull String optionValue) {
        final String[] elements = optionValue.strip().split(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS);
        final Map<String, String> map = new HashMap<>();
        for (String element : elements) {
            final String[] keyValuePair = element.split(LuceneIndexOptions.DELIMITER_BETWEEN_KEY_AND_VALUE);
            final String key = keyValuePair[0].strip();
            final String value = keyValuePair[1].strip();
            map.put(key, value);
        }
        return map;
    }

    /**
     * Parse an option's value to a set of strings.
     * @param optionValue the raw string for option's value
     * @return map
     */
    public static Set<String> parseMultipleElementsOptionValue(@Nonnull String optionValue) {
        final String[] elements = optionValue.strip().split(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS);
        final Set<String> list = new HashSet<>();
        for (String fieldName : elements) {
            list.add(fieldName.strip());
        }
        return list;
    }

    /**
     * Validate the raw string for an option's value has valid format for a key-value map.
     * @param optionValue the raw string for option's value
     * @param exceptionToThrow the exception to throw if it is invalid
     */
    public static void validateKeyValuePairOptionValue(@Nonnull String optionValue, @Nonnull RecordCoreException exceptionToThrow) {
        optionValue = optionValue.strip();
        if (optionValue.startsWith(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS) || optionValue.endsWith(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS)) {
            throw exceptionToThrow;
        }
        final String[] keyValuePairs = optionValue.split(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS);
        for (String rawString : keyValuePairs) {
            final String keyValue = rawString.strip();
            int firstIndex = keyValue.indexOf(LuceneIndexOptions.DELIMITER_BETWEEN_KEY_AND_VALUE);
            if (keyValue.isEmpty()
                    || firstIndex < 1 || firstIndex > keyValue.length() - 2
                    || firstIndex != keyValue.lastIndexOf(LuceneIndexOptions.DELIMITER_BETWEEN_KEY_AND_VALUE)) {
                throw exceptionToThrow;
            }
        }
    }

    /**
     * Validate the raw string for an option's value has valid format for multiple-elements.
     * @param optionValue the raw string for option's value
     * @param exceptionToThrow the exception to throw if it is invalid
     */
    public static void validateMultipleElementsOptionValue(@Nonnull String optionValue, @Nonnull RecordCoreException exceptionToThrow) {
        optionValue = optionValue.strip();
        if (optionValue.startsWith(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS)
                || optionValue.endsWith(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS)
                || optionValue.contains(LuceneIndexOptions.DELIMITER_BETWEEN_KEY_AND_VALUE)) {
            throw exceptionToThrow;
        }
        final String[] fieldNames = optionValue.split(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS);
        for (String rawString : fieldNames) {
            if (rawString.isBlank()) {
                throw exceptionToThrow;
            }
        }
    }
}
