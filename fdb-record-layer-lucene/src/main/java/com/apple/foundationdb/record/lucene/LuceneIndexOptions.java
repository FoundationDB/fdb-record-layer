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
