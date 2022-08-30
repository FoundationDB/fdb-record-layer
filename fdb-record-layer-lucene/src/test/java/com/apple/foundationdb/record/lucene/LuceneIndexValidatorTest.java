/*
 * LuceneIndexValidatorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link LuceneIndexValidator}'s validation for Lucene index options.
 */
public class LuceneIndexValidatorTest {
    @Test
    void testInvalidIndexOptions() {
        // valid options
        final Map<String, String> options = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, " text1 :NGRAM,text2: SYNONYM",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2, text3");
        assertDoesNotThrow(() -> validateIndexOptions(options), "Whitespaces should be stripped out they are valid option values");
        Map<String, String> parsedMap = LuceneIndexOptions.parseKeyValuePairOptionValue(" text1 :NGRAM,text2:SYNONYM");
        Set<String> parsedSet = LuceneIndexOptions.parseMultipleElementsOptionValue("text2, text3");
        assertEquals(parsedMap, Map.of("text1", "NGRAM", "text2", "SYNONYM"));
        assertEquals(parsedSet, Set.of("text2", "text3"));

        // invalid options
        final Map<String, String> options2 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1:NGRAM,text2:SYNONYM,",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2,text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options2));

        // invalid options
        final Map<String, String> options3 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1:NGRAM,,text2:SYNONYM",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2,text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options3));

        // invalid options
        final Map<String, String> options4 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1:NGRAM,text2",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2,text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options4));

        // invalid options
        final Map<String, String> options5 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1:NGRAM,text2:SYNONYM",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2:text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options5));

        // invalid options
        final Map<String, String> options6 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1:NGRAM,text2:SYNONYM",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, ",text2,text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options6));

        // invalid options
        final Map<String, String> options7 = ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text1,text2",
                LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS, "text2,text3");
        assertThrows(MetaDataException.class,
                () -> validateIndexOptions(options7));
    }

    void validateIndexOptions(@Nonnull Map<String, String> indexOptions) {
        Index index = new Index("Complex$multiple_analyzer_autocomplete",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                LuceneIndexTypes.LUCENE,
                indexOptions);
        LuceneIndexValidator.validateIndexOptions(index);
    }
}
