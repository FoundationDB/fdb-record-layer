/*
 * LuceneAnalyzerRegistryTest.java
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

import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.google.common.collect.ImmutableMap;
//import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Test for {@link LuceneAnalyzerRegistry}.
 */
public class LuceneAnalyzerRegistryTest {
    @Test
    void searchForAutoCompleteWithSynonymEnabledOnFullText() {
        // Build an index with auto-complete enabled and synonym analyzer used for full-text
        final Index index = new Index("Simple_with_auto_complete",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                LuceneIndexTypes.LUCENE,
                ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                        LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));
        // Assert the synonym analyzer is used for query analyzer for full-text search
        Assertions.assertEquals(SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(index, LuceneAnalyzerType.FULL_TEXT, Map.of()).provideQueryAnalyzer().getUniqueIdentifier());
        // Assert the standard analyzer is used for query analyzer for auto-complete suggestions
        Assertions.assertEquals(LuceneAnalyzerWrapper.STANDARD_ANALYZER_NAME,
                LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(index, LuceneAnalyzerType.AUTO_COMPLETE, Map.of()).provideQueryAnalyzer().getUniqueIdentifier());
    }
}
