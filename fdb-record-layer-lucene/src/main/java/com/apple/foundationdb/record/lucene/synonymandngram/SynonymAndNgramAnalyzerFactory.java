/*
 * SynonymAndNgramAnalyzerFactory.java
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

package com.apple.foundationdb.record.lucene.synonymandngram;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@AutoService(LuceneAnalyzerFactory.class)
public class SynonymAndNgramAnalyzerFactory implements LuceneAnalyzerFactory {
    public static final String NGRAM_FIELD_PREFIX = "ng-";
    public static final String ANALYZER_NAME = "SYNONYM_AND_NGRAM";

    @Nonnull
    @Override
    public String getName() {
        return ANALYZER_NAME;
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    @Override
    public Analyzer getIndexAnalyzer(@Nonnull Index index) {
        try {
            final String minLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MIN_SIZE)).orElse(NgramAnalyzer.DEFAULT_MINIMUM_NGRAM_TOKEN_LENGTH);
            final String maxLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MAX_SIZE)).orElse(NgramAnalyzer.DEFAULT_MAXIMUM_NGRAM_TOKEN_LENGTH);
            final String edgesOnly = Optional.ofNullable(index.getOption(IndexOptions.NGRAM_TOKEN_EDGES_ONLY)).orElse(NgramAnalyzer.DEFAULT_NGRAM_WITH_EDGES_ONLY);
            return new SynonymAndNgramIndexAnalyzer(StandardAnalyzer.STOP_WORDS_SET, Integer.parseInt(minLengthString), Integer.parseInt(maxLengthString), Boolean.parseBoolean(edgesOnly));
        } catch (NumberFormatException ex) {
            throw new RecordCoreArgumentException("Invalid index option for token size", ex);
        }
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    @Override
    public Analyzer getQueryAnalyzer(@Nonnull Index index, @Nonnull Analyzer indexAnalyzer) {
        return new SynonymAndNgramQueryAnalyzer(StandardAnalyzer.STOP_WORDS_SET);
    }

    @Nonnull
    @Override
    public Function<String, List<String>> getFieldSplitter() {
        return field -> ImmutableList.of(field, NGRAM_FIELD_PREFIX.concat(field));
    }
}
