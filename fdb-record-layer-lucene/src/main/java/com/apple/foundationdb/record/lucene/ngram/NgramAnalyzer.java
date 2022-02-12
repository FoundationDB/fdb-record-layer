/*
 * NgramAnalyzer.java
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

package com.apple.foundationdb.record.lucene.ngram;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.google.auto.service.AutoService;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class NgramAnalyzer extends StopwordAnalyzerBase {
    private static final String DEFAULT_MINIMUM_NGRAM_TOKEN_LENGTH = "3";
    private static final String DEFAULT_MAXIMUM_NGRAM_TOKEN_LENGTH = "30";
    private static final String DEFAULT_NGRAM_WITH_EDGES_ONLY = "false";

    private final int minTokenLength;
    private final int maxTokenLength;
    private final boolean edgesOnly;

    public NgramAnalyzer(@Nullable CharArraySet stopwords, int minTokenLength, int maxTokenLength, boolean edgesOnly) {
        super(stopwords);
        this.minTokenLength = minTokenLength;
        this.maxTokenLength = maxTokenLength;
        this.edgesOnly = edgesOnly;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final StandardTokenizer src = new StandardTokenizer();
        TokenStream tok = new LowerCaseFilter(src);
        tok = new StopFilter(tok, stopwords);
        tok = edgesOnly ? new EdgeNGramTokenFilter(tok, minTokenLength, maxTokenLength, true)
                        : new NGramTokenFilter(tok, minTokenLength, maxTokenLength, true);

        return new TokenStreamComponents(src, tok);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }

    @AutoService(LuceneAnalyzerFactory.class)
    public static class NgramAnalyzerFactory implements LuceneAnalyzerFactory {
        public static final String ANALYZER_NAME = "NGRAM";

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
                final String minLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MIN_SIZE)).orElse(DEFAULT_MINIMUM_NGRAM_TOKEN_LENGTH);
                final String maxLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MAX_SIZE)).orElse(DEFAULT_MAXIMUM_NGRAM_TOKEN_LENGTH);
                final String edgesOnly = Optional.ofNullable(index.getOption(LuceneIndexOptions.NGRAM_TOKEN_EDGES_ONLY)).orElse(DEFAULT_NGRAM_WITH_EDGES_ONLY);
                return new NgramAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, Integer.parseInt(minLengthString), Integer.parseInt(maxLengthString), Boolean.parseBoolean(edgesOnly));
            } catch (NumberFormatException ex) {
                throw new RecordCoreArgumentException("Invalid index option for token size", ex);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other.getClass() != this.getClass()) {
                return false;
            }
            
            NgramAnalyzerFactory otherFactory = (NgramAnalyzerFactory) other;
            return Objects.equals(this.getName(), otherFactory.getName());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.getName());
        }
    }
}
