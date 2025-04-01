/*
 * SynonymAnalyzer.java
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

package com.apple.foundationdb.record.lucene.synonym;

import com.apple.foundationdb.record.lucene.AnalyzerChooser;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerType;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.metadata.Index;
import com.google.auto.service.AutoService;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * The analyzer for index with synonym enabled.
 */
public class SynonymAnalyzer extends StopwordAnalyzerBase {
    @Nonnull
    private final String name;

    private int maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;

    @Nonnull
    public String getName() {
        return name;
    }

    @Nullable
    public CharArraySet getStopwords() {
        return stopwords;
    }

    public void setMaxTokenLength(int length) {
        this.maxTokenLength = length;
    }

    public int getMaxTokenLength() {
        return this.maxTokenLength;
    }

    public SynonymAnalyzer(@Nullable CharArraySet stopwords, @Nonnull String name) {
        super(stopwords);
        this.name = name;
    }

    public SynonymAnalyzer(@Nullable CharArraySet stopwords, @Nonnull String name, int maxTokenLength) {
        super(stopwords);
        this.name = name;
        this.maxTokenLength = maxTokenLength;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    protected TokenStreamComponents createComponents(String fieldName) {
        final UAX29URLEmailTokenizer src = new UAX29URLEmailTokenizer();
        src.setMaxTokenLength(maxTokenLength);
        TokenStream tok = new LowerCaseFilter(src);
        tok = new StopFilter(tok, stopwords);
        tok = new SynonymGraphFilter(tok, getSynonymMap(), true);

        return new TokenStreamComponents(r -> {
            src.setMaxTokenLength(maxTokenLength);
            src.setReader(r);
        }, tok);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }

    @Nonnull
    private SynonymMap getSynonymMap() {
        return SynonymMapRegistryImpl.instance().getSynonymMap(name);
    }

    /**
     * An analyzer factory including in fly synonym tokenizing on query time.
     * The synonyms are not indexed to disk. So no index rebuilding is needed if the wordnet file gets updated.
     */
    @AutoService(LuceneAnalyzerFactory.class)
    public static class QueryOnlySynonymAnalyzerFactory implements LuceneAnalyzerFactory {
        public static final String ANALYZER_FACTORY_NAME = "SYNONYM";

        @Nonnull
        @Override
        public String getName() {
            return ANALYZER_FACTORY_NAME;
        }

        @Nonnull
        @Override
        public LuceneAnalyzerType getType() {
            return LuceneAnalyzerType.FULL_TEXT;
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index index) {
            return LuceneAnalyzerWrapper::getStandardAnalyzerWrapper;
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public AnalyzerChooser getQueryAnalyzerChooser(@Nonnull Index index, @Nonnull AnalyzerChooser indexAnalyzerChooser) {
            final String name = Objects.requireNonNullElse(index.getOption(LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION),
                    EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
            return () -> new LuceneAnalyzerWrapper(ANALYZER_FACTORY_NAME,
                    new SynonymAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, name));
        }
    }

    /**
     * An analyzer factory including synonym tokenizing on both index time and query time.
     * Only authoritative phrase for each synonym group is included in the token stream.
     */
    @AutoService(LuceneAnalyzerFactory.class)
    public static class AuthoritativeSynonymOnlyAnalyzerFactory implements LuceneAnalyzerFactory {
        public static final String ANALYZER_FACTORY_NAME = "INDEX_ONLY_SYNONYM";

        @Nonnull
        @Override
        public String getName() {
            return ANALYZER_FACTORY_NAME;
        }

        @Nonnull
        @Override
        public LuceneAnalyzerType getType() {
            return LuceneAnalyzerType.FULL_TEXT;
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index index) {
            final String name = Objects.requireNonNullElse(index.getOption(LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION),
                    EnglishSynonymMapConfig.AuthoritativeOnlyEnglishSynonymMapConfig.CONFIG_NAME);
            return () -> new LuceneAnalyzerWrapper(ANALYZER_FACTORY_NAME,
                    new SynonymAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, name));
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public AnalyzerChooser getQueryAnalyzerChooser(@Nonnull Index index, @Nonnull AnalyzerChooser indexAnalyzerChooser) {
            return indexAnalyzerChooser;
        }
    }
}
