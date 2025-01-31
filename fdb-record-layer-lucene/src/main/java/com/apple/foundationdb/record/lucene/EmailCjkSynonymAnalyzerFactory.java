/*
 * EmailCjkSynonymAnalyzerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.google.auto.service.AutoService;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Factory to build index and query {@link org.apache.lucene.analysis.Analyzer} for {@link EmailCjkSynonymAnalyzer}.
 */
@AutoService(LuceneAnalyzerFactory.class)
public class EmailCjkSynonymAnalyzerFactory implements LuceneAnalyzerFactory {
    public static final String ANALYZER_FACTORY_NAME = "SYNONYM_EMAIL";
    public static final String UNIQUE_IDENTIFIER = "synonym_email";

    private static final String DEFAULT_MINIMUM_TOKEN_LENGTH = "3";
    private static final String DEFAULT_MAXIMUM_TOKEN_LENGTH = "30";

    public static final CharArraySet MINIMAL_STOP_WORDS = new CharArraySet(List.of("into", "onto", "the"), true);

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
        try {
            final String minLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MIN_SIZE)).orElse(DEFAULT_MINIMUM_TOKEN_LENGTH);
            final String maxLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MAX_SIZE)).orElse(Integer.toString(UAX29URLEmailAnalyzer.DEFAULT_MAX_TOKEN_LENGTH));

            return () -> new LuceneAnalyzerWrapper(UNIQUE_IDENTIFIER,
                    new EmailCjkSynonymAnalyzer(MINIMAL_STOP_WORDS, 1, Integer.parseInt(minLengthString), Integer.parseInt(maxLengthString), true,
                            false, null));
        } catch (NumberFormatException ex) {
            throw new RecordCoreException("Invalid parameter for Lucene analyzer's token length", ex);
        }
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    @Override
    public AnalyzerChooser getQueryAnalyzerChooser(@Nonnull Index index, @Nonnull AnalyzerChooser indexAnalyzerChooser) {
        try {
            final String minLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MIN_SIZE)).orElse(DEFAULT_MINIMUM_TOKEN_LENGTH);
            final String maxLengthString = Optional.ofNullable(index.getOption(IndexOptions.TEXT_TOKEN_MAX_SIZE)).orElse(DEFAULT_MAXIMUM_TOKEN_LENGTH);
            final String synonymConfigName = index.getOption(LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION);
            return () -> new LuceneAnalyzerWrapper(UNIQUE_IDENTIFIER,
                    new EmailCjkSynonymAnalyzer(MINIMAL_STOP_WORDS, 1, Integer.parseInt(minLengthString), Integer.parseInt(maxLengthString), true,
                            synonymConfigName != null, synonymConfigName != null ? SynonymMapRegistryImpl.instance().getSynonymMap(synonymConfigName) : null));
        } catch (NumberFormatException ex) {
            throw new RecordCoreException("Invalid parameter for Lucene analyzer's token length", ex);
        }
    }
}
