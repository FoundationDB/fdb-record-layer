/*
 * LuceneAnalyzerType.java
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

import org.apache.lucene.analysis.Analyzer;

import javax.annotation.Nonnull;

/**
 * The type used to determine how the {@link Analyzer} built by {@link LuceneAnalyzerFactory} is used.
 */
public enum LuceneAnalyzerType {
    FULL_TEXT(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION),
    AUTO_COMPLETE(LuceneIndexOptions.AUTO_COMPLETE_ANALYZER_NAME_OPTION, LuceneIndexOptions.AUTO_COMPLETE_ANALYZER_NAME_PER_FIELD_OPTION);

    private String analyzerOptionKey;
    private String analyzerPerFieldOptionKey;

    LuceneAnalyzerType(@Nonnull String indexOptionKey, @Nonnull String analyzerPerFieldOptionKey) {
        this.analyzerOptionKey = indexOptionKey;
        this.analyzerPerFieldOptionKey = analyzerPerFieldOptionKey;
    }

    @Nonnull
    public String getAnalyzerOptionKey() {
        return analyzerOptionKey;
    }

    @Nonnull
    public String getAnalyzerPerFieldOptionKey() {
        return analyzerPerFieldOptionKey;
    }
}
