/*
 * LuceneAnalyzerWrapper.java
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
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;

import javax.annotation.Nonnull;

/**
 * A wrapper for {@link Analyzer} and its unique identifier.
 */
public class LuceneAnalyzerWrapper {
    public static final String STANDARD_ANALYZER_NAME = "STANDARD";

    private final String uniqueIdentifier;
    private final Analyzer analyzer;

    public LuceneAnalyzerWrapper(@Nonnull String uniqueIdentifier, @Nonnull Analyzer analyzer) {
        this.uniqueIdentifier = uniqueIdentifier;
        this.analyzer = analyzer;
    }

    /**
     * This is different from {@link LuceneAnalyzerFactory#getName()}, which is used as identifier for a factory included in the meta-data.
     * This identifier is to exclusively identify the {@link Analyzer}, which could be different given different texts input.
     * @return this analyzer's unique identifier
     */
    @Nonnull
    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    @Nonnull
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource") //intentional
    public static LuceneAnalyzerWrapper getStandardAnalyzerWrapper() {
        final UAX29URLEmailAnalyzer backingAnalyzer = new UAX29URLEmailAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        backingAnalyzer.setMaxTokenLength(30);
        return new LuceneAnalyzerWrapper(STANDARD_ANALYZER_NAME, backingAnalyzer);
    }
}
