/*
 * LuceneAutoCompleteAnalyzerFactory.java
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
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/**
 * Factory to build index and query {@link org.apache.lucene.analysis.Analyzer} for auto-complete suggestions.
 */
@AutoService(LuceneAnalyzerFactory.class)
public class LuceneAutoCompleteAnalyzerFactory implements LuceneAnalyzerFactory {
    public static final String ANALYZER_FACTORY_NAME = "AUTO_COMPLETE_DEFAULT";

    @Nonnull
    @Override
    public String getName() {
        return ANALYZER_FACTORY_NAME;
    }

    @Nonnull
    @Override
    public LuceneAnalyzerType getType() {
        return LuceneAnalyzerType.AUTO_COMPLETE;
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    @Override
    public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index index) {
        return LuceneAnalyzerWrapper::getStandardAnalyzerWrapper;
    }
}
