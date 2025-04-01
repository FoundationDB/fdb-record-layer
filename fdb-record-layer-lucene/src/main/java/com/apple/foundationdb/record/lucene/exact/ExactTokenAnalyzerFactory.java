/*
 * ExactTokenAnalyzerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.exact;

import com.apple.foundationdb.record.lucene.AnalyzerChooser;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerType;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.metadata.Index;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/**
 * Constructs a new instance of {@link ExactTokenAnalyzer}.
 */
@AutoService(LuceneAnalyzerFactory.class)
public class ExactTokenAnalyzerFactory implements LuceneAnalyzerFactory  {

    public static final String NAME = "QUERY_ONLY_EXACT_ANALYZER";

    public static final String UNIQUE_NAME = "query_only_exact_analyzer";


    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public LuceneAnalyzerType getType() {
        return LuceneAnalyzerType.FULL_TEXT;
    }

    @Nonnull
    @Override
    public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index ignored) {
        return new AnalyzerChooser() {
            @Nonnull
            @Override
            public LuceneAnalyzerWrapper chooseAnalyzer() {
                return new LuceneAnalyzerWrapper(UNIQUE_NAME, new ExactTokenAnalyzer());
            }
        };
    }

    @Nonnull
    @Override
    public AnalyzerChooser getQueryAnalyzerChooser(@Nonnull Index ignored, @Nonnull AnalyzerChooser alsoIgnored) {
        return new AnalyzerChooser() {
            @Nonnull
            @Override
            public LuceneAnalyzerWrapper chooseAnalyzer() {
                return new LuceneAnalyzerWrapper(UNIQUE_NAME, new ExactTokenAnalyzer());
            }
        };
    }
}
