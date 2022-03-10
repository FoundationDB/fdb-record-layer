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

import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.google.auto.service.AutoService;
import org.apache.lucene.analysis.Analyzer;

import javax.annotation.Nonnull;
import java.util.Objects;

@AutoService(LuceneAnalyzerFactory.class)
public class NgramAndSynonymAnalyzerFactory implements LuceneAnalyzerFactory {
    public static final String ANALYZER_NAME = "NGRAM_AND_SYNONYM";

    @Nonnull
    @Override
    public String getName() {
        return ANALYZER_NAME;
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    @Override
    public Analyzer getIndexAnalyzer(@Nonnull Index index) {
        return new NgramAnalyzer.NgramAnalyzerFactory().getIndexAnalyzer(index);
    }

    @Nonnull
    @Override
    public Analyzer getQueryAnalyzer(@Nonnull Index index, @Nonnull Analyzer indexAnalyzer) {
        return new SynonymAnalyzer.SynonymAnalyzerFactory().getQueryAnalyzer(index, indexAnalyzer);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }

        NgramAndSynonymAnalyzerFactory otherFactory = (NgramAndSynonymAnalyzerFactory) other;
        return Objects.equals(this.getName(), otherFactory.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.getName());
    }
}
