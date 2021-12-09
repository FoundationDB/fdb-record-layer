/*
 * SynonymAndNgramIndexAnalyzer.java
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

import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import javax.annotation.Nullable;

public class SynonymAndNgramIndexAnalyzer extends NgramAnalyzer {
    public SynonymAndNgramIndexAnalyzer(@Nullable CharArraySet stopwords, int minTokenLength, int maxTokenLength, boolean edgesOnly) {
        super(stopwords, minTokenLength, maxTokenLength, edgesOnly);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        if (fieldName.startsWith(SynonymAndNgramAnalyzerFactory.NGRAM_FIELD_PREFIX)) {
            final StandardTokenizer src = new StandardTokenizer();
            TokenStream tok = new LowerCaseFilter(src);
            tok = new StopFilter(tok, stopwords);
            tok = edgesOnly ? new EdgeNGramTokenFilter(tok, minTokenLength, maxTokenLength, true)
                            : new NGramTokenFilter(tok, minTokenLength, maxTokenLength, true);
            return new TokenStreamComponents(src, tok);
        } else {
            final StandardTokenizer src = new StandardTokenizer();
            TokenStream tok = new LowerCaseFilter(src);
            tok = new StopFilter(tok, stopwords);
            return new TokenStreamComponents(src, tok);
        }
    }
}
