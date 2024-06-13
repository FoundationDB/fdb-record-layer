/*
 * AutoCompleteAnalyzer.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.lucene.filter.CjkUnigramFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;

/**
 * An analyzer that is used to analyze the auto_complete input.
 * Essentially, this analyzer combines {@link org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer} and {@link CjkUnigramFilter}
 * to apply relatively minimal transformation on the text.
 * */
public class AutoCompleteAnalyzer extends StopwordAnalyzerBase {

    public static final String UNIQUE_IDENTIFIER = "AUTO_COMPLETE";

    public AutoCompleteAnalyzer() {
        super(null);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") //intentional pattern of lucene
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer src;
        TokenStream result;
        src = new UAX29URLEmailTokenizer();
        result = new CJKWidthFilter(src);
        result = new CjkUnigramFilter(result);
        result = new LowerCaseFilter(result);
        result = new ASCIIFoldingFilter(result);
        return new TokenStreamComponents(src, result);
    }

    @Override
    protected TokenStream normalize(final String fieldName, final TokenStream in) {
        return new LowerCaseFilter(new CJKWidthFilter(in));
    }
}
