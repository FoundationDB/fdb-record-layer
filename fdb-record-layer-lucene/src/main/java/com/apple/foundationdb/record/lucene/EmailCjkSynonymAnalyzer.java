/*
 * EmailCjkSynonymAnalyzer.java
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
import com.apple.foundationdb.record.lucene.filter.AlphanumericLengthFilter;
import com.apple.foundationdb.record.lucene.filter.CjkUnigramFilter;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizerFactory;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

/**
 * An analyzer that can handle emails, CJK, and synonyms. It essentially combines UAX29URLEmailAnalyzer, CJKUnigramFilter,
 * and SynonymGraphFilter.
 */

public class EmailCjkSynonymAnalyzer extends StopwordAnalyzerBase {

    public static final String UNIQUE_IDENTIFIER = "SYNONYM_EMAIL";

    private final int minAlphanumericTokenLength;
    private int minTokenLength;
    private int maxTokenLength;
    private final boolean withEmailTokenizer;
    private final boolean withSynonymGraphFilter;
    @Nullable
    private final SynonymMap synonymMap;

    public EmailCjkSynonymAnalyzer(@Nonnull CharArraySet stopwords, int minTokenLength, int minAlphanumericTokenLength, int maxTokenLength,
                                   boolean withEmailTokenizer,
                                   boolean withSynonymGraphFilter, @Nullable SynonymMap synonymMap) {
        super(stopwords);
        this.minTokenLength = minTokenLength;
        this.maxTokenLength = maxTokenLength;
        this.withEmailTokenizer = withEmailTokenizer;
        this.withSynonymGraphFilter = withSynonymGraphFilter;
        this.minAlphanumericTokenLength = minAlphanumericTokenLength;
        this.synonymMap = synonymMap;
    }

    @Deprecated // due to the use of deprecated WordDelimiterFilter
    @Override
    @SuppressWarnings("PMD.CloseResource") //intentional pattern of lucene
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer src;
        TokenStream result;
        if (withEmailTokenizer()) {
            src = new UAX29URLEmailTokenizerFactory(Collections.emptyMap()).create(attributeFactory(fieldName));
            result = new CJKWidthFilter(src);
            result = new CjkUnigramFilter(result);
            result = new WordDelimiterFilter(result, WordDelimiterFilter.GENERATE_WORD_PARTS | WordDelimiterFilter.PRESERVE_ORIGINAL, null);
            result = new org.apache.lucene.analysis.core.LowerCaseFilter(result);
            result = new ASCIIFoldingFilter(result);
        } else {
            src = new StandardTokenizer();
            result = new CJKWidthFilter(src);
            result = new org.apache.lucene.analysis.core.LowerCaseFilter(result);
            result = new ASCIIFoldingFilter(result);
            result = new CjkUnigramFilter(result);
        }
        result = new AlphanumericLengthFilter(result, minAlphanumericTokenLength, getMaxTokenLength());
        if (withSynonymGraphFilter()) {
            result = new SynonymGraphFilter(result, getSynonymMap(), true);
        }
        result = new StopFilter(result, stopwords);
        return new TokenStreamComponents(src, result);
    }

    @Override
    protected TokenStream normalize(final String fieldName, final TokenStream in) {
        return new LowerCaseFilter(new CJKWidthFilter(in));
    }

    public int getMinTokenLength() {
        return minTokenLength;
    }

    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    protected boolean withSynonymGraphFilter() {
        return withSynonymGraphFilter;
    }

    protected boolean withEmailTokenizer() {
        return withEmailTokenizer;
    }

    @Nonnull
    protected SynonymMap getSynonymMap() {
        if (!withSynonymGraphFilter || synonymMap == null) {
            throw new RecordCoreException("Invalid call to get synonym map for EmailCjkSynonymAnalyzer, which is not enabled with synonym and valid map");
        }
        return synonymMap;
    }
}
