/*
 * AlphanumericCjkAnalyzer.java
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

import com.apple.foundationdb.record.lucene.filter.AlphanumericLengthFilter;
import com.apple.foundationdb.record.lucene.filter.CjkUnigramFilter;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A CJK Analyzer which applies a minimum and maximum token length to non-CJK tokens. This is useful
 * when anticipating text which has a mixture of CJK and non-CJK tokens in it, as it will ensure that non-CJK
 * tokens will adhere to length limitations, but will ignore CJK tokens during that process.
 */
public class AlphanumericCjkAnalyzer extends StopwordAnalyzerBase {
    public static final int DEFAULT_MIN_TOKEN_LENGTH = 3;

    public static final String UNIQUE_IDENTIFIER = "cjk";

    private int minTokenLength;
    private int maxTokenLength;
    @Nullable
    private final String synonymName;

    //when set to true, this will break tokens which exceed the max token length into smaller ones
    //when set to false, this will ignore tokens which exceed the max token length
    private final boolean breakLongTokens;

    public AlphanumericCjkAnalyzer(@Nonnull CharArraySet stopWords) {
        this(stopWords, null);
    }

    public AlphanumericCjkAnalyzer(@Nonnull CharArraySet stopWords, boolean breakLongTokens) {
        this(stopWords, DEFAULT_MIN_TOKEN_LENGTH, StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH, breakLongTokens, null);
    }

    public AlphanumericCjkAnalyzer(@Nonnull CharArraySet stopWords, @Nullable String synonymName) {
        this(stopWords, DEFAULT_MIN_TOKEN_LENGTH, StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH, true, synonymName);
    }

    public AlphanumericCjkAnalyzer(@Nonnull CharArraySet stopWords,
                                   int minTokenLength,
                                   int maxTokenLength,
                                   @Nullable String synonymName) {
        this(stopWords, minTokenLength, maxTokenLength, true, synonymName);
    }

    /**
     * Create a new {@code AlphanumericCjkAnalyzer}. This has an additional parameter that most analyzers don't have,
     * the {@code minAlphanumericTokenLength}. This is used to filter out any tokens that are too small <em>and</em>
     * consistent only of alphanumeric characters, so CJK unigrams below that length are <em>not</em> filtered
     * out. For example, with a min token length of 1 and a min alphanumeric token length of 3, the string "5人" would
     * get tokenized into a single token, "人", but the string "500人" becomes two tokens, "500" and "人".
     *
     * @param stopWords the stop words to exclude
     * @param minTokenLength the minimum length of any token
     * @param maxTokenLength the maximum token length
     * @param breakLongTokens if true, the long tokens are broken up.
     * @param synonymName the name of the synonym map to use, or {@code null} if no synonyms are to be used
     */
    public AlphanumericCjkAnalyzer(@Nonnull CharArraySet stopWords,
                                   int minTokenLength,
                                   int maxTokenLength,
                                   boolean breakLongTokens,
                                   @Nullable String synonymName) {
        super(stopWords);
        this.breakLongTokens = breakLongTokens;
        this.minTokenLength = minTokenLength;
        this.maxTokenLength = maxTokenLength;
        this.synonymName = synonymName;
    }

    public void setMaxTokenLength(int length) {
        this.maxTokenLength = length;
    }

    public int getMaxTokenLength() {
        return this.maxTokenLength;
    }

    public void setMinTokenLength(int length) {
        this.minTokenLength = length;
    }

    public int getMinTokenLength() {
        return this.minTokenLength;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") //intentional pattern of lucene
    protected Analyzer.TokenStreamComponents createComponents(final String fieldName) {
        final UAX29URLEmailTokenizer src = new UAX29URLEmailTokenizer();
        if (breakLongTokens) {
            src.setMaxTokenLength(maxTokenLength);
        }

        TokenStream result = new CJKWidthFilter(src);
        result = new LowerCaseFilter(result);
        result = new ASCIIFoldingFilter(result);
        result = new CjkUnigramFilter(result);
        result = new AlphanumericLengthFilter(result, minTokenLength, getMaxTokenLength());
        result = new StopFilter(result, stopwords);

        SynonymMap synonymMap = getSynonymMap();
        if (synonymMap != null) {
            result = new SynonymGraphFilter(result, synonymMap, true);
        }
        if (breakLongTokens) {
            return new TokenStreamComponents(r -> {
                src.setMaxTokenLength(maxTokenLength);
                src.setReader(r);
            }, result);
        } else {
            return new TokenStreamComponents(src, result);
        }
    }

    @Override
    protected TokenStream normalize(final String fieldName, final TokenStream in) {
        return new LowerCaseFilter(new CJKWidthFilter(in));
    }

    @Nullable
    private SynonymMap getSynonymMap() {
        if (synonymName == null) {
            return null;
        } else {
            return SynonymMapRegistryImpl.instance().getSynonymMap(synonymName);
        }
    }
}
