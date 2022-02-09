/*
 * LuceneAnalyzerTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class LuceneAnalyzerTest {
    @Test
    void testNgramAnalyzer() throws Exception {
        String input = "hello RL";
        Collection<String> result = new HashSet<>();

        tokenizeWithAnalyzer(result, input, new NgramAnalyzer(null, 3, 10, false));
        Assertions.assertEquals(ImmutableSet.of("hel", "ell", "llo", "hell", "ello", "hello", "rl"), result);
    }

    @Test
    void testEdgesOnlyNgramAnalyzer() throws Exception {
        String input = "hello RL";
        Collection<String> result = new HashSet<>();

        tokenizeWithAnalyzer(result, input, new NgramAnalyzer(null, 3, 10, true));
        Assertions.assertEquals(ImmutableSet.of("hel", "hell", "hello", "rl"), result);
    }

    @Test
    void testNgramAnalyzerWithStopWords() throws Exception {
        final CharArraySet stopSet = new CharArraySet(List.of("hello"), false);
        CharArraySet stopWords = CharArraySet.unmodifiableSet(stopSet);

        String input = "hello RL";
        Collection<String> result = new HashSet<>();

        tokenizeWithAnalyzer(result, input, new NgramAnalyzer(stopWords, 3, 10, false));
        Assertions.assertEquals(ImmutableSet.of("rl"), result);
    }

    @Test
    void testSynonymAnalyzer() throws Exception {
        String input = "Hello RL";
        Collection<String> result = new HashSet<>();

        tokenizeWithAnalyzer(result, input, new SynonymAnalyzer(null, EnglishSynonymMapConfig.CONFIG_NAME));
        Assertions.assertEquals(ImmutableSet.of("hi", "how", "hullo", "howdy", "rl", "hello", "do", "you"), result);
    }

    @Test
    void testStandardAnalyzer() throws Exception {
        String input = "hello RL";
        Collection<String> result = new HashSet<>();

        tokenizeWithAnalyzer(result, input, new StandardAnalyzer());
        Assertions.assertEquals(ImmutableSet.of("hello", "rl"), result);
    }

    private static void tokenizeWithAnalyzer(Collection<String> result, @Nonnull String input, Analyzer analyzer) throws IOException {
        try (TokenStream stream = analyzer.tokenStream("field", new StringReader(input))) {
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
            stream.end();
        }
    }
}
