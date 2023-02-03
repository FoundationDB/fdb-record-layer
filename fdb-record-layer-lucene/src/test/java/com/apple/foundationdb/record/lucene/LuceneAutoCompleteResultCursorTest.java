/*
 * LuceneAutoCompleteResultCursorTest.java
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LuceneAutoCompleteResultCursorTest {

    private static Analyzer getTestAnalyzer() {
        return new EnglishAnalyzer();
    }

    private static Stream<Arguments> searchArgs(String[][] matches) {
        // Each match is a 3-tuple of:
        //   0. The text to search through
        //   1. The match results without highlighting
        //   2. The match results with highlighting
        // A value of "null" indicates that the search text was not found
        return Arrays.stream(matches).flatMap(match -> Stream.of(
                Arguments.of(false, match[0], match[1]),
                Arguments.of(true, match[0], match[2])
        ));
    }

    // Auto-complete searches for the prefix "qua"
    private static final String[][] QUA_MATCHES = {
            {"quality", "quality", "<b>qua</b>lity"},
            {"The basic qualia of objects", "The basic qualia of objects", "The basic <b>qua</b>lia of objects"},
            {"Quality over quantity!", "Quality over quantity!", "<b>Qua</b>lity over <b>qua</b>ntity!"},
            {"quorum logic", null, null},
            {"square", null, null},
            {"example qua example", "example qua example", "example <b>qua</b> example"},
    };

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForQua() {
        return searchArgs(QUA_MATCHES);
    }

    @ParameterizedTest(name = "searchForQua[highlight={0},text={1}]")
    @MethodSource
    void searchForQua(boolean highlight, String text, String expected) throws IOException {
        assertSearchMatches("qua", Collections.emptyList(), "qua", highlight, text, expected);
    }

    // Auto-complete searches for the phrase "good mor" (with a prefix search on "mor")
    private static final String[][] GOOD_MOR_MATCHES = {
            {"Good morning!", "Good morning!", "<b>Good</b> <b>mor</b>ning!"},
            {"It is all for the good, and I'll see you on the morrow", "It is all for the good, and I'll see you on the morrow", "It is all for the <b>good</b>, and I'll see you on the <b>mor</b>row"},
            {"The more good we do, the more good we see", "The more good we do, the more good we see", "The <b>mor</b>e <b>good</b> we do, the <b>mor</b>e <b>good</b> we see"},
            {"Good day!", null, null},
            {"Morning!", null, null},
    };

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForGoodMor() {
        return searchArgs(GOOD_MOR_MATCHES);
    }

    @ParameterizedTest(name = "searchForGoodMor[highlight={0},text={1}]")
    @MethodSource
    void searchForGoodMor(boolean highlight, String text, String expected) throws IOException {
        assertSearchMatches("Good mor", List.of("good"), "mor", highlight, text, expected);
    }

    // Auto-complete searches for the phrase "hello world " (ending space intentional--indicates "world" is not a prefix search)
    private static final String[][] HELLO_WORLD_MATCHES = {
            {"Hello, world!", "Hello, world!", "<b>Hello</b>, <b>world</b>!"},
            {"Hello, worldlings!", null, null},
            {"World--hello!", "World--hello!", "<b>World</b>--<b>hello</b>!"},
            {"Worldly--hello!", null, null},
    };

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForHelloWorld() {
        return searchArgs(HELLO_WORLD_MATCHES);
    }

    @ParameterizedTest(name = "searchForHelloWorld[highlight={0},text={1}]")
    @MethodSource
    void searchForHelloWorld(boolean highlight, String text, String expected) throws IOException {
        assertSearchMatches("Hello World ", List.of("hello", "world"), null, highlight, text, expected);
    }

    private static void assertSearchMatches(String queryString, List<String> expectedTokens, @Nullable String expectedPrefixToken,
                                            boolean highlight, String text, @Nullable String expectedMatch) throws IOException {
        final Analyzer analyzer = getTestAnalyzer();

        List<String> tokens = new ArrayList<>();
        String prefixToken = LuceneAutoCompleteResultCursor.getQueryTokens(analyzer, queryString, tokens);
        assertEquals(expectedTokens, tokens);
        assertEquals(expectedPrefixToken, prefixToken);

        Set<String> queryTokenSet = new HashSet<>(tokens);
        @Nullable String match = LuceneHighlighting.searchAllMaybeHighlight("text", analyzer, text, queryTokenSet, prefixToken == null ? Collections.emptySet() : Collections.singleton(prefixToken), true,
                new LuceneScanQueryParameters.LuceneQueryHighlightParameters(highlight), null);
        assertEquals(expectedMatch, match);
    }
}
