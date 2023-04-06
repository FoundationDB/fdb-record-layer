/*
 * LuceneAutoCompletedMatchesTest.java
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LuceneAutoCompletedMatchesTest {
    private static Analyzer getTestAnalyzer() {
        //return new EnglishAnalyzer();
        return new StandardAnalyzer();
    }

    private static Stream<Arguments> searchArgs(String[][] matches) {
        return Arrays.stream(matches).map(match ->
                Arguments.of(match[0], match[1]));
    }

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForQua() {
        return ImmutableList.of(
                Arguments.of("quality", ImmutableList.of("quality")),
                Arguments.of("The basic qualia of objects", ImmutableList.of("qualia")),
                Arguments.of("Quality over quantity!", ImmutableList.of("Quality", "quantity")),
                Arguments.of("quorum logic", ImmutableList.of()),
                Arguments.of("example qua example", ImmutableList.of("qua"))).stream();
    }

    @ParameterizedTest(name = "searchForQua[text={1}]")
    @MethodSource
    void searchForQua(String text, List<String> expected) throws IOException {
        assertComputeAllMatches("qua", Collections.emptyList(), ImmutableSet.of("qua"), text, expected);
    }

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForGoodMor() {
        return ImmutableList.of(
                Arguments.of("Good morning!", ImmutableList.of("Good", "morning")),
                Arguments.of("It is all for the good, and I'll see you on the morrow", ImmutableList.of("good", "morrow")),
                Arguments.of("The more good we do, the more good we see", ImmutableList.of("more", "good", "more", "good")),
                Arguments.of("Good day!", ImmutableList.of("Good")),
                Arguments.of("Morning!", ImmutableList.of("Morning"))).stream();
    }

    @ParameterizedTest(name = "searchForGoodMor[text={0}]")
    @MethodSource
    void searchForGoodMor(String text, List<String> expected) throws IOException {
        assertComputeAllMatches("Good mor", List.of("good"), ImmutableSet.of("mor"), text, expected);
    }
    
    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForHelloWorld() {
        return ImmutableList.of(
                Arguments.of("Hello, world!", ImmutableList.of("Hello", "world")),
                Arguments.of("Hello, worldlings!", ImmutableList.of("Hello")),
                Arguments.of("World--hello!", ImmutableList.of("World", "hello")),
                Arguments.of("Worldly--hello!", ImmutableList.of("hello"))).stream();
    }

    @ParameterizedTest(name = "searchForHelloWorld[text={1}]")
    @MethodSource
    void searchForHelloWorld(String text, List<String> expected) throws IOException {
        assertComputeAllMatches("Hello World ", List.of("hello", "world"), ImmutableSet.of(), text, expected);
    }

    private static void assertComputeAllMatches(@Nonnull final String queryString, @Nonnull final List<String> expectedTokens,
                                                @Nullable final Set<String> expectedPrefixTokens,
                                                @Nonnull final String text,
                                                @Nonnull final List<String> expectedMatches) {
        final Analyzer analyzer = getTestAnalyzer();
        LuceneAutoCompleteHelpers.AutoCompleteTokens tokens = LuceneAutoCompleteHelpers.getQueryTokens(analyzer, queryString);
        assertEquals(expectedTokens, tokens.getQueryTokens());
        assertEquals(expectedPrefixTokens, tokens.getPrefixTokens());
        List<String> match =
                LuceneAutoCompleteHelpers.computeAllMatches("text", analyzer, text, tokens);
        assertEquals(expectedMatches, match);
    }

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForHelloWorldInPhrase() {
        return ImmutableList.of(
                Arguments.of("Hello, world!", ImmutableList.of("Hello", "world")),
                Arguments.of("world, Hello!", ImmutableList.of()),
                Arguments.of("Hello to the entire world!", ImmutableList.of()),
                Arguments.of("Hello to the entire hello, world!", ImmutableList.of("hello", "world")),
                Arguments.of("Hello, hello, world, world!", ImmutableList.of("hello", "world"))).stream();
    }

    @ParameterizedTest(name = "searchForHelloWorldInPhrase[text={1}]")
    @MethodSource
    void searchForHelloWorldInPhrase(final String text, final List<String> expected) throws IOException {
        assertComputeAllMatchesForPhrase("Hello World ", List.of("hello", "world"), null, text, expected);
    }

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> searchForGoodMorInPhrase() {
        return ImmutableList.of(
                Arguments.of("Good Morning!", ImmutableList.of("Good", "Morning")),
                Arguments.of("Good Morrow Morning!", ImmutableList.of("Good", "Morrow")),
                Arguments.of("Morning!", ImmutableList.of()),
                Arguments.of("Morning, Good it is! (Yoda)", ImmutableList.of()),
                Arguments.of("Good, Good, good, more mornings!", ImmutableList.of("good", "more"))).stream();
    }

    @ParameterizedTest(name = "searchForGoodMorInPhrase[text={1}]")
    @MethodSource
    void searchForGoodMorInPhrase(final String text, final List<String> expected) throws IOException {
        assertComputeAllMatchesForPhrase("Good Mor", List.of("good"), "mor", text, expected);
    }

    private static void assertComputeAllMatchesForPhrase(@Nonnull final String queryString,
                                                         @Nonnull final List<String> expectedTokens,
                                                         @Nullable final String expectedPrefixToken,
                                                         @Nonnull final String text,
                                                         @Nonnull final List<String> expectedMatches) {
        final Analyzer analyzer = getTestAnalyzer();
        LuceneAutoCompleteHelpers.AutoCompleteTokens tokens = LuceneAutoCompleteHelpers.getQueryTokens(analyzer, queryString);
        assertEquals(expectedTokens, tokens.getQueryTokens());
        assertEquals(expectedPrefixToken, tokens.getPrefixTokenOrNull());
        List<String> match =
                LuceneAutoCompleteHelpers.computeAllMatchesForPhrase("text", analyzer, text, tokens);
        assertEquals(expectedMatches, match);
    }
}
