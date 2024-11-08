/*
 * TextTokenizerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common.text;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link TextTokenizer}.
 */
public class TextTokenizerTest {

    private List<String> tokenList(@Nonnull TextTokenizer tokenizer, @Nonnull String text, int version) {
        return tokenizer.tokenizeToList(text, version, TextTokenizer.TokenizerMode.INDEX);
    }

    // Reconstruct the original token list from the mapping from token to offset
    private List<String> reconstitutedTokenList(@Nonnull TextTokenizer tokenizer, @Nonnull String text, int version) {
        Map<String, List<Integer>> map = tokenizer.tokenizeToMap(text, version, TextTokenizer.TokenizerMode.INDEX);
        int maxOffset = 0;
        for (List<Integer> offsetList : map.values()) {
            maxOffset = Math.max(maxOffset, offsetList.get(offsetList.size() - 1));
        }
        String[] tokenArr = new String[maxOffset + 1];
        Arrays.fill(tokenArr, "");
        for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
            for (int offset : entry.getValue()) {
                tokenArr[offset] = entry.getKey();
            }
        }
        return Arrays.asList(tokenArr);
    }

    private static final List<List<String>> EXPECTED_DEFAULT_SAMPLE_TOKENS = List.of(
            List.of("the", "angstrom", "unit", "a", "was", "named", "after", "anders", "angstrom"),
            List.of("according", "to", "the", "encyclopædia", "æthelred", "the", "unræd", "was", "king", "from", "966", "to", "1016"), // note that æ is together
            List.of("hello", "there"),
            List.of("苹果园区"), // Note that this is making no effort to break apart CJK sequences
            List.of("蘋果園區"),
            List.of("text", "tokenization", "and", "normalization", "is"), // the emojis were removed!
            List.of("who", "started", "the", "fire"),
            List.of("apres", "deux", "napoleons", "france", "a", "recu", "un", "thiers"),
            List.of("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen", "horte", "ich", "wahrend", "ich", "die", "friedrichstraße", "hinunterlief"), // note that ß is still linked and that compound words are kept
            List.of("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι", "ο", "σωκρατης", "ειναι", "ανθρωπος", "ο", "σωκρατης", "ειναι", "θνητος"), // note the lack of stress marks and that final sigmas are still final for some reason
            List.of("나는", "한국어를", "못해"), // hard to tell from here, but the Hangul here is encoded in Jamo
            List.of("נון"), // note the lack of dagesh but final nun remains final
            List.of("english", "used", "to", "have", "multiple", "versions", "of", "the", "letter", "s"), // note that older s's have been modernized
            List.of("two", "households", "both", "alike", "in", "dignity", "in", "fair", "verona", "where", "we", "lay", "our", "scene", "from", "ancient", "grudge", "break", "to", "new", "mutiny", "where", "civil", "blood", "makes", "civil", "hands", "unclean", "from", "forth", "the", "fatal", "loins", "of", "these", "two", "foes", "a", "pair", "of", "star-cross", "d", "lovers", "take", "their", "life", "whose", "misadventur", "d", "piteous", "overthrows", "doth", "with", "their", "death", "bury", "their", "parents", "strife", "the", "fearful", "passage", "of", "their", "death-mark", "d", "love", "and", "the", "continuance", "of", "their", "parents", "rage", "which", "but", "their", "children", "s", "end", "nought", "could", "remove", "is", "now", "the", "two", "hours", "traffic", "of", "our", "stage", "the", "which", "if", "you", "with", "patient", "ears", "attend", "what", "here", "shall", "miss", "our", "toil", "shall", "strive", "to", "mend"),
            List.of("актер", "посетил", "многие", "достопримечательности", "москвы"), // note the lack of stress marks
            List.of("లద", "అద", "అరధలనదన"), // note that combining vowels have (perhaps erroneously) been removed
            List.of("การสะกดการนตไทยมความซบซอนมาก"), // note that no attempt was made to break the words apart
            List.of("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"), // note that this UUID is still in tact
            List.of("א", "שפראך", "איז", "א", "דיאלעקט", "מיט", "אן", "ארמיי", "און", "פלאט") // removing niqqud is arguably wrong for Yiddish, but it is correct for Hebrew (as are removing harakat from Arabic)
    );

    private static final List<List<String>> EXPECTED_PREFIX_V0_SAMPLE_TOKENS = List.of(
            List.of("the", "ang", "uni", "a", "was", "nam", "aft", "and", "ang"),
            List.of("acc", "to", "the", "enc", "æth", "the", "unr", "was", "kin", "fro", "966", "to", "101"),
            List.of("hel", "the"),
            List.of("苹果园"),
            List.of("蘋果園"),
            List.of("tex", "tok", "and", "nor", "is"),
            List.of("who", "sta", "the", "fir"),
            List.of("apr", "deu", "nap", "fra", "a", "rec", "un", "thi"),
            List.of("die", "nat", "hat", "die", "wel", "gew", "hor", "ich", "wah", "ich", "die", "fri", "hin"),
            List.of("ολο", "οι", "ανθ", "ειν", "θνη", "ο", "σωκ", "ειν", "ανθ", "ο", "σωκ", "ειν", "θνη"),
            List.of("나ᄂ", "한", "못"),
            List.of("נון"),
            List.of("eng", "use", "to", "hav", "mul", "ver", "of", "the", "let", "s"),
            List.of("two", "hou", "bot", "ali", "in", "dig", "in", "fai", "ver", "whe", "we", "lay", "our", "sce", "fro", "anc", "gru", "bre", "to", "new", "mut", "whe", "civ", "blo", "mak", "civ", "han", "unc", "fro", "for", "the", "fat", "loi", "of", "the", "two", "foe", "a", "pai", "of", "sta", "d", "lov", "tak", "the", "lif", "who", "mis", "d", "pit", "ove", "dot", "wit", "the", "dea", "bur", "the", "par", "str", "the", "fea", "pas", "of", "the", "dea", "d", "lov", "and", "the", "con", "of", "the", "par", "rag", "whi", "but", "the", "chi", "s", "end", "nou", "cou", "rem", "is", "now", "the", "two", "hou", "tra", "of", "our", "sta", "the", "whi", "if", "you", "wit", "pat", "ear", "att", "wha", "her", "sha", "mis", "our", "toi", "sha", "str", "to", "men"),
            List.of("акт", "пос", "мно", "дос", "мос"),
            List.of("లద", "అద", "అరధ"),
            List.of("การ"),
            List.of("htt", "www", "fak", "193", "hm"),
            List.of("א", "שפר", "איז", "א", "דיא", "מיט", "אן", "ארמ", "און", "פלא")
    );

    private static final List<List<String>> EXPECTED_PREFIX_V1_SAMPLE_TOKENS = List.of(
            List.of("the", "angs", "unit", "a", "was", "name", "afte", "ande", "angs"),
            List.of("acco", "to", "the", "ency", "æthe", "the", "unræ", "was", "king", "from", "966", "to", "1016"),
            List.of("hell", "ther"),
            List.of("苹果园区"),
            List.of("蘋果園區"),
            List.of("text", "toke", "and", "norm", "is"),
            List.of("who", "star", "the", "fire"),
            List.of("apre", "deux", "napo", "fran", "a", "recu", "un", "thie"),
            List.of("die", "nati", "hat", "die", "welt", "gewo", "hort", "ich", "wahr", "ich", "die", "frie", "hinu"),
            List.of("ολοι", "οι", "ανθρ", "εινα", "θνητ", "ο", "σωκρ", "εινα", "ανθρ", "ο", "σωκρ", "εινα", "θνητ"),
            List.of("나느", "한ᄀ", "못ᄒ"),
            List.of("נון"),
            List.of("engl", "used", "to", "have", "mult", "vers", "of", "the", "lett", "s"),
            List.of("two", "hous", "both", "alik", "in", "dign", "in", "fair", "vero", "wher", "we", "lay", "our", "scen", "from", "anci", "grud", "brea", "to", "new", "muti", "wher", "civi", "bloo", "make", "civi", "hand", "uncl", "from", "fort", "the", "fata", "loin", "of", "thes", "two", "foes", "a", "pair", "of", "star", "d", "love", "take", "thei", "life", "whos", "misa", "d", "pite", "over", "doth", "with", "thei", "deat", "bury", "thei", "pare", "stri", "the", "fear", "pass", "of", "thei", "deat", "d", "love", "and", "the", "cont", "of", "thei", "pare", "rage", "whic", "but", "thei", "chil", "s", "end", "noug", "coul", "remo", "is", "now", "the", "two", "hour", "traf", "of", "our", "stag", "the", "whic", "if", "you", "with", "pati", "ears", "atte", "what", "here", "shal", "miss", "our", "toil", "shal", "stri", "to", "mend"),
            List.of("акте", "посе", "мног", "дост", "моск"),
            List.of("లద", "అద", "అరధల"),
            List.of("การส"),
            List.of("http", "www.", "fake", "1932", "hm"),
            List.of("א", "שפרא", "איז", "א", "דיאל", "מיט", "אן", "ארמי", "און", "פלאט")
    );

    private static final List<List<String>> EXPECTED_FILTERED_SAMPLE_TOKENS = List.of(
            List.of("", "angstrom", "unit", "", "was", "named", "after", "anders", "angstrom"),
            List.of("according", "to", "", "encyclopædia", "æthelred", "", "unræd", "was", "king", "from", "966", "to", "1016"),
            List.of("hello", "there"),
            List.of("苹果园区"),
            List.of("蘋果園區"),
            List.of("text", "tokenization", "", "normalization", "is"),
            List.of("who", "started", "", "fire"),
            List.of("apres", "deux", "napoleons", "france", "", "recu", "un", "thiers"),
            List.of("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen", "horte", "ich", "wahrend", "ich", "die", "friedrichstraße", "hinunterlief"),
            List.of("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι", "ο", "σωκρατης", "ειναι", "ανθρωπος", "ο", "σωκρατης", "ειναι", "θνητος"),
            List.of("나는", "한국어를", "못해"),
            List.of("נון"),
            List.of("english", "used", "to", "have", "multiple", "versions", "", "", "letter", "s"),
            List.of("two", "households", "both", "alike", "", "dignity", "", "fair", "verona", "where", "we", "lay", "our", "scene", "from", "ancient", "grudge", "break", "to", "new", "mutiny", "where", "civil", "blood", "makes", "civil", "hands", "unclean", "from", "forth", "", "fatal", "loins", "", "these", "two", "foes", "", "pair", "", "star-cross", "d", "lovers", "take", "their", "life", "whose", "misadventur", "d", "piteous", "overthrows", "doth", "with", "their", "death", "bury", "their", "parents", "strife", "", "fearful", "passage", "", "their", "death-mark", "d", "love", "", "", "continuance", "", "their", "parents", "rage", "which", "but", "their", "children", "s", "end", "nought", "could", "remove", "is", "now", "", "two", "hours", "traffic", "", "our", "stage", "", "which", "if", "you", "with", "patient", "ears", "attend", "what", "here", "shall", "miss", "our", "toil", "shall", "strive", "to", "mend"),
            List.of("актер", "посетил", "многие", "достопримечательности", "москвы"),
            List.of("లద", "అద", "అరధలనదన"),
            List.of("การสะกดการนตไทยมความซบซอนมาก"),
            List.of("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"),
            List.of("א", "שפראך", "איז", "א", "דיאלעקט", "מיט", "אן", "ארמיי", "און", "פלאט")
    );

    private static final List<List<String>> EXPECTED_RECONSTITUTED_UNIQUE_LIMIT_SAMPLE_TOKENS = List.of(
            List.of("the", "angstrom", "unit", "a", "was"),
            List.of("according", "to", "the", "encyclopædia", "æthelred", "the"),
            List.of("hello", "there"),
            List.of("苹果园区"),
            List.of("蘋果園區"),
            List.of("text", "tokenization", "and", "normalization", "is"),
            List.of("who", "started", "the", "fire"),
            List.of("apres", "deux", "napoleons", "france", "a"),
            List.of("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen"),
            List.of("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι"),
            List.of("나는", "한국어를", "못해"),
            List.of("נון"),
            List.of("english", "used", "to", "have", "multiple"),
            List.of("two", "households", "both", "alike", "in"),
            List.of("актер", "посетил", "многие", "достопримечательности", "москвы"),
            List.of("లద", "అద", "అరధలనదన"),
            List.of("การสะกดการนตไทยมความซบซอนมาก"),
            List.of("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"),
            List.of("א", "שפראך", "איז", "א", "דיאלעקט", "מיט")
    );

    private static <T, U, R> Stream<R> zip(final Collection<T> first, final Collection<U> second,
                                           final BiFunction<T, U, R> combiner) {
        final Iterator<T> firstIterator = first.iterator();
        final Iterator<U> secondIterator = second.iterator();
        return StreamSupport.stream(Spliterators.spliterator(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return firstIterator.hasNext() || secondIterator.hasNext();
            }

            @Override
            public R next() {
                final T firstNext = firstIterator.hasNext() ? firstIterator.next() : null;
                final U secondNext = secondIterator.hasNext() ? secondIterator.next() : null;
                return combiner.apply(firstNext, secondNext);
            }
        }, Math.max(first.size(), second.size()), Spliterator.SIZED), false);
    }

    @Nonnull
    private static Stream<Arguments> test(final List<List<String>> expectedTokens,
                                          final TextTokenizer tokenizer, final int globalMinVersion) {
        final Iterator<String> sampleIterator = TextSamples.ALL.iterator();
        final Iterator<List<String>> tokenIterator = expectedTokens.iterator();
        return StreamSupport.stream(Spliterators.spliterator(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return sampleIterator.hasNext() || tokenIterator.hasNext();
            }

            @Override
            public Arguments next() {
                final String sample = sampleIterator.hasNext() ? sampleIterator.next() : null;
                final List<String> tokens = tokenIterator.hasNext() ? tokenIterator.next() : null;
                return Arguments.of(
                        Named.of(tokenizer.getName(), tokenizer),
                        globalMinVersion,
                        sample, tokens);
            }
        }, Math.max(TextSamples.ALL.size(), expectedTokens.size()), Spliterator.SIZED), false);
    }

    static Stream<Arguments> compatibility() {
        final TextTokenizer filteringTokenizer = filteredTokenizer();
        return Stream.of(
                        test(EXPECTED_DEFAULT_SAMPLE_TOKENS, UniqueTokenLimitTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION),
                        test(EXPECTED_DEFAULT_SAMPLE_TOKENS, DefaultTextTokenizer.instance(), DefaultTextTokenizer.instance().getMinVersion()),
                        test(EXPECTED_PREFIX_V0_SAMPLE_TOKENS, PrefixTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION),
                        test(EXPECTED_PREFIX_V1_SAMPLE_TOKENS, PrefixTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION + 1),
                        test(EXPECTED_FILTERED_SAMPLE_TOKENS, filteringTokenizer, DefaultTextTokenizer.instance().getMinVersion()))
                .flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource
    void compatibility(TextTokenizer tokenizer, int version, String text, List<String> tokens) {
        assertEquals(tokens, tokenList(tokenizer, text, version));
    }

    static Stream<Arguments> reconstituted() {
        final TextTokenizer filteringTokenizer = filteredTokenizer();
        return Stream.of(
                        test(EXPECTED_RECONSTITUTED_UNIQUE_LIMIT_SAMPLE_TOKENS, UniqueTokenLimitTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION),
                        test(EXPECTED_DEFAULT_SAMPLE_TOKENS, DefaultTextTokenizer.instance(), DefaultTextTokenizer.instance().getMinVersion()),
                        test(EXPECTED_PREFIX_V0_SAMPLE_TOKENS, PrefixTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION),
                        test(EXPECTED_PREFIX_V1_SAMPLE_TOKENS, PrefixTextTokenizer.instance(), TextTokenizer.GLOBAL_MIN_VERSION + 1),
                        test(EXPECTED_FILTERED_SAMPLE_TOKENS, filteringTokenizer, DefaultTextTokenizer.instance().getMinVersion()))
                .flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource
    void reconstituted(TextTokenizer tokenizer, int version, String text, List<String> tokens) {
        assertEquals(tokens, reconstitutedTokenList(tokenizer, text, version));
    }

    @Nonnull
    private static TextTokenizer filteredTokenizer() {
        final Set<String> stopWords = ImmutableSet.of("the", "of", "in", "and", "a", "an", "some");
        return FilteringTextTokenizer.create(
                "filter_common",
                new DefaultTextTokenizerFactory(),
                (token, version) -> !stopWords.contains(token.toString())
        ).getTokenizer();
    }
}
