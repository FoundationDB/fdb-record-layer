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
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link TextTokenizer}.
 */
public class TextTokenizerTest {
    private TextTokenizer defaultTokenizer = DefaultTextTokenizer.instance();
    private TextTokenizer prefixTokenizer = PrefixTextTokenizer.instance();
    private TextTokenizer uniqueLimitTokenizer = UniqueTokenLimitTextTokenizer.instance();

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

    private static final List<List<String>> EXPECTED_DEFAULT_SAMPLE_TOKENS = Arrays.asList(
            Arrays.asList("the", "angstrom", "unit", "a", "was", "named", "after", "anders", "angstrom"),
            Arrays.asList("according", "to", "the", "encyclopædia", "æthelred", "the", "unræd", "was", "king", "from", "966", "to", "1016"), // note that æ is together
            Arrays.asList("hello", "there"),
            Arrays.asList("苹果园区"), // Note that this is making no effort to break apart CJK sequences
            Arrays.asList("蘋果園區"),
            Arrays.asList("text", "tokenization", "and", "normalization", "is"), // the emojis were removed!
            Arrays.asList("who", "started", "the", "fire"),
            Arrays.asList("apres", "deux", "napoleons", "france", "a", "recu", "un", "thiers"),
            Arrays.asList("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen", "horte", "ich", "wahrend", "ich", "die", "friedrichstraße", "hinunterlief"), // note that ß is still linked and that compound words are kept
            Arrays.asList("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι", "ο", "σωκρατης", "ειναι", "ανθρωπος", "ο", "σωκρατης", "ειναι", "θνητος"), // note the lack of stress marks and that final sigmas are still final for some reason
            Arrays.asList("나는", "한국어를", "못해"), // hard to tell from here, but the Hangul here is encoded in Jamo
            Arrays.asList("נון"), // note the lack of dagesh but final nun remains final
            Arrays.asList("english", "used", "to", "have", "multiple", "versions", "of", "the", "letter", "s"), // note that older s's have been modernized
            Arrays.asList("two", "households", "both", "alike", "in", "dignity", "in", "fair", "verona", "where", "we", "lay", "our", "scene", "from", "ancient", "grudge", "break", "to", "new", "mutiny", "where", "civil", "blood", "makes", "civil", "hands", "unclean", "from", "forth", "the", "fatal", "loins", "of", "these", "two", "foes", "a", "pair", "of", "star-cross", "d", "lovers", "take", "their", "life", "whose", "misadventur", "d", "piteous", "overthrows", "doth", "with", "their", "death", "bury", "their", "parents", "strife", "the", "fearful", "passage", "of", "their", "death-mark", "d", "love", "and", "the", "continuance", "of", "their", "parents", "rage", "which", "but", "their", "children", "s", "end", "nought", "could", "remove", "is", "now", "the", "two", "hours", "traffic", "of", "our", "stage", "the", "which", "if", "you", "with", "patient", "ears", "attend", "what", "here", "shall", "miss", "our", "toil", "shall", "strive", "to", "mend"),
            Arrays.asList("актер", "посетил", "многие", "достопримечательности", "москвы"), // note the lack of stress marks
            Arrays.asList("లద", "అద", "అరధలనదన"), // note that combining vowels have (perhaps erroneously) been removed
            Arrays.asList("การสะกดการนตไทยมความซบซอนมาก"), // note that no attempt was made to break the words apart
            Arrays.asList("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"), // note that this UUID is still in tact
            Arrays.asList("א", "שפראך", "איז", "א", "דיאלעקט", "מיט", "אן", "ארמיי", "און", "פלאט") // removing niqqud is arguably wrong for Yiddish, but it is correct for Hebrew (as are removing harakat from Arabic)
    );

    private static final List<List<String>> EXPECTED_PREFIX_V0_SAMPLE_TOKENS = Arrays.asList(
            Arrays.asList("the", "ang", "uni", "a", "was", "nam", "aft", "and", "ang"),
            Arrays.asList("acc", "to", "the", "enc", "æth", "the", "unr", "was", "kin", "fro", "966", "to", "101"),
            Arrays.asList("hel", "the"),
            Arrays.asList("苹果园"),
            Arrays.asList("蘋果園"),
            Arrays.asList("tex", "tok", "and", "nor", "is"),
            Arrays.asList("who", "sta", "the", "fir"),
            Arrays.asList("apr", "deu", "nap", "fra", "a", "rec", "un", "thi"),
            Arrays.asList("die", "nat", "hat", "die", "wel", "gew", "hor", "ich", "wah", "ich", "die", "fri", "hin"),
            Arrays.asList("ολο", "οι", "ανθ", "ειν", "θνη", "ο", "σωκ", "ειν", "ανθ", "ο", "σωκ", "ειν", "θνη"),
            Arrays.asList("나ᄂ", "한", "못"),
            Arrays.asList("נון"),
            Arrays.asList("eng", "use", "to", "hav", "mul", "ver", "of", "the", "let", "s"),
            Arrays.asList("two", "hou", "bot", "ali", "in", "dig", "in", "fai", "ver", "whe", "we", "lay", "our", "sce", "fro", "anc", "gru", "bre", "to", "new", "mut", "whe", "civ", "blo", "mak", "civ", "han", "unc", "fro", "for", "the", "fat", "loi", "of", "the", "two", "foe", "a", "pai", "of", "sta", "d", "lov", "tak", "the", "lif", "who", "mis", "d", "pit", "ove", "dot", "wit", "the", "dea", "bur", "the", "par", "str", "the", "fea", "pas", "of", "the", "dea", "d", "lov", "and", "the", "con", "of", "the", "par", "rag", "whi", "but", "the", "chi", "s", "end", "nou", "cou", "rem", "is", "now", "the", "two", "hou", "tra", "of", "our", "sta", "the", "whi", "if", "you", "wit", "pat", "ear", "att", "wha", "her", "sha", "mis", "our", "toi", "sha", "str", "to", "men"),
            Arrays.asList("акт", "пос", "мно", "дос", "мос"),
            Arrays.asList("లద", "అద", "అరధ"),
            Arrays.asList("การ"),
            Arrays.asList("htt", "www", "fak", "193", "hm"),
            Arrays.asList("א", "שפר", "איז", "א", "דיא", "מיט", "אן", "ארמ", "און", "פלא")
    );

    private static final List<List<String>> EXPECTED_PREFIX_V1_SAMPLE_TOKENS = Arrays.asList(
            Arrays.asList("the", "angs", "unit", "a", "was", "name", "afte", "ande", "angs"),
            Arrays.asList("acco", "to", "the", "ency", "æthe", "the", "unræ", "was", "king", "from", "966", "to", "1016"),
            Arrays.asList("hell", "ther"),
            Arrays.asList("苹果园区"),
            Arrays.asList("蘋果園區"),
            Arrays.asList("text", "toke", "and", "norm", "is"),
            Arrays.asList("who", "star", "the", "fire"),
            Arrays.asList("apre", "deux", "napo", "fran", "a", "recu", "un", "thie"),
            Arrays.asList("die", "nati", "hat", "die", "welt", "gewo", "hort", "ich", "wahr", "ich", "die", "frie", "hinu"),
            Arrays.asList("ολοι", "οι", "ανθρ", "εινα", "θνητ", "ο", "σωκρ", "εινα", "ανθρ", "ο", "σωκρ", "εινα", "θνητ"),
            Arrays.asList("나느", "한ᄀ", "못ᄒ"),
            Arrays.asList("נון"),
            Arrays.asList("engl", "used", "to", "have", "mult", "vers", "of", "the", "lett", "s"),
            Arrays.asList("two", "hous", "both", "alik", "in", "dign", "in", "fair", "vero", "wher", "we", "lay", "our", "scen", "from", "anci", "grud", "brea", "to", "new", "muti", "wher", "civi", "bloo", "make", "civi", "hand", "uncl", "from", "fort", "the", "fata", "loin", "of", "thes", "two", "foes", "a", "pair", "of", "star", "d", "love", "take", "thei", "life", "whos", "misa", "d", "pite", "over", "doth", "with", "thei", "deat", "bury", "thei", "pare", "stri", "the", "fear", "pass", "of", "thei", "deat", "d", "love", "and", "the", "cont", "of", "thei", "pare", "rage", "whic", "but", "thei", "chil", "s", "end", "noug", "coul", "remo", "is", "now", "the", "two", "hour", "traf", "of", "our", "stag", "the", "whic", "if", "you", "with", "pati", "ears", "atte", "what", "here", "shal", "miss", "our", "toil", "shal", "stri", "to", "mend"),
            Arrays.asList("акте", "посе", "мног", "дост", "моск"),
            Arrays.asList("లద", "అద", "అరధల"),
            Arrays.asList("การส"),
            Arrays.asList("http", "www.", "fake", "1932", "hm"),
            Arrays.asList("א", "שפרא", "איז", "א", "דיאל", "מיט", "אן", "ארמי", "און", "פלאט")
    );

    private static final List<List<String>> EXPECTED_FILTERED_SAMPLE_TOKENS = Arrays.asList(
            Arrays.asList("", "angstrom", "unit", "", "was", "named", "after", "anders", "angstrom"),
            Arrays.asList("according", "to", "", "encyclopædia", "æthelred", "", "unræd", "was", "king", "from", "966", "to", "1016"),
            Arrays.asList("hello", "there"),
            Arrays.asList("苹果园区"),
            Arrays.asList("蘋果園區"),
            Arrays.asList("text", "tokenization", "", "normalization", "is"),
            Arrays.asList("who", "started", "", "fire"),
            Arrays.asList("apres", "deux", "napoleons", "france", "", "recu", "un", "thiers"),
            Arrays.asList("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen", "horte", "ich", "wahrend", "ich", "die", "friedrichstraße", "hinunterlief"),
            Arrays.asList("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι", "ο", "σωκρατης", "ειναι", "ανθρωπος", "ο", "σωκρατης", "ειναι", "θνητος"),
            Arrays.asList("나는", "한국어를", "못해"),
            Arrays.asList("נון"),
            Arrays.asList("english", "used", "to", "have", "multiple", "versions", "", "", "letter", "s"),
            Arrays.asList("two", "households", "both", "alike", "", "dignity", "", "fair", "verona", "where", "we", "lay", "our", "scene", "from", "ancient", "grudge", "break", "to", "new", "mutiny", "where", "civil", "blood", "makes", "civil", "hands", "unclean", "from", "forth", "", "fatal", "loins", "", "these", "two", "foes", "", "pair", "", "star-cross", "d", "lovers", "take", "their", "life", "whose", "misadventur", "d", "piteous", "overthrows", "doth", "with", "their", "death", "bury", "their", "parents", "strife", "", "fearful", "passage", "", "their", "death-mark", "d", "love", "", "", "continuance", "", "their", "parents", "rage", "which", "but", "their", "children", "s", "end", "nought", "could", "remove", "is", "now", "", "two", "hours", "traffic", "", "our", "stage", "", "which", "if", "you", "with", "patient", "ears", "attend", "what", "here", "shall", "miss", "our", "toil", "shall", "strive", "to", "mend"),
            Arrays.asList("актер", "посетил", "многие", "достопримечательности", "москвы"),
            Arrays.asList("లద", "అద", "అరధలనదన"),
            Arrays.asList("การสะกดการนตไทยมความซบซอนมาก"),
            Arrays.asList("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"),
            Arrays.asList("א", "שפראך", "איז", "א", "דיאלעקט", "מיט", "אן", "ארמיי", "און", "פלאט")
    );

    private static final List<List<String>> EXPECTED_RECONSTITUTED_UNIQUE_LIMIT_SAMPLE_TOKENS = Arrays.asList(
            Arrays.asList("the", "angstrom", "unit", "a", "was"),
            Arrays.asList("according", "to", "the", "encyclopædia", "æthelred", "the"),
            Arrays.asList("hello", "there"),
            Arrays.asList("苹果园区"),
            Arrays.asList("蘋果園區"),
            Arrays.asList("text", "tokenization", "and", "normalization", "is"),
            Arrays.asList("who", "started", "the", "fire"),
            Arrays.asList("apres", "deux", "napoleons", "france", "a"),
            Arrays.asList("die", "nationalmannschaft", "hat", "die", "weltmeisterschaft", "gewonnen"),
            Arrays.asList("ολοι", "οι", "ανθρωποι", "ειναι", "θνητοι"),
            Arrays.asList("나는", "한국어를", "못해"),
            Arrays.asList("נון"),
            Arrays.asList("english", "used", "to", "have", "multiple"),
            Arrays.asList("two", "households", "both", "alike", "in"),
            Arrays.asList("актер", "посетил", "многие", "достопримечательности", "москвы"),
            Arrays.asList("లద", "అద", "అరధలనదన"),
            Arrays.asList("การสะกดการนตไทยมความซบซอนมาก"),
            Arrays.asList("https", "www.example.com", "fake-path", "1932e32ab3efc0014228eadc28219da2", "hm"),
            Arrays.asList("א", "שפראך", "איז", "א", "דיאלעקט", "מיט")
    );

    public void testTokenization(@Nonnull Function<String, List<String>> tokenizationFunction, @Nonnull List<List<String>> tokenLists) {
        boolean missing = false;
        for (int i = 0; i < TextSamples.ALL.size(); i++) {
            final String text = TextSamples.ALL.get(i);
            List<String> tokens = tokenizationFunction.apply(text);
            if (i < tokenLists.size()) {
                List<String> expected = tokenLists.get(i);
                assertEquals(expected, tokens);
            } else {
                System.out.println("string: " + text);
                System.out.println("tokens: Arrays.asList(\"" + String.join("\", \"", tokens) + "\")");
                missing = true;
            }
        }
        assertFalse(missing);
    }

    public void compatibility(@Nonnull TextTokenizer tokenizer, int version, @Nonnull List<List<String>> tokenLists) {
        testTokenization(text -> tokenList(tokenizer, text, version), tokenLists);
    }

    public void reconstituted(@Nonnull TextTokenizer tokenizer, int version, @Nonnull List<List<String>> tokenLists) {
        testTokenization(text -> reconstitutedTokenList(tokenizer, text, version), tokenLists);
    }

    @Test
    public void defaultTokenizer() {
        compatibility(defaultTokenizer, defaultTokenizer.getMinVersion(), EXPECTED_DEFAULT_SAMPLE_TOKENS);
        reconstituted(defaultTokenizer, defaultTokenizer.getMinVersion(), EXPECTED_DEFAULT_SAMPLE_TOKENS);
    }

    @Test
    public void prefixV0() {
        compatibility(prefixTokenizer, TextTokenizer.GLOBAL_MIN_VERSION, EXPECTED_PREFIX_V0_SAMPLE_TOKENS);
        reconstituted(prefixTokenizer, TextTokenizer.GLOBAL_MIN_VERSION, EXPECTED_PREFIX_V0_SAMPLE_TOKENS);
    }

    @Test
    public void prefixV1() {
        compatibility(prefixTokenizer, TextTokenizer.GLOBAL_MIN_VERSION + 1, EXPECTED_PREFIX_V1_SAMPLE_TOKENS);
        reconstituted(prefixTokenizer, TextTokenizer.GLOBAL_MIN_VERSION + 1, EXPECTED_PREFIX_V1_SAMPLE_TOKENS);
    }

    @Test
    public void filtering() {
        final Set<String> stopWords = ImmutableSet.of("the", "of", "in", "and", "a", "an", "some");
        TextTokenizer filteringTokenizer = FilteringTextTokenizer.create(
                "filter_common",
                new DefaultTextTokenizerFactory(),
                (token, version) -> !stopWords.contains(token.toString())
        ).getTokenizer();
        compatibility(filteringTokenizer, defaultTokenizer.getMinVersion(), EXPECTED_FILTERED_SAMPLE_TOKENS);
        reconstituted(filteringTokenizer, defaultTokenizer.getMinVersion(), EXPECTED_FILTERED_SAMPLE_TOKENS);
    }

    @Test
    public void uniqueTokenLimitCompatibility() {
        compatibility(uniqueLimitTokenizer, TextTokenizer.GLOBAL_MIN_VERSION, EXPECTED_DEFAULT_SAMPLE_TOKENS);
        reconstituted(uniqueLimitTokenizer, TextTokenizer.GLOBAL_MIN_VERSION, EXPECTED_RECONSTITUTED_UNIQUE_LIMIT_SAMPLE_TOKENS);
    }
}
