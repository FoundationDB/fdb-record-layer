/*
 * LuceneHighlighterTest.java
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

package com.apple.foundationdb.record.lucene.highlight;

import com.apple.foundationdb.record.lucene.AlphanumericCjkAnalyzer;
import com.apple.foundationdb.record.lucene.AlphanumericLengthFilterFactory;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.RegistrySynonymGraphFilterFactory;
import com.apple.foundationdb.record.lucene.Utf8Chars;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.search.LuceneQueryParserFactoryProvider;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.cjk.CJKWidthFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizerFactory;
import org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tests on the text processing of the Lucene Highlight functionality. The intent here is to bypass
 * the expensive query planning process in order to ensure that the highlight matching functionality
 * works as expected.
 * <p>
 * As a side effect of the verification of highlighting, this test also
 * outlines the behavior of the various different analyzers on text matching (so, for example, tests
 * here will fail if a given analyzer fails to match a specific query).
 */
public class LuceneHighlighterTest {


    private static final String ROSE_BY_ANY_OTHER_NAME = "'Tis but thy name that is my enemy;\n" +
                                                         "Thou art thyself, though not a Montague.\n" +
                                                         "What's Montague? It is nor hand, nor foot,\n" +
                                                         "Nor arm, nor face, nor any other part\n" +
                                                         "Belonging to a man. O, be some other name!\n" +
                                                         "What's in a name? That which we call a rose\n" +
                                                         "By any other name would smell as sweet;\n" +
                                                         "So Romeo would, were he not Romeo call'd,\n" +
                                                         "Retain that dear perfection which he owes\n" +
                                                         "Without that title. Romeo, doff thy name,\n" +
                                                         "And for that name which is no part of thee\n" +
                                                         "Take all myself.";

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map, so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    private static final CharArraySet stopWords = EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

    public static Stream<Arguments> analyzers() throws IOException {
        //use a custom analyzer to build an Alphanumeric CJK analyzer
        Analyzer acjkWithSynonyms = CustomAnalyzer.builder()
                .withTokenizer(UAX29URLEmailTokenizerFactory.class, new HashMap<>(Map.of("maxTokenLength", "30")))
                .addTokenFilter(CJKWidthFilterFactory.class)
                .addTokenFilter(LowerCaseFilterFactory.class)
                .addTokenFilter(ASCIIFoldingFilterFactory.class)
                .addTokenFilter(AlphanumericLengthFilterFactory.class, new HashMap<>(Map.of("min", "1", "max", "30")))
                .addTokenFilter(StopFilterFactory.class)
                .addTokenFilter(RegistrySynonymGraphFilterFactory.class, new HashMap<>(Map.of("synonyms", EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME)))
                .build();

        UAX29URLEmailAnalyzer noStopWords = new UAX29URLEmailAnalyzer(CharArraySet.EMPTY_SET);
        noStopWords.setMaxTokenLength(30);
        final Analyzer standardAnalyzer = LuceneAnalyzerWrapper.getStandardAnalyzerWrapper().getAnalyzer();
        final Analyzer acjkNoSynonyms = new AlphanumericCjkAnalyzer(stopWords, 1, 30, true, null);
        return Stream.of(
                Arguments.of("Standard", standardAnalyzer, standardAnalyzer),
                Arguments.of("Synonym", new SynonymAnalyzer(stopWords, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME, 30), standardAnalyzer),
                Arguments.of("Ngram", standardAnalyzer, new NgramAnalyzer(stopWords, 3, 30, false)),
                Arguments.of("AlphaCjk-NoSynonyms", acjkNoSynonyms, acjkNoSynonyms),
                Arguments.of("AlphaCjk-Synonyms", acjkWithSynonyms, acjkNoSynonyms),
                Arguments.of("Standard-noStopWords", noStopWords, noStopWords)
        );
    }

    public static Stream<Arguments> specialCharacterAnalyzerCombinations() throws IOException {
        return analyzers().flatMap(args -> {
            Object[] analyzerArgs = args.get();
            return Utf8Chars.getUnusualTokenizableChars().map(ch -> Arguments.of(analyzerArgs[0], analyzerArgs[1], analyzerArgs[2], ch, Integer.toHexString(ch.codePointAt(0))));
        });
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithNoSnippets(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws IOException { //the first string is the analyzer name
        String text = "Hello record layer";
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:hello", -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {0}, new int[] {5}), result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSynonyms(String ignore, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws IOException { //the first string is the analyzer name
        String text = "apple apple apple apple park apple apple";

        if (isSynonymAnalyzer(queryAnalyzer)) {
            HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:\"malus pumila park\"", -1);
            assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {18, 24}, new int[] {23, 28}), result);
        }

        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:\"apple park\"", -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {18, 24}, new int[] {23, 28}), result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void ngramHighlightsNgrams(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws IOException {
        //only the ngram analyzer will pick these up as a match, all others will treat them as full-word queries
        //and won't return results
        Assumptions.assumeTrue(indexAnalyzer instanceof NgramAnalyzer, "Only care about Ngram analyzers");

        String text = "Hello record layer";
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:hel", -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {0}, new int[] {5}), result);

        result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:cord", -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {6}, new int[] {12}), result);

    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithSnippets(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws IOException { //the first string is the analyzer name
        String text = "Good Morning From Apple News It?s Monday, July 11. Here?s what you need to know. ";

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:appl*");
        String correctString = "Good Morning From Apple News It?s...";
        assertHighlightCorrect(new HighlightedTerm("text", correctString, new int[] {18}, new int[] {23}), result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithWildcardSnippetsInsideHtml(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws IOException {
        String link = "https://apple.news/AgZ7a_IT4TpKFF3kAyZsvUg";
        String text = "Good Morning From Apple News It?s Monday, July 11. Here?s what you need to know. " +
                      "Top Stories Former Trump adviser Steve Bannon agreed to testify to the January 6 committee " +
                      "after months of defying a congressional subpoena. The Washington Post " + link + "?";

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:*appl*");
        String correctString = "Good Morning From Apple News It?s...The Washington Post " + link + "?";
        /*
         * If the length of the link exceeds the token limit of the analyzer, then the returned highlight
         * element will have a different end point
         */
        int maxTokenLength = getMaxTokenSize(indexAnalyzer);
        if (maxTokenLength < 0) {
            int[] starts = new int[] {18, 56};
            int[] ends = new int[] {23, 99};
            assertHighlightCorrect(new HighlightedTerm("text", correctString, starts, ends), result);
        } else {
            int[] starts = new int[] {18, 56};
            int[] ends = new int[] {23, Math.min(starts[1] + maxTokenLength, 99)};
            assertHighlightCorrect(new HighlightedTerm("text", correctString, starts, ends), result);
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightTextWithMultipleTermMatches(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, ROSE_BY_ANY_OTHER_NAME, "text:name");
        String correctString;
        HighlightedTerm expected;
        if (isSynonymAnalyzer(queryAnalyzer)) {
            //"call" is a synonym for "name", so we have to catch that as well
            correctString = "'Tis but thy name that is my...be some other name!\nWhat's in a name? " +
                            "That which we call a rose\nBy any other name would smell as..., doff thy name,\n" +
                            "And for that name which is no...";
            expected = new HighlightedTerm("text", correctString, new int[] {13, 45, 63, 83, 108, 141, 160}, new int[] {17, 49, 67, 87, 112, 145, 164});
        } else {
            correctString = "'Tis but thy name that is my...be some other name!\nWhat's in a name? " +
                            "That which...By any other name would smell as..., doff thy name,\n" +
                            "And for that name which is no...";
            expected = new HighlightedTerm("text", correctString, new int[] {13, 45, 63, 95, 128, 147}, new int[] {17, 49, 67, 99, 132, 151});
        }
        assertHighlightCorrect(expected, result);
    }


    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightMultiplePrefixMatches(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, ROSE_BY_ANY_OTHER_NAME, "text:monta*");
        String correctString = "...though not a Montague.\nWhat's Montague? It is...";
        assertHighlightCorrect(new HighlightedTerm("text", correctString, new int[] {16, 33}, new int[] {24, 41}), result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightLinesUp(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * A test to ensure that every analyzer's start and end points actually line up by automatically
         * checking the string. This is a slight variation on the other tests, where the offsets were manually
         * computed.
         */
        final String text = "record record record record record record " +
                            "layer " +
                            "record record record record record record record record record record " +
                            "layer " +
                            "record record " +
                            "layer " +
                            "record record record record record record record record";

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:layer", 4);
        Assertions.assertEquals(3, result.getNumHighlights(), "Incorrect number of highlights!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String highlightedText = result.getSummarizedText().substring(s, e);
            Assertions.assertEquals("layer", highlightedText, "Incorrect highlight!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInTermQueries(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Email addresses should be tokenized as a single token. However, The NgramAnalyzer tokenized
         */
        final String text = "Date: Thu, 10 Mar 2022 02:21:41 -0800 (PST)\n" +
                            "From: Jamison Gisele Lilia Bank Alerts <alerts@JamisonGiseleLilia.com>\n" +
                            "To: jannette.rawhouser@demo.org\n" +
                            "Bcc: bcc70@apple.com\n" +
                            "Message-ID: <659260106.1.1646907701959@localhost>\n" +
                            "Subject: much out.  And we 'll do it\n" +
                            "MIME-Version: 1.0\n" +
                            "Content-Type: multipart/mixed;\n" +
                            "        boundary=\"----=_Part_0_247087736.1646907701951\"\n" +
                            "\n" +
                            "------=_Part_0_247087736.1646907701951\n" +
                            "Content-Type: text/plain; charset=us-ascii\n" +
                            "Content-Transfer-Encoding: 7bit";

        final HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:bcc70@apple.com", 4);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com", result.getSummarizedText().substring(s, e), "Incorrect highlight positions!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInPrefixQueries(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Email addresses should be tokenized as a single token. However, The NgramAnalyzer tokenized
         */
        final String text = "Date: Thu, 10 Mar 2022 02:21:41 -0800 (PST)\n" +
                            "From: Jamison Gisele Lilia Bank Alerts <alerts@JamisonGiseleLilia.com>\n" +
                            "To: jannette.rawhouser@demo.org\n" +
                            "Bcc: bcc70@apple.com\n" +
                            "Message-ID: <659260106.1.1646907701959@localhost>\n" +
                            "Subject: much out.  And we 'll do it\n" +
                            "MIME-Version: 1.0\n" +
                            "Content-Type: multipart/mixed;\n" +
                            "        boundary=\"----=_Part_0_247087736.1646907701951\"\n" +
                            "\n" +
                            "------=_Part_0_247087736.1646907701951\n" +
                            "Content-Type: text/plain; charset=us-ascii\n" +
                            "Content-Transfer-Encoding: 7bit";

        //prefix the entire email
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:bcc70@apple.com*", 4);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com", result.getSummarizedText().substring(s, e), "Incorrect highlight positions!");
        }

        //prefix the part before the @
        result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:bcc70*", 4);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com", result.getSummarizedText().substring(s, e), "Incorrect highlight positions!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInWildcardQueries(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Email addresses should be tokenized as a single token. However, The NgramAnalyzer tokenized
         */
        final String text = "Date: Thu, 10 Mar 2022 02:21:41 -0800 (PST)\n" +
                            "From: Jamison Gisele Lilia Bank Alerts <alerts@JamisonGiseleLilia.com>\n" +
                            "To: jannette.rawhouser@demo.org\n" +
                            "Bcc: bcc70@apple.com\n" +
                            "Message-ID: <659260106.1.1646907701959@localhost>\n" +
                            "Subject: much out.  And we 'll do it\n" +
                            "MIME-Version: 1.0\n" +
                            "Content-Type: multipart/mixed;\n" +
                            "        boundary=\"----=_Part_0_247087736.1646907701951\"\n" +
                            "\n" +
                            "------=_Part_0_247087736.1646907701951\n" +
                            "Content-Type: text/plain; charset=us-ascii\n" +
                            "Content-Transfer-Encoding: 7bit";

        //prefix the entire email
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:bcc70@a*e.com", 4);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com", result.getSummarizedText().substring(s, e), "Incorrect highlight positions!");
        }
    }

    @Nonnull
    private String specialCharacterText(@Nonnull String specialCharacter) {
        return "Do we match special characters like " + specialCharacter + " even when its mashed together like " + specialCharacter + "noSpaces?";
    }


    @MethodSource("specialCharacterAnalyzerCombinations")
    @ParameterizedTest(name = "{0},{3}")
    void highlightsSpecialCharacterPrefixSearch(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer, String specialCharacter) throws Exception {
        String text = specialCharacterText(specialCharacter);

        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:" + specialCharacter.toLowerCase(Locale.ROOT) + "*", 1);
        Assertions.assertEquals(2, result.getNumHighlights(), "Incorrect number of highlights!");
        Assertions.assertEquals("...like " + specialCharacter + " even...like " + specialCharacter + "noSpaces?", result.getSummarizedText(), "Incorrect summary string!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String subStr = result.getSummarizedText().substring(s, e);
            if (!subStr.equals(specialCharacter)) {
                Assertions.assertEquals(specialCharacter + "noSpaces", subStr, "Incorrect Highlight positions!");
            }
        }
    }

    @MethodSource("specialCharacterAnalyzerCombinations")
    @ParameterizedTest(name = "{0},{3}")
    void highlightsSpecialCharacterTerm(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer, String specialCharacter) throws Exception {
        String text = specialCharacterText(specialCharacter);

        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:" + specialCharacter.toLowerCase(Locale.ROOT), 1);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        Assertions.assertEquals("...like " + specialCharacter + " even...", result.getSummarizedText(), "Incorrect summary string!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String subStr = result.getSummarizedText().substring(s, e);
            Assertions.assertEquals(specialCharacter, subStr, "Incorrect highlight value!");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("analyzers")
    void highlightsReallyLongTermsTermQuery(String name, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Checks the behavior of highlighting when given really long term queries
         */
        String longTerm = "reallyLongTermWhichTakesHundredsAndHundredsNoIMeanItLotsAndLotsOfCharactersSoItWillExceedTheLimitOfOurConfigurations";
        String text = "This is a " + longTerm + "  I think, but maybe not also because things are weird";

        int maxTokenSize = getMaxTokenSize(queryAnalyzer);
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:" + longTerm.toLowerCase(Locale.ROOT).substring(0, maxTokenSize - 1) + "*", 1);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        if ("Ngram".equals(name)) {
            Assertions.assertEquals("...a " + longTerm + "  ...", result.getSummarizedText(), "Incorrect summary string!");
            for (int i = 0; i < result.getNumHighlights(); i++) {
                int s = result.getHighlightStart(i);
                int e = result.getHighlightEnd(i);
                String subStr = result.getSummarizedText().substring(s, e);
                Assertions.assertEquals(longTerm, subStr, "Incorrect highlight value!");
            }
        } else {
            Assertions.assertEquals("...a " + longTerm + "...", result.getSummarizedText(), "Incorrect summary string!");
            for (int i = 0; i < result.getNumHighlights(); i++) {
                int s = result.getHighlightStart(i);
                int e = result.getHighlightEnd(i);
                String subStr = result.getSummarizedText().substring(s, e);
                Assertions.assertEquals(longTerm.substring(0, maxTokenSize), subStr, "Incorrect highlight value!");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("analyzers")
    void highlightsReallyLongTermsPartialPrefix(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Checks the behavior of highlighting when given really long term queries
         */
        String longTerm = "reallyLongTermWhichTakesHundredsAndHundredsNoIMeanItLotsAndLotsOfCharactersSoItWill" +
                          "ExceedTheLimitOfOurConfigurationsAndThenThereIsMoreTextAftewardsCauseIWantToBeEvenLonger" +
                          "AndLongerAndLongerAndDoIEverRunOutofEnglishWordsINeverDoIAmAnEducatedPersonIThinkMay";
        String text = "This is a " + longTerm + "  I think, but maybe not also because things are weird";


        final String prefix = longTerm.substring(0, 25);
        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:" + prefix.toLowerCase(Locale.ROOT) + "*", 1);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");


        int maxTokenLength = getMaxTokenSize(indexAnalyzer);
        if (maxTokenLength < 0) {
            //if there is no max token length, an extra whitespace between the word and the ... is expected
            Assertions.assertEquals("...a " + longTerm + "  ...", result.getSummarizedText(), "Incorrect summary string!");
        } else {
            Assertions.assertEquals("...a " + longTerm + "...", result.getSummarizedText(), "Incorrect summary string!");
        }

        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String subStr = result.getSummarizedText().substring(s, e);
            //the term match is either the maxTokenLength (if the analyzer has one), or the entire long term
            if (maxTokenLength < 0) {
                Assertions.assertEquals(longTerm, subStr, "Incorrect highlight value!");
            } else {
                Assertions.assertEquals(longTerm.substring(0, maxTokenLength), subStr, "Incorrect highlight value!");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("analyzers")
    void highlightsReallyLongTermsFullPrefix(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        /*
         * Checks the behavior of highlighting when given really long term queries
         */
        String longTerm = "reallyLongTermWhichTakesHundredsAndHundredsNoIMeanItLotsAndLotsOfCharactersSoItWillExceedTheLimitOfOurConfigurations";
        String text = "This is a " + longTerm + "  I think, but maybe not also because things are weird";

        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, text, "text:" + longTerm.toLowerCase(Locale.ROOT) + "*", 1);

        int maxTokenSize = getMaxTokenSize(indexAnalyzer);
        if (maxTokenSize > 0) {
            /*
             * Analyzers which break apart long terms into multiple tokens will not be able to match the full
             * text as a single string, so they should return an empty highlight
             */
            Assertions.assertEquals(0, result.getNumHighlights(), "Did not return empty for limited analyzers!");
        } else {

            Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
            Assertions.assertEquals("...a " + longTerm + "  ...", result.getSummarizedText(), "Incorrect summary string!");
            for (int i = 0; i < result.getNumHighlights(); i++) {
                int s = result.getHighlightStart(i);
                int e = result.getHighlightEnd(i);
                String subStr = result.getSummarizedText().substring(s, e);
                Assertions.assertEquals(longTerm, subStr, "Incorrect highlight value!");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("analyzers")
    void highlightsCjkQueryTerms(String ignored, Analyzer queryAnalyzer, Analyzer indexAnalyzer) throws Exception {
        String input = "water水水물-of的の의。house屋家집\nyou你君너";

        HighlightedTerm result = doHighlight(queryAnalyzer, indexAnalyzer, input, "text:家", 1);
        Assertions.assertEquals(1, result.getNumHighlights(), "Incorrect number of highlights!");
        Assertions.assertEquals("...house屋家집\n...", result.getSummarizedText(), "Incorrect summary string!");
        for (int i = 0; i < result.getNumHighlights(); i++) {
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String subStr = result.getSummarizedText().substring(s, e);
            Assertions.assertEquals("家", subStr, "Incorrect highlight value!");
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private HighlightedTerm doHighlight(Analyzer queryAnalyzer, Analyzer indexAnalyzer, final String text, String query) throws IOException {
        return doHighlight(queryAnalyzer, indexAnalyzer, text, query, 3);
    }

    private HighlightedTerm doHighlight(Analyzer queryAnalyzer, Analyzer indexAnalyzer, final String text, String queryString, int snippetSize) throws IOException {
        UnifiedHighlighter highlighter = LuceneHighlighting.makeHighlighter("text", indexAnalyzer, snippetSize);
        QueryParser queryParser = LuceneQueryParserFactoryProvider.instance().getParserFactory().createMultiFieldQueryParser(new String[] {"text"}, queryAnalyzer, Collections.emptyMap());
        queryParser.setAllowLeadingWildcard(true);
        try {
            Query query = queryParser.parse(queryString);
            BooleanQuery bq = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).build();
            final Object result = highlighter.highlightWithoutSearcher("text", bq, text, 100);
            Assertions.assertTrue(result instanceof HighlightedTerm, "Did not return a string!");
            return (HighlightedTerm)result;
        } catch (ParseException e) {
            Assertions.fail("Failed to parse Lucene query");
        }
        return null;
    }

    private void assertHighlightCorrect(HighlightedTerm expected, HighlightedTerm actual) {
        Assertions.assertEquals(expected.getSummarizedText(), actual.getSummarizedText(), "Incorrect snippet result!");
        Assertions.assertEquals(expected.getNumHighlights(), actual.getNumHighlights(), "Incorrect number of highlights found!");
        for (int i = 0; i < expected.getNumHighlights(); i++) {
            Assertions.assertEquals(expected.getHighlightStart(i), actual.getHighlightStart(i), "Incorrect highlight start!");
            Assertions.assertEquals(expected.getHighlightEnd(i), actual.getHighlightEnd(i), "Incorrect highlight end!");
        }
    }

    private int getMaxTokenSize(Analyzer analyzer) {
        int maxTokenLength = -1;
        if (analyzer instanceof StandardAnalyzer) {
            maxTokenLength = ((StandardAnalyzer)analyzer).getMaxTokenLength();
        } else if (analyzer instanceof UAX29URLEmailAnalyzer) {
            maxTokenLength = ((UAX29URLEmailAnalyzer)analyzer).getMaxTokenLength();
        } else if (analyzer instanceof AlphanumericCjkAnalyzer) {
            maxTokenLength = ((AlphanumericCjkAnalyzer)analyzer).getMaxTokenLength();
        } else if (analyzer instanceof SynonymAnalyzer) {
            maxTokenLength = ((SynonymAnalyzer)analyzer).getMaxTokenLength();
        } else if (analyzer instanceof CustomAnalyzer) {
            CustomAnalyzer ca = (CustomAnalyzer)analyzer;
            final TokenizerFactory tokenizerFactory = ca.getTokenizerFactory();
            String mtStr = tokenizerFactory.getOriginalArgs().get("maxTokenLength");
            if (mtStr != null) {
                maxTokenLength = Integer.parseInt(mtStr);
            }
        }

        return maxTokenLength;
    }

    private boolean isSynonymAnalyzer(final Analyzer analyzer) {
        if (analyzer instanceof SynonymAnalyzer) {
            return true;
        } else if (analyzer instanceof CustomAnalyzer) {
            CustomAnalyzer ca = (CustomAnalyzer)analyzer;
            final List<TokenFilterFactory> tokenFilterFactories = ca.getTokenFilterFactories();
            for (TokenFilterFactory tff : tokenFilterFactories) {
                if (tff instanceof SynonymGraphFilterFactory || tff instanceof RegistrySynonymGraphFilterFactory) {
                    return true;
                }
            }
        }
        return false;
    }
}
