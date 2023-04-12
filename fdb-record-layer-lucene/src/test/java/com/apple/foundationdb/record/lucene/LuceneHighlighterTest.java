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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Tests on the text processing of the Lucene Highlight functionality. The intent here is to bypass
 * the expensive query planning process in order to ensure that the highlight matching functionality
 * works as expected.
 */
public class LuceneHighlighterTest {


    private static final String ROSE_BY_ANY_OTHER_NAME="'Tis but thy name that is my enemy;\n" +
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
    public static void setup(){
        //set up the English Synonym Map, so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    public static Stream<Arguments> analyzers() {
        return Stream.of(
                Arguments.of("Standard", LuceneAnalyzerWrapper.getStandardAnalyzerWrapper().getAnalyzer()),
                Arguments.of("Synonym", new SynonymAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME)),
                Arguments.of("Ngram", new NgramAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, 3, 30, false))
        );
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithNoSnippets(String ignored, Analyzer analyzer) throws IOException { //the first string is the analyzer name
        String text = "Hello record layer";
        Query tq = new TermQuery(new Term("text", "hello"));
        HighlightedTerm result = doHighlight(analyzer, text, tq, -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {0}, new int[] {5}),result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void ngramHighlightsNgrams(String ignored, Analyzer analyzer) throws IOException {
        //only the ngram analyzer will pick these up as a match, all others will treat them as full-word queries
        //and won't return results
        Assumptions.assumeTrue(analyzer instanceof NgramAnalyzer,"Only care about Ngram analyzers");

        String text = "Hello record layer";
        Query tq = new TermQuery(new Term("text", "hel"));
        HighlightedTerm result = doHighlight(analyzer, text, tq, -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {0}, new int[] {5}), result);

        tq = new TermQuery(new Term("text","cord"));
        result = doHighlight(analyzer, text, tq, -1);
        assertHighlightCorrect(new HighlightedTerm("text", text, new int[] {6}, new int[] {12}), result);

    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithSnippets(String ignored, Analyzer analyzer) throws IOException { //the first string is the analyzer name
        String text = "Good Morning From Apple News It?s Monday, July 11. Here?s what you need to know. ";

        PrefixQuery pq = new PrefixQuery(new Term("text", "appl"));
        final HighlightedTerm result = doHighlight(analyzer, text, pq);
        String correctString = "Good Morning From Apple News It?s...";
        assertHighlightCorrect(new HighlightedTerm("text",correctString,new int[]{18},new int[]{23}),result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightsSimpleTextWithWildcardSnippetsInsideHtml(String ignored, Analyzer analyzer) throws IOException {
        String text = "Good Morning From Apple News It?s Monday, July 11. Here?s what you need to know. " +
                      "Top Stories Former Trump adviser Steve Bannon agreed to testify to the January 6 committee " +
                      "after months of defying a congressional subpoena. The Washington Post https://apple.news/AgZ7a_IT4TpKFF3kAyZsvUg?";

        Query pq = new RegexpQuery(new Term("text", ".*appl.*"));
        final HighlightedTerm result = doHighlight(analyzer, text, pq);
        String correctString = "Good Morning From Apple News It?s...The Washington Post https://apple.news/AgZ7a_IT4TpKFF3kAyZsvUg?";
        int[] starts = new int[] {18, 56};
        int[] ends = new int[] {23, 99};
        assertHighlightCorrect(new HighlightedTerm("text",correctString,starts,ends),result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightTextWithMultipleTermMatches(String ignored, Analyzer analyzer) throws Exception{

        Query pq = new TermQuery(new Term("text","name"));
        final HighlightedTerm result = doHighlight(analyzer, ROSE_BY_ANY_OTHER_NAME,pq);
        String correctString;
        HighlightedTerm expected;
        if(analyzer instanceof SynonymAnalyzer){
            //"call" is a synonym for "name", so we have to catch that as well
            correctString = "'Tis but thy name that is my...be some other name!\nWhat's in a name? " +
                            "That which we call a rose\nBy any other name would smell as..., doff thy name,\n" +
                            "And for that name which is no...";
            expected = new HighlightedTerm("text",correctString,new int[]{13,45,63,83,108,141,160},new int[]{17,49,67,87,112,145,164});
        }else {
            correctString = "'Tis but thy name that is my...be some other name!\nWhat's in a name? " +
                                   "That which...By any other name would smell as..., doff thy name,\n" +
                                   "And for that name which is no...";
            expected = new HighlightedTerm("text",correctString,new int[]{13,45,63,95,128,147},new int[]{17,49,67,99,132,151});
        }
        assertHighlightCorrect(expected,result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightMultiplePrefixMatches(String ignored, Analyzer analyzer) throws Exception{

        Query pq = new PrefixQuery(new Term("text","monta"));
        final HighlightedTerm result = doHighlight(analyzer, ROSE_BY_ANY_OTHER_NAME,pq);
        String correctString = "...though not a Montague.\nWhat's Montague? It is...";
        assertHighlightCorrect(new HighlightedTerm("text",correctString,new int[]{16,33},new int[]{24,41}),result);
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightLinesUp(String ignored, Analyzer analyzer) throws Exception{
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

        Query query = new TermQuery(new Term("text","layer"));
        final HighlightedTerm result = doHighlight(analyzer,text,query,4);
        Assertions.assertEquals(3,result.getNumHighlights(),"Incorrect number of highlights!");
        for(int i=0;i<result.getNumHighlights();i++){
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            String highlightedText = result.getSummarizedText().substring(s,e);
            Assertions.assertEquals("layer",highlightedText,"Incorrect highlight!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInTermQueries(String ignored, Analyzer analyzer) throws Exception{
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

        Query query = new TermQuery(new Term("text","bcc70@apple.com"));
        final HighlightedTerm result = doHighlight(analyzer,text,query,4);
        Assertions.assertEquals(1,result.getNumHighlights(),"Incorrect number of highlights!");
        for(int i=0;i<result.getNumHighlights();i++){
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com",result.getSummarizedText().substring(s,e),"Incorrect highlight positions!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInPrefixQueries(String ignored, Analyzer analyzer) throws Exception{
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
        Query query = new PrefixQuery(new Term("text","bcc70@apple.com"));
        HighlightedTerm result = doHighlight(analyzer,text,query,4);
        Assertions.assertEquals(1,result.getNumHighlights(),"Incorrect number of highlights!");
        for(int i=0;i<result.getNumHighlights();i++){
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com",result.getSummarizedText().substring(s,e),"Incorrect highlight positions!");
        }

        //prefix the part before the @
        query = new PrefixQuery(new Term("text","bcc70"));
        result = doHighlight(analyzer,text,query,4);
        Assertions.assertEquals(1,result.getNumHighlights(),"Incorrect number of highlights!");
        for(int i=0;i<result.getNumHighlights();i++){
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com",result.getSummarizedText().substring(s,e),"Incorrect highlight positions!");
        }
    }

    @MethodSource("analyzers")
    @ParameterizedTest(name = "{0}")
    void highlightEmailsInWildcardQueries(String ignored, Analyzer analyzer) throws Exception{
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
        Query query = new WildcardQuery(new Term("text","bcc70@a*e.com"));
        HighlightedTerm result = doHighlight(analyzer,text,query,4);
        Assertions.assertEquals(1,result.getNumHighlights(),"Incorrect number of highlights!");
        for(int i=0;i<result.getNumHighlights();i++){
            int s = result.getHighlightStart(i);
            int e = result.getHighlightEnd(i);
            Assertions.assertEquals("bcc70@apple.com",result.getSummarizedText().substring(s,e),"Incorrect highlight positions!");
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private HighlightedTerm doHighlight(Analyzer analyzer,final String text, Query query) throws IOException {
        return doHighlight(analyzer, text, query,3);
    }

    private HighlightedTerm doHighlight(Analyzer analyzer,final String text, Query query,int snippetSize) throws IOException {
        UnifiedHighlighter highlighter = LuceneHighlighting.makeHighlighter("text",analyzer,snippetSize);

        BooleanQuery bq = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).build();

        final Object result= highlighter.highlightWithoutSearcher("text", bq, text, 100);
        Assertions.assertTrue(result instanceof HighlightedTerm, "Did not return a string!");
        return (HighlightedTerm)result;
    }

    private void assertHighlightCorrect(HighlightedTerm expected, HighlightedTerm actual) {
        Assertions.assertEquals(expected.getSummarizedText(), actual.getSummarizedText(), "Incorrect snippet result!");
        Assertions.assertEquals(expected.getNumHighlights(), actual.getNumHighlights(), "Incorrect number of highlights found!");
        for (int i = 0; i < expected.getNumHighlights(); i++) {
            Assertions.assertEquals(expected.getHighlightStart(i), actual.getHighlightStart(i), "Incorrect highlight start!");
            Assertions.assertEquals(expected.getHighlightEnd(i), actual.getHighlightEnd(i), "Incorrect highlight end!");
        }
    }
}
