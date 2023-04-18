/*
 * AlphanumericCjkAnalyzerTest.java
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class AlphanumericCjkAnalyzerTest {
    @Test
    void verifyTextWithMixedCharactersIsTokenizedAsExpected() throws IOException {
        Analyzer analyzer = new AlphanumericCjkAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, 3, 30, null);
        String input = "water水水물-of的の의。house屋家집\nyou你君너";

        //should skip of, but not you
        List<String> text = readTokenizedText(analyzer, input);

        Assertions.assertIterableEquals(List.of(
                "water", "水", "水", "물", "的", "の", "의",
                "house", "屋", "家", "집", "you", "你", "君", "너"
        ), text, "Incorrect tokenized string!");
    }

    @Test
    void verifyAlphanumericMinTokenLengthIsRespected() throws IOException {
        Analyzer analyzer = new AlphanumericCjkAnalyzer(CharArraySet.EMPTY_SET, 4, 6, null);
        String input = "当when全all 行 can 每each。醒-wake";

        //should skip can and all, but when, each, and wake should be in there
        List<String> text = readTokenizedText(analyzer, input);


        Assertions.assertIterableEquals(List.of(
                "当", "when", "全", "行", "每", "each", "醒", "wake"), text, "Incorrect tokenized string!");
    }

    @Test
    void verifyAlphanumericMaxTokenLengthIsRespected() throws IOException {
        Analyzer analyzer = new AlphanumericCjkAnalyzer(CharArraySet.EMPTY_SET, 1, 3, false, null);
        String input = "当when全all 行 can 每each。醒-wake，我I\n之of";

        //should skip when, each, and wake for being too long
        List<String> text = readTokenizedText(analyzer, input);

        Assertions.assertIterableEquals(List.of(
                "当", "全", "all", "行", "can", "每", "醒", "我", "i", "之", "of"), text, "Incorrect tokenized string!");
    }

    @Test
    void breaksLongTokensApart() throws IOException {
        Analyzer analyzer = new AlphanumericCjkAnalyzer(CharArraySet.EMPTY_SET, 1, 3, true, null);
        String input = "当when全all 行 can 每each。醒-wake，我I\n之of";

        //should skip when, each, and wake for being too long
        List<String> text = readTokenizedText(analyzer, input);

        Assertions.assertIterableEquals(List.of(
                "当", "whe", "n", "全", "all", "行", "can",
                "每", "eac", "h", "醒", "wak", "e", "我", "i", "之", "of"), text, "Incorrect tokenized string!");
    }

    @Test
    void verifyStopwordsAreExcluded() throws IOException {
        Analyzer analyzer = new AlphanumericCjkAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        String input = "当of全all 行 can 每each。醒-wake";

        //of should be ignored, but everything else is there
        List<String> text = readTokenizedText(analyzer, input);

        Assertions.assertIterableEquals(List.of(
                "当", "全", "all", "行", "can", "每", "each", "醒", "wake"), text, "Incorrect tokenized String!");
    }

    private List<String> readTokenizedText(Analyzer analyzer, String input) throws IOException {
        TokenStream tokenizer = analyzer.tokenStream("text", input);
        CharTermAttribute termAttr = tokenizer.addAttribute(CharTermAttribute.class);
        tokenizer.reset();
        List<String> terms = new ArrayList<>();
        while (tokenizer.incrementToken()) {
            terms.add(termAttr.toString());
        }
        return terms;
    }
}
