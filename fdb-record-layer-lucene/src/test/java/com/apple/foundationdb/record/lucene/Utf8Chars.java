/*
 * Utf8Chars.java
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

import com.google.common.collect.Streams;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

import java.util.stream.Stream;

/**
 * Utility testing class to identify special characters for lucene tokenization. We block out a few
 * different groups of different UTF-8 code points so that we can generate lots and lots of verification
 * tests for different non-english characters.
 */
public class Utf8Chars {

    //UTF code points that are unassigned (and therefore skipped during tokenization)
    private static final Bits UNDEFINED_CODE_POINTS = sparseBits(
            0x0378, 0x0379, 0x0380, 0x0381, 0x0382, 0x0383, 0x038b, 0x038d, 0x03a2 //greek standard
    );

    //terms that are punctuation in one language or another, and thus ignored
    private static final Bits PUNCTUATION = sparseBits(
            0x0375, 0x037e, 0x0384, 0x0385, 0x0387, 0x03f6, //greek punctuation and special marks
            0x0482, 0x0483, 0x0484, 0x0485, 0x0486, 0x0487, 0x0488, 0x0489 // cyrillic punctuation and special marks
    );

    //codes that are equivalent to some _other_ unicode character, and so won't match when searched on,
    //because lucene only tokenizes the other one
    private static final Bits DUPLICATES = sparseBits(
            0x03f6 //greek lunate epsilon
    );

    /**
     * Get tokens that we expect Lucene's analyzers to be able to tokenize(and therefore, for us to be able to search
     * on).
     * <p>
     * In general, Lucene's stock analyzers will ignore terms that it deems to be "punctuation" or "special"
     * in some other way. Specifically, it follows the
     * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a> in terms of character recognition.
     * <p>
     * This method returns a subset of UTF-8 code points which we determine _should_ be tokenized (although
     * not necessarily exhaustively).
     *
     * @return a subset of UTF-8 code points which we manually determine _should_ be tokenized and searchable.
     */
    public static Stream<String> getUnusualTokenizableChars() {
        return Streams.concat(getGreekChars(), getCyrillicChars(), getExtendedLatin());
    }

    /**
     * Get tokenizable Greek and Coptic UTF-8 characters.
     *
     * @return Greek and Coptic UTF-8 characters that we expect to be tokenizable.
     */
    public static Stream<String> getGreekChars() {
        return Stream.iterate(0x0370, cp -> cp < 0x03FF, cp -> cp + 1)
                .filter(cp -> !UNDEFINED_CODE_POINTS.get(cp)
                              && !PUNCTUATION.get(cp)
                              && !DUPLICATES.get(cp))
                .map(Character::toString)
                .filter(t -> !foldsToAscii(t));
    }

    /**
     * Get tokenizable Cyrillic UTF-8 characters.
     *
     * @return Cyrillic UTF-8 characters that we expect to be tokenizable.
     */
    public static Stream<String> getCyrillicChars() {
        return Stream.iterate(0x0400, cp -> cp < 0x04FF, cp -> cp + 1)
                .filter(cp -> !UNDEFINED_CODE_POINTS.get(cp)
                              && !PUNCTUATION.get(cp)
                              && !DUPLICATES.get(cp))
                .map(Character::toString)
                .filter(t -> !foldsToAscii(t));
    }

    /**
     * Get extended latin UTF-8 characters. These are normal latin characters with diareses, macrons,
     * dots above and below, etc. to denote different pronunciations. They should all be searchable.
     *
     * @return extended latin UTF-8 characters.
     */
    public static Stream<String> getExtendedLatin() {
        return Stream.iterate(0x1E00, cp -> cp < 0x1EFF, cp -> cp + 1)
                .filter(cp -> !UNDEFINED_CODE_POINTS.get(cp)
                              && !PUNCTUATION.get(cp)
                              && !DUPLICATES.get(cp))
                .map(Character::toString)
                .filter(t -> !foldsToAscii(t));
    }

    private static Bits sparseBits(int... codepoints) {
        final int len = codepoints[codepoints.length - 1] + 1;
        final SparseFixedBitSet bitset = new SparseFixedBitSet(len);
        for (int i : codepoints) {
            bitset.set(i);
        }
        return new Bits() {
            @Override
            public boolean get(int index) {
                return index < len && bitset.get(index);
            }

            @Override
            public int length() {
                return 0x10FFFF + 1;
            }
        };
    }

    private static boolean foldsToAscii(String text) {
        char[] input = text.toCharArray();
        char[] output = new char[4 * input.length]; //max size of a single UTF character is 4 bytes
        int finalPos = ASCIIFoldingFilter.foldToASCII(input, 0, output, 0, input.length);
        for (int i = 0; i < finalPos; i++) {
            if (output[i] != input[i]) {
                return true;
            }
        }
        return false;
    }
}
