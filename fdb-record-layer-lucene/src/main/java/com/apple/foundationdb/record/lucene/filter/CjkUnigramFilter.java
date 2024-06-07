/*
 * CjkUnigramFilter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.filter;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An analyzer that forms unigrams of CJK terms.
 * NOTICE:
 * This analyzer contains code that was
 * taken from {@link org.apache.lucene.analysis.cjk.CJKBigramFilter} and modified
 */
public final class CjkUnigramFilter extends TokenFilter {
    // when we emit a unigram, it's then marked as this type
    public static final String SINGLE_TYPE = "<SINGLE>";

    // the types from standardtokenizer
    private static final String HAN_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.IDEOGRAPHIC];
    private static final String HIRAGANA_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HIRAGANA];
    private static final String KATAKANA_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.KATAKANA];
    private static final String HANGUL_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HANGUL];

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    // buffers containing codepoint and offsets in parallel
    int[] buffer = new int[8];
    int[] startOffset = new int[8];
    int[] endOffset = new int[8];
    // length of valid buffer
    int bufferLen;
    // current buffer index
    int index;

    // the last end offset
    int lastEndOffset;

    private boolean exhausted;

    public CjkUnigramFilter(TokenStream in) {
        super(in);
    }

    /*
     * much of this complexity revolves around handling the special case of a
     * "lone cjk character" where cjktokenizer would output a unigram. this
     * is also the only time we ever have to captureState.
     */
    @Override
    public boolean incrementToken() throws IOException {
        while (true) {
            if (hasBufferedUnigram()) {
                flushUnigram();
                return true;
            }

            if (!doNext()) { // read next token from stream
                return false;
            }

            if (!isCjkType(typeAtt.type())) {
                return true;  // not a CJK type; just return token as-is.
            }

            refill();
        }
    }

    private boolean isCjkType(final String type) {
        return HAN_TYPE.equals(type) || HIRAGANA_TYPE.equals(type) ||
                KATAKANA_TYPE.equals(type) || HANGUL_TYPE.equals(type);
    }

    /**
     * looks at next input token, returning false is none is available.
     */
    private boolean doNext() throws IOException {
        if (exhausted) {
            return false;
        } else if (input.incrementToken()) {
            return true;
        } else {
            exhausted = true;
            return false;
        }
    }

    /**
     * refills buffers with new data from the current token.
     */
    private void refill() {
        // compact buffers to keep them smallish if they become large
        // just a safety check, but technically we only need the last codepoint
        if (bufferLen > 64) {
            int last = bufferLen - 1;
            buffer[0] = buffer[last];
            startOffset[0] = startOffset[last];
            endOffset[0] = endOffset[last];
            bufferLen = 1;
            index -= last;
        }

        char[] termBuffer = termAtt.buffer();
        int len = termAtt.length();

        int newSize = bufferLen + len;
        buffer = ArrayUtil.grow(buffer, newSize);
        startOffset = ArrayUtil.grow(startOffset, newSize);
        endOffset = ArrayUtil.grow(endOffset, newSize);

        int start = offsetAtt.startOffset();
        int end = offsetAtt.endOffset();
        lastEndOffset = end;

        if (end - start != len) {
            // crazy offsets (modified by synonym or charfilter): just preserve
            int cp;
            for (int i = 0; i < len; i += Character.charCount(cp)) {
                cp = buffer[bufferLen] = Character.codePointAt(termBuffer, i, len);
                startOffset[bufferLen] = start;
                endOffset[bufferLen] = end;
                bufferLen++;
            }
        } else {
            // normal offsets
            int cp;
            int cpLen;
            for (int i = 0; i < len; i += cpLen) {
                cp = buffer[bufferLen] = Character.codePointAt(termBuffer, i, len);
                cpLen = Character.charCount(cp);
                startOffset[bufferLen] = start;
                start = endOffset[bufferLen] = start + cpLen;
                bufferLen++;
            }
        }
    }

    /**
     * Flushes a unigram token to output from our buffer.
     * This happens when we encounter isolated CJK characters, either the whole
     * CJK string is a single character, or we encounter a CJK character surrounded
     * by space, punctuation, english, etc, but not beside any other CJK.
     */
    private void flushUnigram() {
        clearAttributes();
        char[] termBuffer = termAtt.resizeBuffer(2); // maximum unigram length (2 surrogates)
        int len = Character.toChars(buffer[index], termBuffer, 0);
        termAtt.setLength(len);
        offsetAtt.setOffset(startOffset[index], endOffset[index]);
        typeAtt.setType(SINGLE_TYPE);
        index++;
    }

    /**
     * True if we have a single codepoint sitting in our buffer, where its future
     * depends upon not-yet-seen inputs.
     */
    private boolean hasBufferedUnigram() {
        return (bufferLen - index) > 0;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        bufferLen = 0;
        index = 0;
        lastEndOffset = 0;
        exhausted = false;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") //nothing to actually close
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        CjkUnigramFilter that = (CjkUnigramFilter) o;
        return bufferLen == that.bufferLen &&
                index == that.index &&
                lastEndOffset == that.lastEndOffset &&
                exhausted == that.exhausted &&
                termAtt.equals(that.termAtt) &&
                typeAtt.equals(that.typeAtt) &&
                offsetAtt.equals(that.offsetAtt) &&
                Arrays.equals(buffer, that.buffer) &&
                Arrays.equals(startOffset, that.startOffset) &&
                Arrays.equals(endOffset, that.endOffset);
    }

    static boolean isUnigramTokenType(final String type) {
        return CjkUnigramFilter.SINGLE_TYPE.equals(type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(buffer), Arrays.hashCode(startOffset), Arrays.hashCode(endOffset), termAtt, typeAtt, offsetAtt, bufferLen, index, lastEndOffset, exhausted);
    }
}
