/*
 * DefaultTextTokenizer.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.BreakIterator;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the default tokenizer used by full-text indexes. It will split the text
 * on whitespace, normalize the input into Unicode normalization form KD (compatibility
 * decomposition), case fold input to lower case, and strip all diacritical marks. This
 * is appropriate for exact matching of many languages (those that use whitespace
 * as their word separator, e.g., most European languages, Korean, Semitic languages,
 * etc.), but it doesn't handle highly synthetic languages particularly well, nor does
 * it handle languages like Chinese, Japanese, or Thai that do not generally use whitespace
 * to indicate word boundaries.
 */
@API(API.Status.EXPERIMENTAL)
public class DefaultTextTokenizer implements TextTokenizer {
    @Nonnull
    private static final DefaultTextTokenizer INSTANCE = new DefaultTextTokenizer();

    // This unicode normalized form splits diacritical marks so that they can be
    // removed. It also collapses equivalent graphemes, e.g., "ﬆ" into "st".
    @Nonnull
    private static final Normalizer.Form NORMALIZED_FORM = Normalizer.Form.NFKD;

    @Nonnull
    private static final Pattern DIACRITICAL_PATTERN = Pattern.compile("\\p{M}+");

    /**
     * The name of the default tokenizer. This can be used to explicitly
     * require the default tokenizer in a text index.
     */
    @Nonnull
    public static final String NAME = "default";

    private DefaultTextTokenizer() {
    }

    /**
     * Get this class's singleton. This text tokenizer maintains no state,
     * so only one instance is needed.
     *
     * @return this tokenizer's singleton instance
     */
    @Nonnull
    public static DefaultTextTokenizer instance() {
        return INSTANCE;
    }

    // Turn an iterator that
    private static class BreakIteratorWrapper implements Iterator<String> {
        @Nonnull
        private final BreakIterator underlying;
        @Nonnull
        private final String text;
        @Nullable
        private String nextToken = null;
        private int lastBreak;
        @Nonnull
        private Matcher matcher;

        private BreakIteratorWrapper(@Nonnull BreakIterator underlying, @Nonnull String text) {
            this.underlying = underlying;
            this.text = text;
            this.lastBreak = underlying.first();
            this.matcher = DIACRITICAL_PATTERN.matcher("");
        }

        @Override
        public boolean hasNext() {
            if (nextToken != null) {
                return true;
            }
            int nextBreak = underlying.following(lastBreak);
            while (nextToken == null && nextBreak != BreakIterator.DONE) {
                String token = text.substring(lastBreak, nextBreak);
                // Normalize the string to a standard normalization.
                // This is done prior to checking for alphabetic characters
                // because some Unicode characters (like the blackboard
                // section) are recognized as characters only once compatibility
                // equivalents are normalized away.
                if (!Normalizer.isNormalized(token, NORMALIZED_FORM)) {
                    token = Normalizer.normalize(token, NORMALIZED_FORM);
                }
                boolean isToken = false;
                for (int i = 0; i < token.length(); i++) {
                    if (Character.isLetterOrDigit(token.charAt(i))) {
                        isToken = true;
                        break;
                    }
                }
                if (isToken) {
                    // Case-fold (using Locale.ROOT to avoid different things happening in Turkey)
                    // and remove diacritical marks. The diacritical filter might be too
                    // aggressive in that it will also do things like strip away vowels in
                    // many abugidas. It does the right thing with Hangul Jamo, though.
                    //
                    // Example transformations:
                    //     hELlo -> hello
                    //     Igloo -> igloo (note: if not Locale.ROOT and run in Turkey, the "i" will be missing a tittle)
                    //     Après -> apres
                    //     Здра́вствуйте -> здравствуите
                    //     אֶתְנַחְתָּ֑א -> אתנחתא
                    //     అన్నం -> అనన (note: this is essentially stripping the vowels away, which might be "wrong")
                    //     안녕하세요 -> 안녕하세요 (Hangul Jamo not transformed)
                    token = matcher.reset(token.toLowerCase(Locale.ROOT)).replaceAll("");
                    nextToken = token;
                }
                lastBreak = nextBreak;
                nextBreak = underlying.next();
            }
            return nextToken != null;
        }

        @Nonnull
        @Override
        public String next() {
            if (hasNext()) {
                String next = nextToken;
                nextToken = null;
                return next;
            } else {
                throw new NoSuchElementException("No more tokens found in text");
            }
        }
    }

    /**
     * Tokenize the text based on whitespace. This normalizes the input using the NFKD
     * (compatibility decomposition) normal form, case-folds to lower case, and
     * then strips out diacritical marks. It makes no other attempts to stem words
     * into their base forms, nor does it attempt to make word splits between words
     * in synthetic languages or in languages that do not use whitespace as tokenizers.
     * This tokenizer performs identically when used to tokenize documents at index
     * time and when used to tokenize query strings.
     *
     * @param text source text to split
     * @param version version of the tokenizer to use to split the text
     * @param mode ignored as this tokenizer operates the same way at index and query time
     * @return an iterator over whitespace-separated tokens
     */
    @Nonnull
    @Override
    public Iterator<String> tokenize(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        validateVersion(version);
        final BreakIterator breakIterator = BreakIterator.getWordInstance(Locale.ROOT);
        breakIterator.setText(text);
        return new BreakIteratorWrapper(breakIterator, text);
    }

    /**
     * Get the name for this tokenizer. For default tokenizers, the name is
     * "{@value NAME}".
     *
     * @return the name of the default tokenizer
     */
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the maximum supported version. Currently, there is only
     * one version of this tokenizer, so the maximum version is the
     * same as the minimum version.
     *
     * @return the maximum version supported by this tokenizer
     */
    @Override
    public int getMaxVersion() {
        return getMinVersion();
    }
}
