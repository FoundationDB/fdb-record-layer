/*
 * AllSuffixesTextTokenizer.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * A text tokenizer that first tokenizes a document and then returns all
 * suffixes for all tokens that it finds. This is essentially how one would
 * implement something like an n-gram text index given the machinery available.
 */
public class AllSuffixesTextTokenizer implements TextTokenizer {

    @Nonnull
    private static final AllSuffixesTextTokenizer INSTANCE = new AllSuffixesTextTokenizer();
    @Nonnull
    public static final String NAME = "all_suffixes";

    private AllSuffixesTextTokenizer() {
    }

    private static final class SuffixingIterator implements Iterator<String> {
        @Nonnull
        private final Iterator<? extends CharSequence> underlying;
        @Nullable
        private String current;
        private int pos;

        public SuffixingIterator(@Nonnull Iterator<? extends CharSequence> underlying) {
            this.underlying = underlying;
            this.current = null;
            this.pos = -1;
        }

        @Override
        public boolean hasNext() {
            return (current == null && underlying.hasNext()) || (current != null && pos < current.length() || underlying.hasNext());
        }

        @Override
        public String next() {
            if (current == null || pos == current.length()) {
                current = underlying.next().toString();
                pos = 0;
            }
            final String toReturn = current.substring(pos);
            pos++;
            return toReturn;
        }
    }

    @Nonnull
    @Override
    public Iterator<? extends CharSequence> tokenize(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        final Iterator<? extends CharSequence> underlying = DefaultTextTokenizer.instance().tokenize(text, version, mode);
        if (mode == TokenizerMode.QUERY) {
            return underlying;
        } else {
            return new SuffixingIterator(underlying);
        }
    }

    @Override
    public int getMaxVersion() {
        return DefaultTextTokenizer.instance().getMaxVersion();
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    public static AllSuffixesTextTokenizer instance() {
        return INSTANCE;
    }
}
