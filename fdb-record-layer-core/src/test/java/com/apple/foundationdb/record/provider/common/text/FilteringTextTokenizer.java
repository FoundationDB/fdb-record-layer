/*
 * FilteringTextTokenizer.java
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

import java.util.Iterator;
import java.util.function.BiPredicate;

/**
 * A {@link TextTokenizer} that filters tokens from another tokenizer with a supplied predicate.
 */
public class FilteringTextTokenizer implements TextTokenizer {
    private final String name;
    private final TextTokenizerFactory tokenizerFactory;
    private final BiPredicate<? super CharSequence, ? super Integer> filter;
    private final int minVersion;
    private final int maxVersion;

    private FilteringTextTokenizer(String name, TextTokenizerFactory tokenizerFactory,
                                   BiPredicate<? super CharSequence, ? super Integer> filter) {
        this.name = name;
        this.tokenizerFactory = tokenizerFactory;
        this.filter = filter;

        TextTokenizer tokenizer = tokenizerFactory.getTokenizer();
        this.minVersion = tokenizer.getMinVersion();
        this.maxVersion = tokenizer.getMaxVersion();
    }

    @Override
    public Iterator<? extends CharSequence> tokenize(String text, int version, TokenizerMode tokenizerMode) {
        TextTokenizer underlying = tokenizerFactory.getTokenizer();
        Iterator<? extends CharSequence> tokenIterator = underlying.tokenize(text, version, tokenizerMode);
        return new Iterator<CharSequence>() {
            @Override
            public boolean hasNext() {
                return tokenIterator.hasNext();
            }

            @Override
            public CharSequence next() {
                CharSequence token = tokenIterator.next();
                if (filter.test(token, version)) {
                    return token;
                } else {
                    return "";
                }
            }
        };
    }

    @Override
    public int getMinVersion() {
        return minVersion;
    }

    @Override
    public int getMaxVersion() {
        return maxVersion;
    }

    @Override
    public String getName() {
        return name;
    }

    public static TextTokenizerFactory create(String name, TextTokenizerFactory tokenizerFactory,
                                              BiPredicate<? super CharSequence, ? super Integer> filter) {
        TextTokenizer tokenizer = new FilteringTextTokenizer(name, tokenizerFactory, filter);
        return new TextTokenizerFactory() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public TextTokenizer getTokenizer() {
                return tokenizer;
            }
        };
    }
}
