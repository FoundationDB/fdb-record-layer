/*
 * PrefixTextTokenizer.java
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
import java.util.Iterator;

/**
 * A {@link TextTokenizer} that converts long tokens to a prefix.
 */
public class PrefixTextTokenizer implements TextTokenizer {
    public static final String NAME = "prefix";
    private static PrefixTextTokenizer INSTANCE = new PrefixTextTokenizer();
    private DefaultTextTokenizer underlying = DefaultTextTokenizer.instance();
    private static final int PREFIX_SIZE_V0 = 3;
    private static final int PREFIX_SIZE = 4;

    private PrefixTextTokenizer() {
    }

    public static PrefixTextTokenizer instance() {
        return INSTANCE;
    }

    private Iterator<String> tokenizeV0(@Nonnull String text, @Nonnull TokenizerMode mode) {
        Iterator<String> defaultTokens = underlying.tokenize(text, TextTokenizer.GLOBAL_MIN_VERSION, mode);
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return defaultTokens.hasNext();
            }

            @Override
            public String next() {
                final String nextUnderyling = defaultTokens.next();
                if (nextUnderyling.length() <= PREFIX_SIZE_V0) {
                    return nextUnderyling;
                } else {
                    return nextUnderyling.substring(0, PREFIX_SIZE_V0);
                }
            }
        };
    }

    private Iterator<String> tokenizeV1(@Nonnull String text, @Nonnull TokenizerMode mode) {
        Iterator<String> defaultTokens = underlying.tokenize(text, TextTokenizer.GLOBAL_MIN_VERSION, mode);
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return defaultTokens.hasNext();
            }

            @Override
            public String next() {
                final String nextUnderyling = defaultTokens.next();
                if (nextUnderyling.length() <= PREFIX_SIZE) {
                    return nextUnderyling;
                } else {
                    return nextUnderyling.substring(0, PREFIX_SIZE);
                }
            }
        };
    }

    @Nonnull
    @Override
    public Iterator<String> tokenize(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        validateVersion(version);
        if (version == 0) {
            return tokenizeV0(text, mode);
        } else {
            return tokenizeV1(text, mode);
        }
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getMaxVersion() {
        return 1;
    }
}
