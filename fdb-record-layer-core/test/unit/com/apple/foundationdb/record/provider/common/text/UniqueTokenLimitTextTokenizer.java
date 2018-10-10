/*
 * UniqueTokenLimitTextTokenizer.java
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link TextTokenizer} that limits the number of unique tokens.
 */
public class UniqueTokenLimitTextTokenizer implements TextTokenizer {
    @Nonnull
    private static final UniqueTokenLimitTextTokenizer INSTANCE = new UniqueTokenLimitTextTokenizer();
    @Nonnull
    private static final DefaultTextTokenizer DEFAULT_TOKENIZER = DefaultTextTokenizer.instance();
    @Nonnull
    public static final String NAME = "unique_token_limiting";

    private static final int MAX_UNIQUE_TOKENS_V0 = 5;

    private UniqueTokenLimitTextTokenizer() {
    }

    @Nonnull
    public static UniqueTokenLimitTextTokenizer instance() {
        return INSTANCE;
    }

    // Map the version of this tokenizer to the version of the default tokenizer.
    // This way, we can bump this version of the tokenizer without changing
    // the version passed through to the default tokenizer.
    private int getDefaultVersion(int version) {
        return GLOBAL_MIN_VERSION;
    }

    @Nonnull
    @Override
    public Iterator<? extends CharSequence> tokenize(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        return DEFAULT_TOKENIZER.tokenize(text, getDefaultVersion(version), mode);
    }

    // Map the version to a maximum number of tokens. If this is changed, we
    // can bump the version of the tokenizer and use this function to make sure
    // we choose a previous number if given an older version and the updated
    // number if given a more recent version.
    private int getMaxUniqueTokens(int version) {
        return MAX_UNIQUE_TOKENS_V0;
    }

    @Nonnull
    @Override
    public Map<String, List<Integer>> tokenizeToMap(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        final Iterator<? extends CharSequence> tokens = tokenize(text, version, mode);
        final int maxUniqueTokens = getMaxUniqueTokens(version);
        Map<String, List<Integer>> offsetMap = new HashMap<>();
        int offset = 0;
        while (tokens.hasNext()) {
            final String token = tokens.next().toString();
            if (!token.isEmpty()) {
                List<Integer> offsets = offsetMap.get(token);
                if (offsets != null) {
                    offsets.add(offset);
                } else {
                    if (offsetMap.size() == maxUniqueTokens) {
                        // Adding this token would cause this to have more
                        // than the maximum number of unique tokens.
                        // Don't add it; just return the current map.
                        return offsetMap;
                    } else {
                        List<Integer> newOffsets = new ArrayList<>();
                        newOffsets.add(offset);
                        offsetMap.put(token, newOffsets);
                    }
                }
            }
            offset++;
        }
        return offsetMap;
    }

    @Override
    public int getMaxVersion() {
        return 0;
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }
}
