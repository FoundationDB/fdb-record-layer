/*
 * RandomStringParser.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a descriptor string of the form {@code "seed <N> length <M>"} and generates a
 * deterministic random alphanumeric string of length {@code M} using a {@link Random} instance
 * seeded with {@code N}.
 *
 * <p>Example input: {@code "seed 123234 length 1000"}
 */
public final class RandomStringParser {

    private static final Pattern PATTERN =
            Pattern.compile("^seed\\s+(\\d+)\\s+length\\s+(\\d+)$", Pattern.CASE_INSENSITIVE);

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";

    private RandomStringParser() {
    }

    /**
     * Parses {@code descriptor} and returns the generated string.
     *
     * @param descriptor a string matching {@code "seed <N> length <M>"}
     * @return a deterministic random alphanumeric string of length {@code M}
     * @throws IllegalArgumentException if {@code descriptor} does not match the expected format
     */
    @Nonnull
    public static String parse(@Nonnull final String descriptor) {
        final Matcher matcher = PATTERN.matcher(descriptor.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid !randomStr descriptor '" + descriptor + "'. Expected format: \"seed <N> length <M>\"");
        }
        final long seed = Long.parseLong(matcher.group(1));
        final int length = Integer.parseInt(matcher.group(2));
        return generate(seed, length);
    }

    @Nonnull
    private static String generate(final long seed, final int length) {
        final Random random = new Random(seed);
        final StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}