/*
 * StringUtil.java
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

package com.apple.foundationdb.util;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Utility methods for operating with {@link String}s.
 */
public class StringUtils {
    /**
     * Whether the string is a non-empty string containing only numeric characters.
     *
     * @param s string to test for numeric characters
     * @return whether {@code s} contains only numeric characters
     */
    public static boolean isNumeric(@Nonnull String s) {
        return isNumeric(s, 0);
    }

    /**
     * Whether the substring beginning at {@code beginIndex} is non-empty and contains only
     * numeric characters. This should be equivalent to calling {@code isNumeric(s.substring(beginIndex))}.
     *
     * @param s string to test for numeric characters
     * @param beginIndex index to begin testing for numeric characters
     * @return whether the substring of {@code s} beginning at {@code beginIndex} is non-empty and contains only
     *     numeric characters
     */
    public static boolean isNumeric(@Nonnull String s, int beginIndex) {
        return isNumeric(s, beginIndex, s.length());
    }

    /**
     * Whether the substring from {@code beginIndex} to {@code endIndex} is non-empty and contains only
     * numeric characters. This should be equivalent to calling {@code isNumeric(s.substring(beginIndex, endIndex))}.
     *
     * @param s string to test for numeric characters
     * @param beginIndex index to begin testing for numeric characters
     * @param endIndex index to end testing for numeric characters
     * @return whether the substring of {@code s} beginning at {@code beginIndex} and ending at {@code endIndex}
     *     is non-empty and contains only numeric characters
     */
    public static boolean isNumeric(@Nonnull String s, int beginIndex, int endIndex) {
        Preconditions.checkArgument(beginIndex >= 0 && beginIndex <= s.length(),
                "beginIndex should be within bounds");
        Preconditions.checkArgument(endIndex >= beginIndex && endIndex <= s.length(),
                "endIndex should be within bounds");
        if (beginIndex == endIndex) {
            // Empty string is not vacuously numeric. There must be at least one numeric character
            return false;
        }
        for (int i = beginIndex; i < endIndex; i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static final class Replacement implements Comparable<Replacement> {
        private final int index;
        private final String toReplace;
        private final String replaceWith;

        Replacement(int index, String toReplace, String replaceWith) {
            this.index = index;
            this.toReplace = toReplace;
            this.replaceWith = replaceWith;
        }

        @Override
        public int compareTo(final Replacement o) {
            // Order first by index. This means that replacements earlier in the
            // source string will be processed first
            final int indexComparison = Integer.compare(index, o.index);
            if (indexComparison != 0) {
                return indexComparison;
            }

            // For replacements at the same index, prefer the maximum string. If
            // two strings are found at the same location in a source string, then
            // one string must be a prefix of the other. Prefer the longer as it
            // is more specific
            return -1 * toReplace.compareTo(o.toReplace);
        }

        @Nullable
        public Replacement nextOccurrence(@Nonnull String source) {
            int nextIndex = source.indexOf(toReplace, index + 1);
            return nextIndex < 0 ? null : new Replacement(nextIndex, toReplace, replaceWith);
        }
    }

    /**
     * Replace all occurrences of the keys of the {@code replaceMap} in the {@code source} string with their
     * corresponding value in the map. Elements from the {@code replaceMap} are processed in order in the
     * {@code source} string, so if two keys overlap, the one with the lower index will be processed. For example:
     *
     * <pre>{@code
     *     replaceEach("abc", Map.of("ab", "x", "bc", "y")) = "xc";
     *     replaceEach("abb", Map.of("ab", "x", "bb", "y")) = "xb";
     *     replaceEach("abbb", Map.of("ab", "x", "bb", "y")) = "xy";
     * }</pre>
     *
     * <p>
     * If one replacement key is a prefix of another key, then the <em>longer</em> key will be preferred when
     * the longer string is present. (If the other choice was taken, it would never process the longer key, as
     * any time the longer key is found, the shorter key would also be found.) For example:
     * </p>
     *
     * <pre>{@code
     *     replaceEach("abc", Map.of("ab", "x", "abc", "y")) = "y";
     *     replaceEach("ababc", Map.of("ab", "x", "abc", "y")) = "xy";
     *     replaceEach("abcab", Map.of("ab", "x", "abc", "y")) = "yx";
     * }</pre>
     *
     * @param source string to perform the find and replace on
     * @param replaceMap mapping used to transform the {@code source} string
     * @return a string composed by replacing all occurrences of the {@code replaceMap} keys in {@code source}
     *     with their values
     */
    public static String replaceEach(@Nonnull String source, @Nonnull Map<String, String> replaceMap) {
        if (source.isEmpty() || replaceMap.isEmpty()) {
            return source;
        }
        // Maintain a priority queue of replacements. This first is ordered by location in the source,
        // so we'll process the occurrences of strings in order. Ties are broken by taking the longer
        // string first. Note that if two replaced strings are found at the same index, we retain
        // both in the priority queue (but will only actually replace one). The other element is retained
        // so that we know to search for it later on in the string
        final PriorityQueue<Replacement> replacements = new PriorityQueue<>(replaceMap.size());
        for (Map.Entry<String, String> replaceEntry : replaceMap.entrySet()) {
            final String replaceString = replaceEntry.getKey();
            if (replaceString.isEmpty()) {
                continue;
            }
            int replacementIndex = source.indexOf(replaceString);
            if (replacementIndex >= 0) {
                replacements.add(new Replacement(replacementIndex, replaceString, replaceEntry.getValue()));
            }
        }
        if (replacements.isEmpty()) {
            return source;
        }

        int consumedSoFar = 0;
        final StringBuilder builder = new StringBuilder(source.length());
        while (!replacements.isEmpty()) {
            Replacement replacement = replacements.poll();
            if (replacement.index >= consumedSoFar) {
                if (replacement.index > consumedSoFar) {
                    builder.append(source, consumedSoFar, replacement.index);
                    consumedSoFar = replacement.index;
                }
                consumedSoFar += replacement.toReplace.length();
                builder.append(replacement.replaceWith);
            }

            @Nullable Replacement next = replacement.nextOccurrence(source);
            if (next != null) {
                replacements.add(next);
            }
        }
        if (consumedSoFar < source.length()) {
            builder.append(source.substring(consumedSoFar));
        }
        return builder.toString();
    }
}
