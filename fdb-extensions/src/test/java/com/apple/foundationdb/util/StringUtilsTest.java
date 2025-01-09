/*
 * StringUtilsTest.java
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

import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the {@link StringUtils} utility class.
 */
public class StringUtilsTest {
    static Stream<Arguments> isNumeric() {
        return Stream.of(
                Arguments.of("", false),
                Arguments.of("1", true),
                Arguments.of("0", true),
                Arguments.of("12345", true),
                Arguments.of("01", true),
                Arguments.of("42L", false),
                Arguments.of("3.14", false),
                Arguments.of("3.14f", false),
                Arguments.of("four", false),
                Arguments.of(" 7", false),
                Arguments.of("123", true),
                Arguments.of("_123", false)
        );
    }

    @ParameterizedTest(name = "isNumeric[{0}]")
    @MethodSource
    void isNumeric(String s, boolean expectedNumeric) {
        assertEquals(expectedNumeric, StringUtils.isNumeric(s),
                () -> "string \"" + s + "\" should " + (expectedNumeric ? "" : "not ") + "be numeric");
    }

    @ParameterizedTest(name = "isNumeric[{0}]")
    @MethodSource("isNumeric")
    void numericSubStrings(String s, boolean expectedNumeric) {
        for (int i = 0; i <= s.length(); i++) {
            final int beginIndex = i;
            String suffix = s.substring(beginIndex);
            if (suffix.isEmpty()) {
                assertFalse(StringUtils.isNumeric(s, beginIndex), "empty string should not be numeric");
            } else if (expectedNumeric) {
                assertTrue(StringUtils.isNumeric(s, beginIndex), "non-empty substring of numeric string should still be numeric");
            } else {
                assertEquals(StringUtils.isNumeric(suffix), StringUtils.isNumeric(s, beginIndex),
                        () -> "string \"" + s + "\" should be numeric from index " + beginIndex + " if and only if \"" + suffix + "\" is numeric");
            }

            for (int j = i; j <= s.length(); j++) {
                final int endIndex = j;
                String infix = s.substring(beginIndex, endIndex);
                if (infix.isEmpty()) {
                    assertFalse(StringUtils.isNumeric(s, beginIndex, endIndex), "empty string should not be numeric");
                } else if (expectedNumeric) {
                    assertTrue(StringUtils.isNumeric(s, beginIndex, endIndex), "non-empty substring of numeric string should still be numeric");
                } else {
                    assertEquals(StringUtils.isNumeric(infix), StringUtils.isNumeric(s, beginIndex, endIndex),
                            () -> "string \"" + s + "\" should be numeric from indexes " + beginIndex + " to " + endIndex + " if and only if \"" + infix + "\" is numeric");
                }
            }
        }
    }

    static Stream<Arguments> numericStringsOutOfBounds() {
        return Stream.of(
                Arguments.of(0, 4),
                Arguments.of(-1, 2),
                Arguments.of(2, 1),
                Arguments.of(-1, 4)
        );
    }

    @ParameterizedTest(name = "numericStringsOutOfBounds[begin={0}, end={1}]")
    @MethodSource
    void numericStringsOutOfBounds(int beginIndex, int endIndex) {
        assertThrows(IllegalArgumentException.class, () -> StringUtils.isNumeric("abc", beginIndex, endIndex));
    }

    static Stream<Arguments> repeat() {
        int supplementalCodePoint = 0x1f60e; // codepoint for smiling face with sunglasses, which has special UTF-16 encoding
        char highSurrogate = Character.highSurrogate(supplementalCodePoint);
        char lowSurrogate = Character.lowSurrogate(supplementalCodePoint);
        assertEquals("😎", "" + highSurrogate + lowSurrogate);

        return Stream.of(
                Arguments.of('x', -2),
                Arguments.of('x', -1),
                Arguments.of('x', 0),
                Arguments.of('x', 1),
                Arguments.of('x', 2),
                Arguments.of('x', 3),
                Arguments.of('\0', 0),
                Arguments.of('\0', 1),
                Arguments.of('\0', 2),
                Arguments.of('\n', 3),
                Arguments.of('\t', 4),
                Arguments.of('é', 5),
                Arguments.of('Δ', 3),
                Arguments.of('≠', 1),
                Arguments.of('ツ', 4),
                Arguments.of('ㅋ', 10),
                Arguments.of('人', 2),
                Arguments.of(highSurrogate, 0),
                Arguments.of(highSurrogate, 1),
                Arguments.of(highSurrogate, 2),
                Arguments.of(lowSurrogate, 0),
                Arguments.of(lowSurrogate, 1),
                Arguments.of(lowSurrogate, 2),
                Arguments.of('ﬃ', 3)
        );
    }

    @ParameterizedTest(name = "repeat[c={0}, n={1}]")
    @MethodSource
    void repeat(char c, int n) {
        final String r = StringUtils.repeat(c, n);
        assertEquals(Math.max(0, n), r.length());
        for (int i = 0; i < r.length(); i++) {
            assertEquals(c, r.charAt(i));
        }
        // Should be valid UTF-16 unless we are given a surrogate character
        CharsetEncoder encoder = StandardCharsets.UTF_16.newEncoder();
        assertEquals(n <= 0 || !Character.isSurrogate(c), encoder.canEncode(r));
    }

    @Test
    void replaceEmptyMap() {
        final String source = "foo";
        final String sink = StringUtils.replaceEach(source, ImmutableMap.of());
        assertSame(sink, source);
    }

    @Test
    void replaceEmptyString() {
        final String source = "";
        final String sink = StringUtils.replaceEach(source, ImmutableMap.of("a", "b"));
        assertSame(sink, source);
    }

    @Test
    void replaceNoOccurrences() {
        final String source = "abcdefg";
        final String sink = StringUtils.replaceEach(source, ImmutableMap.of("h", "i", "j", "k"));
        assertSame(sink, source);
    }

    static Stream<Arguments> replacements() {
        final Map<String, String> patternToRegexMap = ImmutableMap.of(
                "\\%", "%",
                "\\_", "_",
                "%", ".*",
                "_", ".",
                "|", "\\|",
                "$", "\\$",
                "\\", "\\\\",
                "(", "\\(",
                ")", "\\)"
        );
        return Stream.of(
                Arguments.of("abcabcabc", Map.of("b", "z"), "azcazcazc"),
                Arguments.of("abcabcabc", Map.of("b", ""), "acacac"),
                Arguments.of("abcabcabc", Map.of("b", "de"), "adecadecadec"),
                Arguments.of("abcabcabc", Map.of("ab", "g"), "gcgcgc"),
                Arguments.of("abcabcabc", Map.of("bc", "g"), "agagag"),
                Arguments.of("abcabcabc", Map.of("", "foo"), "abcabcabc"),
                Arguments.of("bbbbbbbbbb", Map.of("bb", "xb"), "xbxbxbxbxb"),
                Arguments.of("ababab suffix", Map.of("ab", "r"), "rrr suffix"),
                Arguments.of("abbbbbabbbbbb", ImmutableMap.of("ab", "x", "bb", "y"), "xyyxyyb"),
                Arguments.of("abbbbbabbbbbb", ImmutableMap.of("bb", "y", "ab", "x"), "xyyxyyb"),
                Arguments.of("withOverlap: ab bc abc", ImmutableMap.of("ab", "x", "bc", "y"), "withOverlap: x y xc"),
                Arguments.of("withOverlap: ab bc abc", ImmutableMap.of("bc", "y", "ab", "x"), "withOverlap: x y xc"),
                Arguments.of("withOverlap: ab bc abc", ImmutableMap.of("ab", "xb", "bc", "y"), "withOverlap: xb y xbc"),
                Arguments.of("withOverlap: abc ab bc", ImmutableMap.of("ab", "x", "bc", "y"), "withOverlap: xc x y"),
                Arguments.of("withPrefix: ab abc", ImmutableMap.of("ab", "x", "abc", "y"), "withPrefix: x y"),
                Arguments.of("withPrefix: ab abc", ImmutableMap.of("abc", "y", "ab", "x"), "withPrefix: x y"),
                Arguments.of("withPrefix: abc ab", ImmutableMap.of("ab", "x", "abc", "y"), "withPrefix: y x"),
                Arguments.of("withPrefix: abc ab", ImmutableMap.of("abc", "y", "ab", "x"), "withPrefix: y x"),
                Arguments.of("not repeated: abcdef", ImmutableMap.of("a", "b", "b", "c", "c", "d"), "not repebted: bcddef"),
                Arguments.of("_like%", patternToRegexMap, ".like.*"),
                Arguments.of("($40)", patternToRegexMap, "\\(\\$40\\)"),
                Arguments.of("50\\%%", patternToRegexMap, "50%.*"),
                Arguments.of("a\\_or\\__", patternToRegexMap, "a_or_."),
                Arguments.of("\\$", patternToRegexMap, "\\\\\\$"),
                Arguments.of("\\\\_", patternToRegexMap, "\\\\_"),

                // From the Javadoc comments:
                Arguments.of("abc", Map.of("ab", "x", "bc", "y"), "xc"),
                Arguments.of("abb", Map.of("ab", "x", "bb", "y"), "xb"),
                Arguments.of("abbb", Map.of("ab", "x", "bb", "y"), "xy"),
                Arguments.of("abc", Map.of("ab", "x", "abc", "y"), "y"),
                Arguments.of("ababc", Map.of("ab", "x", "abc", "y"), "xy"),
                Arguments.of("abcab", Map.of("ab", "x", "abc", "y"), "yx")
        );
    }

    @ParameterizedTest(name = "replacements[source={0}, replaceMap={1}, expected={2}]")
    @MethodSource
    void replacements(final String source, final Map<String, String> replaceMap, final String expected) {
        final String sink = StringUtils.replaceEach(source, replaceMap);
        assertEquals(expected, sink, "Our implementation had mismatched replacement");
    }

    static Stream<Arguments> containsIgnoreCase() {
        return Stream.of(
                Arguments.of("hello", "", true),
                Arguments.of("hello", "lo", true),
                Arguments.of("hello", "Ll", true),
                Arguments.of("hello", "lL", true),
                Arguments.of("hello", "li", false),
                Arguments.of("hello", "hello", true),
                Arguments.of("hello", "hElLo", true),
                Arguments.of("hello", "hElLos", false),

                // Turkish İ/i and I/ı have special equivalency rules
                Arguments.of("hayır", "ır", true),
                Arguments.of("hayır", "ir", true),
                Arguments.of("hayır", "IR", true),
                Arguments.of("hayır", "İR", true),
                Arguments.of("HAYIR", "ır", true),
                Arguments.of("HAYIR", "ir", true),
                Arguments.of("HAYIR", "IR", true),
                Arguments.of("HAYIR", "İR", true),
                Arguments.of("nasilsin", "sil", true),
                Arguments.of("nasilsin", "sıl", true),
                Arguments.of("nasilsin", "SİL", true),
                Arguments.of("nasilsin", "SIL", true),
                Arguments.of("NASİLSİN", "sil", true),
                Arguments.of("NASİLSİN", "sıl", true),
                Arguments.of("NASİLSİN", "SİL", true),
                Arguments.of("NASİLSİN", "SIL", true),

                // Greek
                Arguments.of("Νάξος", "αξ", false), // tonos is checked during equality
                Arguments.of("Νάξος", "άξ", true),
                Arguments.of("Νάξος", "ΑΞ", false),
                Arguments.of("Νάξος", "ΆΞ", true),
                Arguments.of("Νάξος", "ος", true),
                Arguments.of("Νάξος", "ΟΣ", true),
                Arguments.of("Νάξος", "Οσ", true), // final sigma ς is equivalent to non-final σ
                Arguments.of("Νάξος", "νΆΞΟσ", true),
                Arguments.of("Νάξος", "Η νΆΞΟσ", false),
                Arguments.of("η Νάξος", "νΆΞΟσ", true),

                Arguments.of("你好", "好", true),
                Arguments.of("你好", "你", true),
                Arguments.of("你好", "你好", true),
                Arguments.of("你好", "你好吗", false)
        );
    }

    @ParameterizedTest(name = "containsIgnoreCase[source={0}, searchString={1}]")
    @MethodSource
    void containsIgnoreCase(@Nonnull String source, @Nonnull String searchString, boolean expected) {
        assertEquals(expected, StringUtils.containsIgnoreCase(source, searchString),
                () -> "string \"" + source + "\" should " + (expected ? "" : "not ") + "contain \"" + searchString + "\" ignoring case");
    }

    static Stream<Long> containsAllSubstringsIgnoreCase() {
        return RandomizedTestUtils.randomSeeds(12345, 987654, 423, 18378195);
    }

    @ParameterizedTest(name = "containsAllSubstringsIgnoreCase[seed={0}]")
    @MethodSource
    void containsAllSubstringsIgnoreCase(long seed) {
        final Random r = new Random(seed);
        int length = r.nextInt(20) + 1;
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            char c;
            // Construct a string of random characters. Prefer characters
            // from scripts with both upper and lower cases
            double choice = r.nextDouble();
            if (choice < 0.4) {
                // Random Latin (extended plane)
                c = (char) r.nextInt(0x0250);
            } else if (choice < 0.8) {
                // Random Greek, Coptic, Cyrillic, or Armenian
                c = (char) (r.nextInt((0x0590 - 0x0370)) + 0x0370);
            } else {
                // Random character
                c = (char) r.nextInt(Character.MAX_CODE_POINT);
            }
            chars[i] = c;
        }
        String s = new String(chars);

        for (int i = 0; i <= s.length(); i++) {
            for (int j = i; j <= s.length(); j++) {
                char[] substringChars = s.substring(i, j).toCharArray();
                for (int k = 0; k < substringChars.length; k++) {
                    char c = substringChars[k];
                    if (r.nextBoolean()) {
                        // Flip the case of a random assortment of characters
                        if (Character.isUpperCase(c)) {
                            substringChars[k] = Character.toLowerCase(c);
                        } else if (Character.isLowerCase(c)) {
                            substringChars[k] = Character.toUpperCase(c);
                        }
                    }
                }

                String substring = new String(substringChars);
                assertTrue(StringUtils.containsIgnoreCase(s, substring),
                        () -> "string \"" + s + "\" should contain substring \"" + substring + "\" ignoring case");
            }
        }
    }
}
