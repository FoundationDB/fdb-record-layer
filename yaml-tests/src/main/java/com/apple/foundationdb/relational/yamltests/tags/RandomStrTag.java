/*
 * RandomStrTag.java
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

package com.apple.foundationdb.relational.yamltests.tags;

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;

/**
 * YAML custom tag {@code !randomStr} that generates a deterministic random alphanumeric string
 * from a descriptor specifying a seed and a length.
 *
 * <h3>Syntax</h3>
 * <pre>{@code
 * !randomStr seed <N> length <M>
 * }</pre>
 * where {@code N} is the {@code long} seed and {@code M} is the desired string length.
 *
 * <h3>Usage in queries (parameter injection)</h3>
 * <pre>{@code
 * INSERT INTO t (col) VALUES (!! !randomStr seed 42 length 500 !!)
 * }</pre>
 * The tag is resolved once at parse time, so the same descriptor always produces the same string,
 * making tests fully reproducible across runs.
 *
 * <h3>Usage in expected results</h3>
 * <pre>{@code
 * - col: !randomStr seed 42 length 500
 * }</pre>
 * The generated string is compared against the actual column value returned by the query.
 * On mismatch, the error message reports the lengths of both strings and a ±10-character
 * context snippet around the first differing character, avoiding printing the full string.
 *
 * <p>The character set used for generation is {@code a-z0-9}.
 *
 * @see com.apple.foundationdb.relational.yamltests.RandomStringParser
 */
@AutoService(CustomTag.class)
public class RandomStrTag implements CustomTag {

    @Nonnull
    private static final Tag tag = new Tag("!randomStr");

    @Nonnull
    @Override
    public Tag getTag() {
        return tag;
    }

    @Nonnull
    private static final Construct CONSTRUCT_INSTANCE = new AbstractConstruct() {
        @Override
        @Nonnull
        public Matchable construct(Node node) {
            return new RandomStrMatcher(node);
        }
    };

    @Nonnull
    @Override
    public Construct getConstruct() {
        return CONSTRUCT_INSTANCE;
    }

    public static final class RandomStrMatcher implements Matchable {
        private static final int DIFF_CONTEXT = 10;

        @Nonnull
        private final ScalarNode yamlNode;

        @Nonnull
        private final String randomString;

        public RandomStrMatcher(@Nonnull final Node node) {
            this.yamlNode = Assert.castUnchecked(node, ScalarNode.class);
            this.randomString = Matchers.constructRandomString(yamlNode);
        }

        @Override
        public String toString() {
            return tag + " " + yamlNode.getValue() + " → " + Matchers.limitString(randomString);
        }

        @Nonnull
        @Override
        public Matchers.ResultSetMatchResult matches(@Nullable Object other, int rowNumber, @Nonnull String cellRef) {
            final var maybeNull = Matchable.shouldNotBeNull(other, rowNumber, cellRef);
            if (maybeNull.isPresent()) {
                return maybeNull.get();
            }
            if (Verify.verifyNotNull(other).equals(randomString)) {
                return Matchers.ResultSetMatchResult.success();
            }
            final String actual = Verify.verifyNotNull(other).toString();
            return Matchable.prettyPrintError(stringDiff(randomString, actual), rowNumber, cellRef);
        }

        @Nonnull
        private static String stringDiff(@Nonnull final String expected, @Nonnull final String actual) {
            final int expLen = expected.length();
            final int actLen = actual.length();
            int firstDiff = -1;
            final int minLen = Math.min(expLen, actLen);
            for (int i = 0; i < minLen; i++) {
                if (expected.charAt(i) != actual.charAt(i)) {
                    firstDiff = i;
                    break;
                }
            }
            if (firstDiff == -1) {
                firstDiff = minLen; // identical content but different lengths
            }
            final int start = Math.max(0, firstDiff - DIFF_CONTEXT);
            final String prefix = start > 0 ? "..." : "";
            final String expSnippet = prefix + expected.substring(start, Math.min(expLen, firstDiff + DIFF_CONTEXT + 1)) + (firstDiff + DIFF_CONTEXT + 1 < expLen ? "..." : "");
            final String actSnippet = prefix + actual.substring(start, Math.min(actLen, firstDiff + DIFF_CONTEXT + 1)) + (firstDiff + DIFF_CONTEXT + 1 < actLen ? "..." : "");
            final String marker = " ".repeat(prefix.length() + (firstDiff - start)) + "^";
            return String.format(Locale.ROOT,
                    "String mismatch (expected length: %d, actual length: %d), first difference at index %d:%n" +
                    "  expected: '%s'%n" +
                    "  actual:   '%s'%n" +
                    "             %s",
                    expLen, actLen, firstDiff, expSnippet, actSnippet, marker);
        }
    }
}
