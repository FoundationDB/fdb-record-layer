/*
 * Vector16Field.java
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

        public RandomStrMatcher(@Nonnull final Node node) {
            this.yamlNode = Assert.castUnchecked(node, ScalarNode.class);
        }

        @Override
        public String toString() {
            return yamlNode.getValue();
        }

        @Nonnull
        @Override
        public Matchers.ResultSetMatchResult matches(@Nullable Object other, int rowNumber, @Nonnull String cellRef) {
            final var maybeNull = Matchable.shouldNotBeNull(other, rowNumber, cellRef);
            if (maybeNull.isPresent()) {
                return maybeNull.get();
            }
            final String expected = Matchers.constructRandomString(Assert.castUnchecked(yamlNode, ScalarNode.class));
            if (Verify.verifyNotNull(other).equals(expected)) {
                return Matchers.ResultSetMatchResult.success();
            }
            final String actual = Verify.verifyNotNull(other).toString();
            return Matchable.prettyPrintError(stringDiff(expected, actual), rowNumber, cellRef);
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
