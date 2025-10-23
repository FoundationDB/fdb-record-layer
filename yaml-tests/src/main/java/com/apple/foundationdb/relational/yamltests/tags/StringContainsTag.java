/*
 * StringContains.java
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
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@AutoService(CustomTag.class)
public final class StringContainsTag implements CustomTag {

    @Nonnull
    private static final Tag tag = new Tag("!sc");

    @Nonnull
    private static final Construct CONSTRUCT_INSTANCE = new AbstractConstruct() {
        @Override
        public Matchable construct(final Node node) {
            if (!(node instanceof ScalarNode)) {
                Assert.failUnchecked("The value of the string-contains (!sc) tag must be a scalar, however '" + node + "' is found!");
            }
            return new StringContainsMatcher(((ScalarNode) node).getValue());
        }
    };

    public StringContainsTag() {
    }

    @Nonnull
    @Override
    public Tag getTag() {
        return tag;
    }

    @Nonnull
    @Override
    public Construct getConstruct() {
        return CONSTRUCT_INSTANCE;
    }

    public static final class StringContainsMatcher implements Matchable {
        @Nonnull
        private final String value;

        public StringContainsMatcher(@Nonnull final String value) {
            this.value = value;
        }

        @Nonnull
        public String getValue() {
            return value;
        }

        @Nonnull
        @Override
        public Matchers.ResultSetMatchResult matches(@Nullable final Object other, int rowNumber, @Nonnull final String cellRef) {
            final var maybeNull = Matchable.shouldNotBeNull(other, rowNumber, cellRef);
            if (maybeNull.isPresent()) {
                return maybeNull.get();
            }
            if (other instanceof String) {
                final var otherStr = (String)other;
                if (otherStr.contains(value)) {
                    return Matchers.ResultSetMatchResult.success();
                } else {

                    return Matchable.prettyPrintError("expected string containing '" + value + "' instead got '" + other + "'", rowNumber, cellRef);
                }
            }
            return Matchable.prettyPrintError("expected to match against a String value, however we got " + other + " of type " + other.getClass().getSimpleName(),
                    rowNumber, cellRef);
        }
    }
}
