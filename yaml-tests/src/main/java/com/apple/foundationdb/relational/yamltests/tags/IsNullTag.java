/*
 * NullPlaceholder.java
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

import com.apple.foundationdb.relational.yamltests.Matchers;
import com.google.auto.service.AutoService;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@AutoService(CustomTag.class)
public final class IsNullTag implements CustomTag {

    @Nonnull
    private static final Tag tag = new Tag("!null");

    @Nonnull
    private static final Matchable INSTANCE = new IsNullMatcher();

    @Nonnull
    private static final Construct CONSTRUCT_INSTANCE = new AbstractConstruct() {
        @Override
        public Object construct(Node node) {
            return INSTANCE;
        }
    };

    public IsNullTag() {
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

    @Nonnull
    public static String usage() {
        return tag + " _";
    }

    public static final class IsNullMatcher implements Matchable {
        @Nonnull
        @Override
        public Matchers.ResultSetMatchResult matches(@Nullable final Object other, final int rowNumber, @Nonnull final String cellRef) {
            if (other == null) {
                return Matchers.ResultSetMatchResult.success();
            }
            return Matchable.prettyPrintError("expected NULL, got '" + other + "' instead", rowNumber, cellRef);
        }

        @Override
        public String toString() {
            return tag.getValue();
        }
    }
}
