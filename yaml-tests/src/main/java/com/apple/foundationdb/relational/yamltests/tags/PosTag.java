/*
 * IgnoreColumnNameTag.java
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
import com.google.auto.service.AutoService;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;

/**
 * YAML tag for ignoring column name validation in result sets.
 *
 * Usage: !pos {position of the column, 1-based}
 */
@AutoService(CustomTag.class)
public final class PosTag implements CustomTag {

    @Nonnull
    private static final Tag tag = new Tag("!pos");

    @Nonnull
    private static final Construct CONSTRUCT_INSTANCE = new AbstractConstruct() {
        @Override
        public Object construct(Node node) {
            if (!(node instanceof ScalarNode)) {
                Assert.failUnchecked("The value of " + tag.getValue() + " tag must be a scalar, however '" + node + "' is found!");
            }
            final var scalarNode = (ScalarNode)node;
            return new ColumnPosition(Integer.valueOf((scalarNode.getValue())));
        }
    };

    public PosTag() {
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

    public static final class ColumnPosition {
        @Nonnull
        private final Integer columnPosition;

        public ColumnPosition(@Nonnull final Integer columnPosition) {
            this.columnPosition = columnPosition;
        }

        @Nonnull
        public Integer getValue() {
            return columnPosition;
        }
    }
}
