/*
 * CustomYamlConstructor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.Block;
import com.apple.foundationdb.relational.yamltests.block.TestBlock;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.ArrayList;
import java.util.List;

public class CustomYamlConstructor extends SafeConstructor {

    private final List<String> requireLineNumber = new ArrayList<>();

    public CustomYamlConstructor(LoaderOptions loaderOptions) {
        super(loaderOptions);

        yamlConstructors.put(new Tag("!ignore"), new ConstructIgnore());
        yamlConstructors.put(new Tag("!l"), new ConstructLong());
        yamlConstructors.put(new Tag("!sc"), new ConstructStringContains());
        yamlConstructors.put(new Tag("!null"), new ConstructNullPlaceholder());
        yamlConstructors.put(new Tag("!not_null"), new ConstructNotNull());

        //blocks
        requireLineNumber.add(Block.ConfigBlock.CONFIG_BLOCK_SETUP);
        requireLineNumber.add(Block.ConfigBlock.CONFIG_BLOCK_DESTRUCT);
        requireLineNumber.add(TestBlock.TEST_BLOCK);
        // commands
        requireLineNumber.add(Command.COMMAND_LOAD_SCHEMA_TEMPLATE);
        requireLineNumber.add(Command.COMMAND_SET_SCHEMA_STATE);
        requireLineNumber.add(Command.COMMAND_QUERY);
        // query test configs
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_RESULT);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_UNORDERED_RESULT);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_EXPLAIN);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_EXPLAIN_CONTAINS);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_ERROR);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_COUNT);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_PLAN_HASH);
    }

    @Override
    protected Object constructObject(Node node) {
        if (node instanceof ScalarNode) {
            if (requireLineNumber.stream().anyMatch(key -> key.equals(((ScalarNode) node).getValue()))) {
                return new LinedObject(super.constructObject(node), node.getStartMark());
            } else {
                return super.constructObject(node);
            }
        }
        return super.constructObject(node);
    }

    public static final class LinedObject {
        private final Object object;

        private final Mark startMark;

        private LinedObject(final Object object, final Mark startMark) {
            this.object = object;
            this.startMark = startMark;
        }

        public Object getObject() {
            return object;
        }

        public Mark getStartMark() {
            return startMark;
        }

    }

    private static class ConstructIgnore extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            return CustomTag.Ignore.INSTANCE;
        }
    }

    public static class ConstructLong extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            if (!(node instanceof ScalarNode)) {
                Assert.failUnchecked(String.format("The value of the long (!l) tag must be a scalar, however '%s' is found!", node));
            }
            return Long.valueOf(((ScalarNode) node).getValue());
        }
    }

    private static class ConstructStringContains extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            if (!(node instanceof ScalarNode)) {
                Assert.failUnchecked(String.format("The value of the string-contains (!sc) tag must be a scalar, however '%s' is found!", node));
            }
            return new CustomTag.StringContains(((ScalarNode) node).getValue());
        }
    }

    private static class ConstructNullPlaceholder extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            return CustomTag.NullPlaceholder.INSTANCE;
        }
    }

    private static class ConstructNotNull extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            return CustomTag.NotNull.INSTANCE;
        }
    }
}
