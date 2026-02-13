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

import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.IncludeBlock;
import com.apple.foundationdb.relational.yamltests.block.PreambleBlock;
import com.apple.foundationdb.relational.yamltests.block.SetupBlock;
import com.apple.foundationdb.relational.yamltests.block.TestBlock;
import com.apple.foundationdb.relational.yamltests.block.TransactionSetupsBlock;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import com.apple.foundationdb.relational.yamltests.tags.CustomTag;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CustomYamlConstructor extends SafeConstructor {

    private final List<String> requireLineNumber = new ArrayList<>();

    public CustomYamlConstructor(LoaderOptions loaderOptions) {
        super(loaderOptions);

        // custom tags
        final var loader = ServiceLoaderProvider.load(CustomTag.class);
        loader.forEach(customTag -> customTag.accept(yamlConstructors::put));

        //blocks
        requireLineNumber.add(PreambleBlock.OPTIONS);
        requireLineNumber.add(SetupBlock.SETUP_BLOCK);
        requireLineNumber.add(SetupBlock.SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK);
        requireLineNumber.add(TransactionSetupsBlock.TRANSACTION_SETUP);
        requireLineNumber.add(TestBlock.TEST_BLOCK);
        requireLineNumber.add(IncludeBlock.INCLUDE);
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
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_MAX_ROWS);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_SUPPORTED_VERSION);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_INITIAL_VERSION_LESS_THAN);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_INITIAL_VERSION_AT_LEAST);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_SETUP);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_SETUP_REFERENCE);
        requireLineNumber.add(QueryConfig.QUERY_CONFIG_DEBUGGER);
    }

    @Override
    protected Object constructObject(Node node) {
        if (node instanceof ScalarNode) {
            if (requireLineNumber.stream().anyMatch(key -> key.equals(((ScalarNode) node).getValue()))) {
                return new LinedObject(super.constructObject(node), node.getStartMark().getLine() + 1);
            } else {
                return super.constructObject(node);
            }
        }
        return super.constructObject(node);
    }

    public static final class LinedObject {
        private final Object object;

        private final int lineNumber;

        private LinedObject(final Object object, final int lineNumber) {
            this.object = object;
            this.lineNumber = lineNumber;
        }

        /**
         * Remove the {@link LinedObject} wrappers from keys and create a new map.
         * @param blockMap a map from a yaml file that may have keys that had lines added
         * @return a new map where the keys do not have lines added
         */
        public static Map<?, ?> unlineKeys(Map<?, ?> blockMap) {
            return blockMap.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> {
                                if (entry.getKey() instanceof LinedObject) {
                                    return ((LinedObject)entry.getKey()).getObject();
                                } else {
                                    return entry.getKey();
                                }
                            },
                            Map.Entry::getValue));
        }

        @Nonnull
        public Object getObject() {
            return object;
        }

        public int getLineNumber() {
            return lineNumber;
        }

        public static LinedObject cast(@Nonnull Object obj, @Nonnull Supplier<String> msg) {
            Assert.thatUnchecked(obj instanceof LinedObject, ErrorCode.INTERNAL_ERROR, msg);
            return (LinedObject) obj;
        }

        @Override
        public String toString() {
            return object + "@line:" + lineNumber;
        }
    }
}
