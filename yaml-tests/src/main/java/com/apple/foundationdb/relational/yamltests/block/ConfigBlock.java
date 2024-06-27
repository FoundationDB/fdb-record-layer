/*
 * ConfigBlock.java
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/**
 * Implementation of block that serves the purpose of creating the 'environment' needed to run the {@link TestBlock}
 * that follows it. In essence, it consists of a `connectPath` that is required to connect to a database and an
 * ordered list of `steps` to execute. A `step` is nothing but a query to be executed, translating to a special
 * {@link QueryCommand} that executes but doesn't verify anything.
 * <p>
 * The failure handling in case of {@link com.apple.foundationdb.relational.yamltests.block.ConfigBlock} is straight-forward. It {@code throws} downstream exceptions
 * and errors to handled in the consumer. The rationale for this is that if the {@link com.apple.foundationdb.relational.yamltests.block.ConfigBlock} fails at step,
 * there is no guarantee **as of now** that some following {@link Block} can run independent of this failure.
 */
public class ConfigBlock extends Block {

    protected ConfigBlock(int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        super(lineNumber, executionContext);
    }

    @Override
    public void execute() {
        executeExecutables(executables);
        if (getFailureExceptionIfPresent().isPresent()) {
            throw getFailureExceptionIfPresent().get();
        }
    }

    public static class ManualConfigBlock extends ConfigBlock {

        public static final String MANUAL_CONFIG_BLOCK_STEPS = "steps";
        public static final String MANUAL_CONFIG = "config";

        ManualConfigBlock(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            super((((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "config").getKey()).getStartMark().getLine() + 1), executionContext);
            final var configMap = Matchers.map(Matchers.firstEntry(document, "config").getValue());
            setConnectPath(configMap.getOrDefault(BLOCK_CONNECT, null));
            final var steps = getSteps(configMap.getOrDefault(MANUAL_CONFIG_BLOCK_STEPS, null));
            for (final var step : steps) {
                Assert.thatUnchecked(Matchers.map(step).size() == 1, "Illegal Format: A configuration step should be a single command");
                final var resolvedCommand = Objects.requireNonNull(Command.parse(List.of(step)));
                executables.add(connection -> {
                    try {
                        resolvedCommand.invoke(connection, executionContext);
                    } catch (Exception e) {
                        failureException.set(new RuntimeException(String.format("‼️ Error executing step config block step at line %d",
                                resolvedCommand.getLineNumber()), e));
                    }
                });
            }
            executionContext.registerBlock(this);
        }

        private List<?> getSteps(Object steps) {
            if (steps == null) {
                Assert.failUnchecked("Illegal Format: No steps provided in block at line " + lineNumber);
            }
            return Matchers.arrayList(steps);
        }
    }

    public static class SchemaTemplateBlock extends ConfigBlock {

        static final String CATALOG_PATH = "jdbc:embed:/__SYS?schema=CATALOG";
        static final String DOMAIN = "/FRL";
        public static final String DEFINE_TEMPLATE_BLOCK = "schema_template";

        SchemaTemplateBlock(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            super((((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "config").getKey()).getStartMark().getLine() + 1), executionContext);
            setConnectPath(CATALOG_PATH);
            final var identifier = "YAML_" + UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "").substring(0, 16);
            final var schemaTemplateName = identifier + "_TEMPLATE";
            final var databasePath = DOMAIN + "/" + identifier + "_DB";
            final var schemaName = identifier + "_SCHEMA";
            final var steps = new ArrayList<String>();
            steps.add("DROP SCHEMA TEMPLATE IF EXISTS " + schemaTemplateName);
            steps.add("CREATE SCHEMA TEMPLATE " + schemaTemplateName + " " + Matchers.string(Matchers.firstEntry(document, "config").getValue()));
            steps.add("DROP DATABASE IF EXISTS " + databasePath);
            steps.add("CREATE DATABASE " + databasePath);
            steps.add("CREATE SCHEMA " + databasePath + "/" + schemaName + " WITH TEMPLATE " + schemaTemplateName);
            for (final var step : steps) {
                final var resolvedCommand = new QueryCommand(getLineNumber(), step);
                executables.add(connection -> {
                    try {
                        resolvedCommand.invoke(connection, executionContext);
                    } catch (Exception e) {
                        failureException.set(new RuntimeException(String.format("‼️ Error executing config step at line %d",
                                resolvedCommand.getLineNumber()), e));
                    }
                });
            }
            executionContext.registerBlock(this);
            new DestructTemplateBlock(getLineNumber(), executionContext, schemaTemplateName, databasePath);
            executionContext.registerConnectionPath("jdbc:embed:" + databasePath + "?schema=" + schemaName);
        }
    }

    private static class DestructTemplateBlock extends ConfigBlock {
        DestructTemplateBlock(int lineNumber, @Nonnull YamlExecutionContext executionContext, @Nonnull String schemaTemplateName,
                              @Nonnull String databasePath) {
            super(lineNumber, executionContext);
            setConnectPath(SchemaTemplateBlock.CATALOG_PATH);
            final var steps = new ArrayList<String>();
            steps.add("DROP DATABASE " + databasePath);
            steps.add("DROP SCHEMA TEMPLATE " + schemaTemplateName);
            for (final var step : steps) {
                final var resolvedCommand = new QueryCommand(getLineNumber(), step);
                executables.add(connection -> {
                    try {
                        resolvedCommand.invoke(connection, executionContext);
                    } catch (Exception e) {
                        failureException.set(new RuntimeException(String.format("‼️ Error executing config step at line %d",
                                resolvedCommand.getLineNumber()), e));
                    }
                });
            }
            executionContext.registerFinalizeBlock(this);
        }
    }
}
