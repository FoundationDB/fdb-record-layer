/*
 * SetupBlock.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 * Implementation of block that serves the purpose of creating the 'environment' needed to run the {@link TestBlock}
 * that follows it. In essence, it consists of a `connectionURI` that is required to connect to a database and an
 * ordered list of `steps` to execute. A `step` is nothing but a query to be executed, translating to a special
 * {@link QueryCommand} that executes but doesn't verify anything.
 * <p>
 * The failure handling in case of {@link SetupBlock} is straight-forward. It {@code throws} downstream exceptions
 * and errors to handled in the consumer. The rationale for this is that if the {@link SetupBlock} fails at step,
 * there is no guarantee **as of now** that some following {@link ConnectedBlock} can run independent of this failure.
 */
@SuppressWarnings({"PMD.AvoidCatchingThrowable"})
public class SetupBlock extends ConnectedBlock {

    public static final String SETUP_BLOCK = "setup";

    protected SetupBlock(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull URI connectionURI,
                         @Nonnull YamlExecutionContext executionContext) {
        super(lineNumber, executables, connectionURI, executionContext);
    }

    @Override
    public void execute() {
        try {
            executeExecutables(executables);
        } catch (Throwable e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️ Failed to execute all the setup steps in Setup block at " + getLineNumber(),
                    SETUP_BLOCK, getLineNumber());
        }
    }

    public static final class ManualSetupBlock extends SetupBlock {

        public static final String MANUAL_SETUP_BLOCK_STEPS = "steps";

        public static SetupBlock parse(int lineNumber, @Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            try {
                final var setupMap = Matchers.map(document, "setup");
                final var stepsObject = setupMap.getOrDefault(MANUAL_SETUP_BLOCK_STEPS, null);
                if (stepsObject == null) {
                    Assert.failUnchecked("Illegal Format: No steps provided in setup block.");
                }
                final var executables = new ArrayList<Consumer<RelationalConnection>>();
                for (final var step : Matchers.arrayList(stepsObject, "setup steps")) {
                    Assert.thatUnchecked(Matchers.map(step, "setup step").size() == 1, "Illegal Format: A setup step should be a single command");
                    final var resolvedCommand = Objects.requireNonNull(Command.parse(List.of(step), executionContext));
                    executables.add(resolvedCommand::execute);
                }
                return new ManualSetupBlock(lineNumber, executables, executionContext.inferConnectionURI(setupMap.getOrDefault(BLOCK_CONNECT, null)),
                        executionContext);
            } catch (Throwable e) {
                throw executionContext.wrapContext(e, () -> "‼️ Error parsing the setup block at " + lineNumber, SETUP_BLOCK, lineNumber);
            }
        }

        private ManualSetupBlock(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull URI connectionURI,
                                 @Nonnull YamlExecutionContext executionContext) {
            super(lineNumber, executables, connectionURI, executionContext);
        }
    }

    public static final class SchemaTemplateBlock extends SetupBlock {

        static final String DOMAIN = "/FRL";
        public static final String SCHEMA_TEMPLATE_BLOCK = "schema_template";

        public static SetupBlock parse(int lineNumber, @Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            try {
                final var identifier = "YAML_" + UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "").substring(0, 16);
                final var schemaTemplateName = identifier + "_TEMPLATE";
                final var databasePath = DOMAIN + "/" + identifier + "_DB";
                final var schemaName = identifier + "_SCHEMA";
                final var steps = new ArrayList<String>();
                steps.add("DROP SCHEMA TEMPLATE IF EXISTS " + schemaTemplateName);
                steps.add("CREATE SCHEMA TEMPLATE " + schemaTemplateName + " " + Matchers.string(document, "schema template description"));
                steps.add("DROP DATABASE IF EXISTS " + databasePath);
                steps.add("CREATE DATABASE " + databasePath);
                steps.add("CREATE SCHEMA " + databasePath + "/" + schemaName + " WITH TEMPLATE " + schemaTemplateName);
                final var executables = new ArrayList<Consumer<RelationalConnection>>();
                for (final var step : steps) {
                    final var resolvedCommand = QueryCommand.withQueryString(lineNumber, step, executionContext);
                    executables.add(resolvedCommand::execute);
                }
                executionContext.registerFinalizeBlock(
                        DestructTemplateBlock.withDatabaseAndSchema(lineNumber, executionContext, schemaTemplateName, databasePath));
                executionContext.registerConnectionURI("jdbc:embed:" + databasePath + "?schema=" + schemaName);
                return new SchemaTemplateBlock(lineNumber, executables, executionContext);
            } catch (Exception e) {
                throw executionContext.wrapContext(e, () -> "‼️ Error parsing the schema_template block at " + lineNumber, SCHEMA_TEMPLATE_BLOCK, lineNumber);
            }
        }

        private SchemaTemplateBlock(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull YamlExecutionContext executionContext) {
            super(lineNumber, executables, executionContext.inferConnectionURI(0), executionContext);
        }
    }

    private static final class DestructTemplateBlock extends SetupBlock {

        public static DestructTemplateBlock withDatabaseAndSchema(int lineNumber, @Nonnull YamlExecutionContext executionContext,
                                                                  @Nonnull String schemaTemplateName, @Nonnull String databasePath) {
            try {
                final var steps = new ArrayList<String>();
                steps.add("DROP DATABASE " + databasePath);
                steps.add("DROP SCHEMA TEMPLATE " + schemaTemplateName);
                final var executables = new ArrayList<Consumer<RelationalConnection>>();
                for (final var step : steps) {
                    final var resolvedCommand = QueryCommand.withQueryString(lineNumber, step, executionContext);
                    executables.add(resolvedCommand::execute);
                }
                return new DestructTemplateBlock(lineNumber, executables, executionContext);
            } catch (Exception e) {
                throw executionContext.wrapContext(e, () -> "‼️ Error creating the destruct_template block for schema_template block at " + lineNumber, SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK, lineNumber);
            }
        }

        private DestructTemplateBlock(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull YamlExecutionContext executionContext) {
            super(lineNumber, executables, executionContext.inferConnectionURI(0), executionContext);
        }
    }
}
