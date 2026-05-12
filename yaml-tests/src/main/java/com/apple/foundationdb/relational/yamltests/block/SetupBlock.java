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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersionRanges;

import com.apple.foundationdb.relational.yamltests.ConnectionTarget;

import com.google.common.collect.Range;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    protected SetupBlock(@Nonnull YamlReference reference, @Nonnull List<Consumer<YamlConnection>> executables, @Nonnull ConnectionTarget connectionTarget,
                         @Nonnull YamlExecutionContext executionContext) {
        super(reference, executables, connectionTarget, executionContext);
    }

    @Override
    public void execute() {
        try {
            executeExecutables(executables);
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️ Failed to execute all the setup steps in Setup block at " + getReference(),
                    SETUP_BLOCK, getReference());
        }
    }

    public static final class ManualSetupBlock extends SetupBlock {

        public static final String STEPS = "steps";
        public static final String OPTIONS = "options";
        public static final String CONNECTION_OPTIONS = "connection_options";

        public static List<Block> parse(@Nonnull YamlReference reference, @Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            try {
                Options connectionOptions = Options.none();
                final var setupMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(document, "setup"));
                if (setupMap.get(OPTIONS) != null) {
                    final Map<?, ?> optionsMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(setupMap.get(OPTIONS)));
                    if (optionsMap.containsKey(CONNECTION_OPTIONS)) {
                        connectionOptions = TestBlock.TestBlockOptions.parseConnectionOptions(Matchers.map(optionsMap.get(CONNECTION_OPTIONS)));
                    }
                }

                final var stepsObject = setupMap.getOrDefault(STEPS, null);
                if (stepsObject == null) {
                    Assert.failUnchecked("Illegal Format: No steps provided in setup block.");
                }
                final var executables = new ArrayList<Consumer<YamlConnection>>();
                for (final var step : Matchers.arrayList(stepsObject, "setup steps")) {
                    Assert.thatUnchecked(Matchers.map(step, "setup step").size() == 1, "Illegal Format: A setup step should be a single command");
                    final var resolvedCommand = Objects.requireNonNull(Command.parse(reference.getResource(), List.of(step), "unnamed-setup-block", executionContext));
                    executables.add(createSetupExecutable(resolvedCommand, connectionOptions));
                }
                return List.of(new ManualSetupBlock(reference, executables, executionContext.inferConnectionTarget(reference.getResource(), setupMap.getOrDefault(BLOCK_CONNECT, null)),
                        executionContext));
            } catch (Throwable e) {
                throw YamlExecutionContext.wrapContext(e, () -> "‼️ Error parsing the setup block at " + reference, SETUP_BLOCK, reference);
            }
        }

        private ManualSetupBlock(@Nonnull YamlReference reference, @Nonnull List<Consumer<YamlConnection>> executables, @Nonnull ConnectionTarget connectionTarget,
                                 @Nonnull YamlExecutionContext executionContext) {
            super(reference, executables, connectionTarget, executionContext);
        }

        @Nonnull
        private static Consumer<YamlConnection> createSetupExecutable(Command setupCommand, @Nonnull Options connectionOptions) {
            return connection -> {
                try {
                    connection.setConnectionOptions(connectionOptions);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                setupCommand.execute(connection);
            };
        }
    }

    public static final class SchemaTemplateBlock extends SetupBlock {

        static final String DOMAIN = "/FRL";
        public static final String SCHEMA_TEMPLATE_BLOCK = "schema_template";
        public static final String SCHEMA_TEMPLATE_VARIANT_DEFINITION = "definition";
        public static final String INITIAL_VERSION_AT_LEAST = "initialVersionAtLeast";
        public static final String INITIAL_VERSION_LESS_THAN = "initialVersionLessThan";

        @Nonnull
        private List<Block> finalizingBlocks;


        /**
         * A {@code schema_template} variant specified in the .yamsql file.
         *
         * @param reference  the location of this variant in the source .yamsql file
         * @param range  the semantic version range associated with this variant
         * @param createSchemaTemplateSql  the {@code CREATE SCHEMA TEMPLATE} statement built from the variant’s
         *                                 {@code definition} body
         */
        private record Variant(@Nonnull YamlReference reference, @Nonnull Range<SemanticVersion> range,
                               @Nonnull String createSchemaTemplateSql) {
        }

        /**
         * A resolved {@link Variant} holding the {@link QueryCommand} to execute.
         */
        private record VariantCommands(@Nonnull Range<SemanticVersion> range, @Nonnull QueryCommand command) {
        }

        public static List<Block> parse(@Nonnull final YamlReference reference, @Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
            try {
                final String identifier = "YAML_" + UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "").substring(0, 16);
                final String schemaTemplateName = identifier + "_TEMPLATE";
                final String databasePath = DOMAIN + "/" + identifier + "_DB";
                final String schemaName = identifier + "_SCHEMA";

                final List<Consumer<YamlConnection>> executables = new ArrayList<>();

                // Pre-steps: Wipe anything left over from a prior run.
                executables.add(asExecutable("DROP SCHEMA TEMPLATE IF EXISTS " + schemaTemplateName, reference, executionContext));
                executables.add(asExecutable("DROP DATABASE IF EXISTS " + databasePath, reference, executionContext));

                // Parse the list of variants, depending on which `schema_template` syntax is used.
                final List<Variant> variants = (document instanceof List)
                                               ? parseVariantList(document, reference, schemaTemplateName)
                                               : parseSingleVariant(document, reference, schemaTemplateName);

                // Build the main CREATE SCHEMA TEMPLATE step. This is a special executable that inspects the initial
                // version of the connection at execution time and chooses the matching CREATE SCHEMA TEMPLATE to run.
                final List<VariantCommands> variantCommands = variants.stream()
                        .map(variant -> new VariantCommands(variant.range,
                                QueryCommand.withQueryString(variant.reference, variant.createSchemaTemplateSql, executionContext)))
                        .toList();
                executables.add(connection -> {
                    final SemanticVersion initialVersion = connection.getInitialVersion();
                    final VariantCommands chosen = variantCommands.stream()
                            .filter(v -> v.range.contains(initialVersion))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(
                                    "No schema_template variant matches initial version " + initialVersion + " at " + reference));
                    chosen.command.execute(connection);
                });

                // Post-steps: Create the database and the schema based on the chosen template.
                executables.add(asExecutable("CREATE DATABASE " + databasePath, reference, executionContext));
                executables.add(asExecutable("CREATE SCHEMA " + databasePath + "/" + schemaName + " WITH TEMPLATE " + schemaTemplateName, reference, executionContext));

                executionContext.registerConnectionURI(reference.getResource(), "jdbc:embed:" + databasePath + "?schema=" + schemaName);
                return List.of(new SchemaTemplateBlock(reference, schemaTemplateName, databasePath, executables, executionContext));
            } catch (Exception e) {
                throw YamlExecutionContext.wrapContext(e, () -> "‼️ Error parsing the schema_template block at " + reference, SCHEMA_TEMPLATE_BLOCK, reference);
            }
        }

        @Nonnull
        private static Consumer<YamlConnection> asExecutable(@Nonnull String step, @Nonnull YamlReference reference,
                                                             @Nonnull YamlExecutionContext executionContext) {
            return QueryCommand.withQueryString(reference, step, executionContext)::execute;
        }

        @Nonnull
        private static String buildCreateSchemaTemplateSql(final String schemaTemplateName, final String body) {
            return "CREATE SCHEMA TEMPLATE " + schemaTemplateName + " " + body;
        }

        @Nonnull
        private static List<Variant> parseSingleVariant(@Nonnull Object document, @Nonnull YamlReference reference,
                                                        @Nonnull String schemaTemplateName) {
            final String body = Matchers.string(document, "schema template description");
            return List.of(new Variant(reference, SemanticVersionRanges.all(),
                    buildCreateSchemaTemplateSql(schemaTemplateName, body)));
        }

        @Nonnull
        private static List<Variant> parseVariantList(@Nonnull Object document, @Nonnull YamlReference reference, @Nonnull String schemaTemplateName) {
            final List<?> rawVariants = Matchers.arrayList(document, "schema_template variants");
            Assert.thatUnchecked(!rawVariants.isEmpty(), "schema_template list form must declare at least one variant");
            final List<Variant> parsed = new ArrayList<>(rawVariants.size());
            for (final Object raw : rawVariants) {
                final Map<?, ?> rawVariantMap = Matchers.map(raw, "schema_template variant");
                final YamlReference variantReference = reference.getResource().withLineNumber(extractVariantLineNumber(rawVariantMap, reference));
                final Map<?, ?> variantMap = CustomYamlConstructor.LinedObject.unlineKeys(rawVariantMap);
                final Object definitionObj = variantMap.get(SCHEMA_TEMPLATE_VARIANT_DEFINITION);
                Assert.thatUnchecked(definitionObj != null, "schema_template variant requires a '" + SCHEMA_TEMPLATE_VARIANT_DEFINITION + "' key");
                final String definition = Matchers.string(definitionObj, "schema_template variant definition");
                final Range<SemanticVersion> range = parseVariantRange(variantMap);
                parsed.add(new Variant(variantReference, range, buildCreateSchemaTemplateSql(schemaTemplateName, definition)));
            }
            validateVariants(parsed, reference);
            return parsed;
        }

        private static int extractVariantLineNumber(@Nonnull Map<?, ?> rawVariantMap, @Nonnull YamlReference outerReference) {
            return rawVariantMap.keySet().stream()
                    .filter(CustomYamlConstructor.LinedObject.class::isInstance)
                    .mapToInt(k -> ((CustomYamlConstructor.LinedObject) k).getLineNumber())
                    .min()
                    .orElse(outerReference.getLineNumber());
        }

        @Nonnull
        private static Range<SemanticVersion> parseVariantRange(@Nonnull Map<?, ?> variantMap) {
            final boolean hasAtLeast = variantMap.containsKey(INITIAL_VERSION_AT_LEAST);
            final boolean hasLessThan = variantMap.containsKey(INITIAL_VERSION_LESS_THAN);
            if (!hasAtLeast && !hasLessThan) {
                return SemanticVersionRanges.all();
            }
            SemanticVersion lowerBound = SemanticVersion.min();
            SemanticVersion upperBound = SemanticVersion.max();
            if (hasAtLeast) {
                lowerBound = PreambleBlock.parseVersion(variantMap.get(INITIAL_VERSION_AT_LEAST));
            }
            if (hasLessThan) {
                upperBound = PreambleBlock.parseVersion(variantMap.get(INITIAL_VERSION_LESS_THAN));
            }
            Assert.thatUnchecked(lowerBound.compareTo(upperBound) < 0,
                    "schema_template_by_version variant has empty version range [" + lowerBound + ", " + upperBound + ")");
            return Range.closedOpen(lowerBound, upperBound);
        }

        private static void validateVariants(@Nonnull List<Variant> variants, @Nonnull YamlReference reference) {
            final List<Range<SemanticVersion>> ranges = variants.stream().map(Variant::range).collect(Collectors.toList());

            // Check for overlapping ranges.
            final Set<Range<SemanticVersion>> overlapping = SemanticVersionRanges.overlapping(ranges);
            if (!overlapping.isEmpty()) {
                final IllegalArgumentException e = new IllegalArgumentException(
                        "schema_template variants have overlapping version ranges: " + overlapping);
                throw YamlExecutionContext.wrapContext(e,
                        () -> "‼️ Overlapping schema_template variants at " + reference, SCHEMA_TEMPLATE_BLOCK, reference);
            }

            // Check for uncovered ranges.
            final Set<Range<SemanticVersion>> uncovered = SemanticVersionRanges.uncovered(ranges);
            if (!uncovered.isEmpty()) {
                final IllegalArgumentException e = new IllegalArgumentException(
                        "schema_template variants do not cover the complete set of versions; missing: " + uncovered);
                throw YamlExecutionContext.wrapContext(e,
                        () -> "‼️ Non-comprehensive schema_template variants at " + reference, SCHEMA_TEMPLATE_BLOCK, reference);
            }
        }

        private SchemaTemplateBlock(@Nonnull final YamlReference reference, @Nonnull final String schemaTemplateName,
                                    @Nonnull final String databaseName, @Nonnull List<Consumer<YamlConnection>> executables,
                                    @Nonnull YamlExecutionContext executionContext) {
            super(reference, executables, executionContext.inferConnectionTarget(reference.getResource(), 0), executionContext);
            this.finalizingBlocks = List.of(DestructTemplateBlock.withDatabaseAndSchema(reference, executionContext, schemaTemplateName, databaseName));
        }

        @Override
        @Nonnull
        public List<Block> getAndClearFinalizingBlocks() {
            final var toReturn = List.copyOf(finalizingBlocks);
            finalizingBlocks = List.of();
            return toReturn;
        }
    }

    private static final class DestructTemplateBlock extends SetupBlock {

        public static DestructTemplateBlock withDatabaseAndSchema(@Nonnull final YamlReference reference, @Nonnull YamlExecutionContext executionContext,
                                                                  @Nonnull String schemaTemplateName, @Nonnull String databasePath) {
            try {
                final var steps = new ArrayList<String>();
                steps.add("DROP DATABASE " + databasePath);
                steps.add("DROP SCHEMA TEMPLATE " + schemaTemplateName);
                final var executables = new ArrayList<Consumer<YamlConnection>>();
                for (final var step : steps) {
                    final var resolvedCommand = QueryCommand.withQueryString(reference, step, executionContext);
                    executables.add(resolvedCommand::execute);
                }
                return new DestructTemplateBlock(reference, executables, executionContext);
            } catch (Exception e) {
                throw YamlExecutionContext.wrapContext(e, () -> "‼️ Error creating the destruct_template block for schema_template block at " + reference, SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK, reference);
            }
        }

        private DestructTemplateBlock(@Nonnull final YamlReference reference, @Nonnull List<Consumer<YamlConnection>> executables, @Nonnull YamlExecutionContext executionContext) {
            super(reference, executables, executionContext.inferConnectionTarget(reference.getResource(), 0), executionContext);
        }
    }
}
