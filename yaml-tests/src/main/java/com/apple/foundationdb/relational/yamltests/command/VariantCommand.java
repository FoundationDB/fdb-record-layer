/*
 * VariantCommand.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.block.PreambleBlock;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersionRanges;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public final class VariantCommand extends Command {

    public static final String CURRENT_VERSION_AT_LEAST = "currentVersionAtLeast";
    public static final String CURRENT_VERSION_LESS_THAN = "currentVersionLessThan";
    public static final String VARIANT_DEFINITION = "definition";

    @Nonnull
    private final List<SingleVariant> variantCommands;

    private VariantCommand(@Nonnull final YamlReference reference,
                   @Nonnull YamlExecutionContext executionContext,
                   @Nonnull List<SingleVariant> variantCommands) {
        super(reference, executionContext);
        this.variantCommands = variantCommands;
    }

    @Nonnull
    public static VariantCommand parseVariantCommand(@Nonnull final Object document,
                                                     @Nonnull final YamlReference reference,
                                                     @Nonnull final String identifier,
                                                     @Nonnull final YamlExecutionContext executionContext,
                                                     @Nonnull final BiFunction<Object, YamlReference, Command> commandSupplier) {
        final List<?> rawVariants = Matchers.arrayList(document, "command variants");
        Assert.thatUnchecked(!rawVariants.isEmpty(), "variant command must declare at least one variant");

        final ImmutableList.Builder<SingleVariant> parsedVariantsBuilder = ImmutableList.builderWithExpectedSize(rawVariants.size());
        final var allowedVariantMapKeys = ImmutableSet.of(CURRENT_VERSION_AT_LEAST, CURRENT_VERSION_LESS_THAN, VARIANT_DEFINITION);
        for (final Object raw : rawVariants) {
            final Map<?, ?> rawVariantMap = Matchers.map(raw, "variant command map");
            final Map<?, ?> variantMap = CustomYamlConstructor.LinedObject.unlineKeys(rawVariantMap);
            variantMap.keySet().forEach(key ->
                    Assert.thatUnchecked(key instanceof String && allowedVariantMapKeys.contains(key),
                            "a variant in a variant command contains an unsupported key " + key));
            final Object definitionObj = variantMap.get(VARIANT_DEFINITION);
            Assert.thatUnchecked(definitionObj != null, "variant command requires a '" + VARIANT_DEFINITION + "' key");
            final YamlReference variantReference = reference.getResource().withLineNumber(extractVariantLineNumber(rawVariantMap, reference));
            final var command = commandSupplier.apply(definitionObj, variantReference);
            final Range<SemanticVersion> range = parseVariantRange(variantMap);
            parsedVariantsBuilder.add(new SingleVariant(range, command));
        }
        final var parsedVariants = parsedVariantsBuilder.build();
        validateVariants(parsedVariants, reference, identifier);
        return new VariantCommand(reference, executionContext, parsedVariants);
    }

    private static int extractVariantLineNumber(@Nonnull final Map<?, ?> rawVariantMap, @Nonnull final YamlReference outerReference) {
        return rawVariantMap.keySet().stream()
                .filter(CustomYamlConstructor.LinedObject.class::isInstance)
                .mapToInt(k -> ((CustomYamlConstructor.LinedObject) k).getLineNumber())
                .min()
                .orElse(outerReference.getLineNumber());
    }

    @Nonnull
    private static Range<SemanticVersion> parseVariantRange(@Nonnull final Map<?, ?> variantMap) {
        final boolean hasAtLeast = variantMap.containsKey(CURRENT_VERSION_AT_LEAST);
        final boolean hasLessThan = variantMap.containsKey(CURRENT_VERSION_LESS_THAN);
        if (!hasAtLeast && !hasLessThan) {
            return SemanticVersionRanges.all();
        }
        SemanticVersion lowerBound = SemanticVersion.min();
        SemanticVersion upperBound = SemanticVersion.max();
        if (hasAtLeast) {
            lowerBound = PreambleBlock.parseVersion(variantMap.get(CURRENT_VERSION_AT_LEAST));
        }
        if (hasLessThan) {
            upperBound = PreambleBlock.parseVersion(variantMap.get(CURRENT_VERSION_LESS_THAN));
        }
        Assert.thatUnchecked(lowerBound.compareTo(upperBound) < 0,
                "setup variant has empty version range [" + lowerBound + ", " + upperBound + ")");
        return Range.closedOpen(lowerBound, upperBound);
    }

    private static void validateVariants(@Nonnull final List<SingleVariant> variants,
                                         @Nonnull final YamlReference reference,
                                         @Nonnull final String identifier) {
        final List<Range<SemanticVersion>> ranges = variants.stream()
                .map(SingleVariant::range)
                .collect(ImmutableList.toImmutableList());

        // Check for overlapping ranges.
        final Set<Range<SemanticVersion>> overlapping = SemanticVersionRanges.overlapping(ranges);
        if (!overlapping.isEmpty()) {
            final IllegalArgumentException e = new IllegalArgumentException(
                    "command variants have overlapping version ranges: " + overlapping);
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️ Overlapping variants at " + reference, identifier, reference);
        }

        // Check for uncovered ranges.
        final Set<Range<SemanticVersion>> uncovered = SemanticVersionRanges.uncovered(ranges);
        if (!uncovered.isEmpty()) {
            final IllegalArgumentException e = new IllegalArgumentException(
                    "command variants do not cover the complete set of versions; missing: " + uncovered);
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️ Non-comprehensive variants at " + reference, identifier, reference);
        }
    }

    @Override
    void executeInternal(@Nonnull final YamlConnection connection) {
        final SemanticVersion currentVersion = connection.getCurrentVersion();
        final SingleVariant chosen = variantCommands.stream()
                .filter(v -> v.range.contains(currentVersion))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No variant matches current version " + currentVersion + " at " + reference));
        chosen.command.execute(connection);
    }

    public record SingleVariant(@Nonnull Range<SemanticVersion> range, @Nonnull Command command) {
    }
}
