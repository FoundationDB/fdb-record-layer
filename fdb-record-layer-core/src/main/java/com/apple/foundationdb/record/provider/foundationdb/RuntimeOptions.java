/*
 * RuntimeOptions.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRuntimeOption;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class RuntimeOptions implements Correlated<RuntimeOptions> {

    @Nonnull
    private static final RuntimeOptions EMPTY = new RuntimeOptions(ImmutableSet.of());

    @Nonnull
    private final Map<String, RuntimeOption> runtimeOptions;

    public RuntimeOptions(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final List<PRuntimeOption> runtimeOptionProtos) {
        final var runtimeOptionsMapBuilder = ImmutableMap.<String, RuntimeOption>builder();
        for (final var runtimeOptionProto : runtimeOptionProtos) {
            final var runtimeOption = RuntimeOption.fromProto(serializationContext, runtimeOptionProto);
            runtimeOptionsMapBuilder.put(runtimeOption.getName(), runtimeOption);
        }
        this.runtimeOptions = runtimeOptionsMapBuilder.build();
    }

    public RuntimeOptions(@Nonnull final Map<String, RuntimeOption> runtimeOptions) {
        this.runtimeOptions = ImmutableMap.copyOf(runtimeOptions);
    }

    public RuntimeOptions(@Nonnull final Set<RuntimeOption> runtimeOptions) {
        this.runtimeOptions = runtimeOptions.stream()
                .collect(ImmutableMap.toImmutableMap(option -> Objects.requireNonNull(option).getName(),
                        runtimeOption -> runtimeOption));
    }

    @Nonnull
    public Collection<RuntimeOption> getOptions() {
        return runtimeOptions.values();
    }

    @Nonnull
    public Set<String> getOptionNames() {
        return runtimeOptions.keySet();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return computeCorrelatedTo();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedTo() {
        final var correlationsSet = ImmutableSet.<CorrelationIdentifier>builder();
        getOptions().forEach(option -> correlationsSet.addAll(option.getCorrelatedTo()));
        return correlationsSet.build();
    }

    @Nonnull
    @Override
    public RuntimeOptions rebase(@Nonnull final AliasMap translationMap) {
        final var rebasedRuntimeOptionsBuilder = ImmutableMap.<String, RuntimeOption>builder();
        boolean isRebased = false;
        for (final var runtimeOption : runtimeOptions.entrySet()) {
            final var rebasedRuntimeOptionMaybe = runtimeOption.getValue().rebase(translationMap);
            if (rebasedRuntimeOptionMaybe != runtimeOption.getValue()) {
                isRebased = true;
            }
            rebasedRuntimeOptionsBuilder.put(runtimeOption.getKey(), rebasedRuntimeOptionMaybe);
        }
        if (isRebased) {
            return new RuntimeOptions(rebasedRuntimeOptionsBuilder.build());
        }
        return this;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RuntimeOptions translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues) {
        final var translatedRuntimeOptionsBuilder = ImmutableMap.<String, RuntimeOption>builder();
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        boolean isTranslated = false;
        for (final var runtimeOption : runtimeOptions.entrySet()) {
            final var rebasedRuntimeOptionMaybe = runtimeOption.getValue().translateCorrelations(translationMap, shouldSimplifyValues);
            if (rebasedRuntimeOptionMaybe != runtimeOption.getValue()) {
                isTranslated = true;
            }
            translatedRuntimeOptionsBuilder.put(runtimeOption.getKey(), rebasedRuntimeOptionMaybe);
        }
        if (isTranslated) {
            return new RuntimeOptions(translatedRuntimeOptionsBuilder.build());
        }
        return this;
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final var otherRuntimeOption = (RuntimeOptions)other;
        if (otherRuntimeOption.runtimeOptions.size() != runtimeOptions.size()) {
            return false;
        }

        for (final var entry : runtimeOptions.entrySet()) {
            final var otherOption = otherRuntimeOption.runtimeOptions.get(entry.getKey());
            if (otherOption == null || !entry.getValue().semanticEquals(otherOption, aliasMap)) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    public ConstrainedBoolean semanticEqualsTyped(@Nonnull final Object other, @Nonnull final ValueEquivalence valueEquivalence) {
        if (this == other) {
            return ConstrainedBoolean.alwaysTrue();
        }

        if (getClass() != other.getClass()) {
            return ConstrainedBoolean.falseValue();
        }

        final var that = (RuntimeOptions) other;

        if (that.runtimeOptions.size() != runtimeOptions.size()) {
            return ConstrainedBoolean.falseValue();
        }

        ConstrainedBoolean result = ConstrainedBoolean.alwaysTrue();
        for (final var entry : runtimeOptions.entrySet()) {
            final var otherOption = that.runtimeOptions.get(entry.getKey());
            result = result.compose(ignored ->
                    entry.getValue().getValue().semanticEquals(otherOption, valueEquivalence));
        }
        return result;
    }

    @Nonnull
    public Optional<RuntimeOptions> replaceValuesMaybe(@Nonnull final Function<Value, Optional<Value>> replacementFunction) {
        final var replacedRuntimeOptions = ImmutableSet.<RuntimeOption>builder();

        boolean shouldCreateNewInstance = false;
        for (final var runtimeOption : getOptions()) {
            final var replacedRuntimeOptionMaybe = runtimeOption.replaceValuesMaybe(replacementFunction);
            if (replacedRuntimeOptionMaybe.isEmpty()) {
                return Optional.empty();
            }
            final var replacedRuntimeOption = replacedRuntimeOptionMaybe.get();
            replacedRuntimeOptions.add(replacedRuntimeOption);
            if (replacedRuntimeOption != runtimeOption) {
                shouldCreateNewInstance = true;
            }
        }

        if (shouldCreateNewInstance) {
            return Optional.of(new RuntimeOptions(replacedRuntimeOptions.build()));
        }
        return Optional.of(this);
    }

    @Override
    public int semanticHashCode() {
        return computeSemanticHashCode();
    }

    private int computeSemanticHashCode() {
        return Objects.hash(runtimeOptions.values().stream()
                .map(RuntimeOption::semanticHashCode)
                .toArray());
    }

    @Nonnull
    public static RuntimeOptions empty() {
        return EMPTY;
    }

    @Nonnull
    public List<PRuntimeOption> toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return getOptions().stream().map(option -> option.toProto(serializationContext))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public static RuntimeOptions fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final List<PRuntimeOption> runtimeOptionProtos) {
        return new RuntimeOptions(serializationContext, runtimeOptionProtos);
    }
}
