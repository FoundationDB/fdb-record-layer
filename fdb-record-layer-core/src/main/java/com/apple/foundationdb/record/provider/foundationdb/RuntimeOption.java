/*
 * RuntimeOption.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRuntimeOption;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
public final class RuntimeOption implements Typed, Correlated<RuntimeOption>, PlanSerializable, PlanHashable {

    @Nonnull
    private final String name;

    @Nonnull
    private final Value value;

    private RuntimeOption(@Nonnull final String name, @Nonnull final Value value) {
        this.name = name;
        this.value = value;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return value.getResultType();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, name, value);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RuntimeOption that = (RuntimeOption)o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Nonnull
    public Optional<RuntimeOption> replaceValuesMaybe(@Nonnull Function<Value, Optional<Value>> replacementFunction) {
        return replacementFunction.apply(value).map(replacedValue -> {
            if (replacedValue == value) {
                return this;
            } else {
                return new RuntimeOption(name, replacedValue);
            }
        });
    }

    @Nonnull
    @Override
    public PRuntimeOption toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRuntimeOption.newBuilder().setName(name).setValue(value.toValueProto(serializationContext)).build();
    }

    @Nonnull
    public static RuntimeOption fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PRuntimeOption runtimeOption) {
        final var optionName = runtimeOption.getName();
        final var optionValue = Value.fromValueProto(serializationContext, runtimeOption.getValue());
        return new RuntimeOption(optionName, optionValue);
    }

    @Nonnull
    public static <T> RuntimeOption of(@Nonnull final String key, @Nonnull final LiteralValue<T> value) {
        return new RuntimeOption(key, value);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RuntimeOption rebase(@Nonnull final AliasMap translationMap) {
        final var translatedValue = value.rebase(translationMap);
        if (translatedValue == value) {
            return this;
        }
        return new RuntimeOption(name, translatedValue);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final var otherRuntimeOption = (RuntimeOption)other;
        return name.equals(otherRuntimeOption.name) && value.semanticEquals(otherRuntimeOption.value, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(name, value.semanticHashCode());
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RuntimeOption translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues) {
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        final var translatedValue = value.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedValue == value) {
            return this;
        }
        return new RuntimeOption(name, translatedValue);
    }
}
