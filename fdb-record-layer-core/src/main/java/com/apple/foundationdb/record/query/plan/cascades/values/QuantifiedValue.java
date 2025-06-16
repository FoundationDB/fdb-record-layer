/*
 * QuantifiedValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * A scalar value type that is directly derived from an alias.
 */
@API(API.Status.EXPERIMENTAL)
public interface QuantifiedValue extends LeafValue {

    @Nonnull
    CorrelationIdentifier getAlias();

    @Nonnull
    @Override
    default Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(getAlias());
    }

    @Nonnull
    @Override
    default BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> getAlias().equals(((QuantifiedValue)other).getAlias()));
    }

    @Nonnull
    @Override
    default Map<Value, Value> pullUp(@Nonnull final Iterable<? extends Value> toBePulledUpValues,
                                     @Nonnull final EvaluationContext evaluationContext,
                                     @Nonnull final AliasMap aliasMap,
                                     @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                     @Nonnull final CorrelationIdentifier upperBaseAlias) {
        final var alias = getAlias();
        final var areSimpleReferences =
                Streams.stream(toBePulledUpValues)
                        .flatMap(toBePulledUpValue -> toBePulledUpValue.getCorrelatedTo().stream())
                        .noneMatch(a -> !alias.equals(a) && constantAliases.contains(a));
        if (areSimpleReferences) {
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(alias, upperBaseAlias));
            final var translatedMapBuilder = ImmutableMap.<Value, Value>builder();
            for (final var toBePulledUpValue : toBePulledUpValues) {
                translatedMapBuilder.put(toBePulledUpValue, toBePulledUpValue.translateCorrelations(translationMap));
            }
            return translatedMapBuilder.build();
        }

        return LeafValue.super.pullUp(toBePulledUpValues, evaluationContext, aliasMap, constantAliases, upperBaseAlias);
    }
}
