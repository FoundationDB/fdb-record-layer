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
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;

import java.util.Set;

/**
 * A scalar value type that is directly derived from an alias.
 */
@API(API.Status.EXPERIMENTAL)
public interface QuantifiedValue extends LeafValue {

    CorrelationIdentifier getAlias();

    @Override
    default Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(getAlias());
    }

    @Override
    default ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return LeafValue.super.equalsWithoutChildren(other)
                .filter(ignored -> getAlias().equals(((QuantifiedValue)other).getAlias()));
    }

    @Override
    default Multimap<Value, Value> pullUp(final Iterable<? extends Value> toBePulledUpValues,
                                          final EvaluationContext evaluationContext,
                                          final AliasMap aliasMap,
                                          final Set<CorrelationIdentifier> constantAliases,
                                          final CorrelationIdentifier upperBaseAlias) {
        // If all the values to be pulled up are only correlated to this value's correlation ID (or to constants),
        // then we can do a pull up just by translating correlations
        final var alias = getAlias();
        final var areSimpleReferences =
                Streams.stream(toBePulledUpValues)
                        .flatMap(toBePulledUpValue -> toBePulledUpValue.getCorrelatedTo().stream())
                        .allMatch(a -> alias.equals(a) || constantAliases.contains(a));
        if (areSimpleReferences) {
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(alias, upperBaseAlias));
            final var translatedMapBuilder = ImmutableMultimap.<Value, Value>builder();
            for (final var toBePulledUpValue : toBePulledUpValues) {
                translatedMapBuilder.put(toBePulledUpValue, toBePulledUpValue.translateCorrelations(translationMap));
            }
            return translatedMapBuilder.build();
        }

        return LeafValue.super.pullUp(toBePulledUpValues, evaluationContext, aliasMap, constantAliases, upperBaseAlias);
    }
}
