/*
 * Placeholder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PPredicateWithValueAndRanges;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A Placeholder is basically a {@link PredicateWithValueAndRanges} with an alias that is used solely used for index matching.
 */
public class Placeholder extends PredicateWithValueAndRanges implements WithAlias {
    private final CorrelationIdentifier parameterAlias;

    private Placeholder(final Value value,
                        final Set<RangeConstraints> ranges,
                        final CorrelationIdentifier alias) {
        super(value, ranges);
        this.parameterAlias = alias;
    }

    @Override
    public PredicateWithValueAndRanges withValue(final Value value) {
        return new Placeholder(value, getRanges(), parameterAlias);
    }

    public Placeholder withAlias(final CorrelationIdentifier newParameterAlias) {
        if (newParameterAlias.equals(parameterAlias)) {
            return this;
        }
        return new Placeholder(getValue(), getRanges(), newParameterAlias);
    }

    @Override
    public PredicateWithValueAndRanges withRanges(final Set<RangeConstraints> ranges) {
        return new Placeholder(getValue(), ranges, parameterAlias);
    }

    @Override
    public boolean isSargable() {
        return false;
    }

    @Override
    public boolean isTautology() {
        return !isConstraining();
    }

    public static Placeholder newInstanceWithoutRanges(Value value, CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, ImmutableSet.of(), parameterAlias);
    }

    public static Placeholder of(final Value value, final Set<RangeConstraints> ranges, CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, ranges, parameterAlias);
    }

    public boolean isConstraining() {
        return getRanges().stream().anyMatch(RangeConstraints::isConstraining);
    }

    @Override
    public Placeholder withValueAndRanges(final Value value, final Set<RangeConstraints> ranges) {
        return new Placeholder(value, ranges, parameterAlias);
    }

    public Placeholder withExtraRanges(final Set<RangeConstraints> ranges) {
        return new Placeholder(getValue(), Stream.concat(ranges.stream(), getRanges().stream()).collect(ImmutableSet.toImmutableSet()), getParameterAlias());
    }

    @Override
    public Placeholder translateLeafPredicate(final TranslationMap translationMap, final boolean shouldSimplifyValues) {
        return new Placeholder(getValue().translateCorrelations(translationMap),
                getRanges().stream()
                        .map(range -> range.translateCorrelations(translationMap, shouldSimplifyValues))
                        .collect(ImmutableSet.toImmutableSet()), getParameterAlias());
    }

    @Override
    public CorrelationIdentifier getParameterAlias() {
        return parameterAlias;
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final QueryPredicate other,
                                                    final ValueEquivalence valueEquivalence) {
        return super.equalsWithoutChildren(other, valueEquivalence)
                .filter(ignored -> Objects.equals(parameterAlias, ((Placeholder)other).parameterAlias));
    }

    @Override
    public boolean equals(final Object other) {
        if (!super.semanticEquals(other, AliasMap.emptyMap())) {
            return false;
        }
        if (!(other instanceof Placeholder)) {
            return false;
        }
        return parameterAlias.equals(((Placeholder)other).parameterAlias);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        Verify.verify(Iterables.isEmpty(explainSuppliers));
        return ExplainTokensWithPrecedence.of(super.explain(explainSuppliers).getExplainTokens()
                .addWhitespace().addToString("->").addWhitespace().addIdentifier(getParameterAlias().toString()));
    }

    @Override
    public PPredicateWithValueAndRanges toProto(final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("call unsupported");
    }

    @Override
    public PQueryPredicate toQueryPredicateProto(final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("call unsupported");
    }
}
