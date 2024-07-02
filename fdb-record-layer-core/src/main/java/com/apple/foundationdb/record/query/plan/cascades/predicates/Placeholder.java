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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PPredicateWithValueAndRanges;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A Placeholder is basically a {@link PredicateWithValueAndRanges} with an alias that is used solely used for index matching.
 */
public class Placeholder extends PredicateWithValueAndRanges implements WithAlias {

    @Nonnull
    private final CorrelationIdentifier parameterAlias;

    private Placeholder(@Nonnull Value value,
                       @Nonnull final Set<RangeConstraints> ranges,
                       @Nonnull final CorrelationIdentifier alias) {
        super(value, ranges);
        this.parameterAlias = alias;
    }

    @Nonnull
    @Override
    public PredicateWithValueAndRanges withValue(@Nonnull final Value value) {
        return new Placeholder(value, getRanges(), parameterAlias);
    }

    @Nonnull
    @Override
    public PredicateWithValueAndRanges withRanges(@Nonnull final Set<RangeConstraints> ranges) {
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

    @Nonnull
    public static Placeholder newInstance(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, ImmutableSet.of(), parameterAlias);
    }

    public boolean isConstraining() {
        return getRanges().stream().anyMatch(RangeConstraints::isConstraining);
    }

    @Nonnull
    public Placeholder withExtraRanges(@Nonnull final Set<RangeConstraints> ranges) {
        return new Placeholder(getValue(), Stream.concat(ranges.stream(), getRanges().stream()).collect(ImmutableSet.toImmutableSet()), getParameterAlias());
    }

    @Nonnull
    @Override
    public Placeholder translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return new Placeholder(getValue().translateCorrelations(translationMap),
                getRanges().stream().map(range -> range.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet()), getParameterAlias());
    }

    @Nonnull
    @Override
    public CorrelationIdentifier getParameterAlias() {
        return parameterAlias;
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                                       @Nonnull final ValueEquivalence valueEquivalence) {
        return super.equalsWithoutChildren(other, valueEquivalence)
                .filter(ignored -> Objects.equals(parameterAlias, ((Placeholder)other).parameterAlias));
    }

    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
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
    public String toString() {
        return super.toString() + " -> " + getParameterAlias();
    }

    @Nonnull
    @Override
    public PPredicateWithValueAndRanges toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("call unsupported");
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("call unsupported");
    }
}
