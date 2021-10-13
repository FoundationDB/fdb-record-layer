/*
 * ValuePredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.predicates.LiteralValue.formatLiteral;

/**
 * A predicate consisting of a {@link Value} and a {@link Comparison}.
 */
@API(API.Status.EXPERIMENTAL)
public class ValuePredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;
    @Nonnull
    private final Comparison comparison;

    public ValuePredicate(@Nonnull Value value, @Nonnull Comparison comparison) {
        this.value = value;
        this.comparison = comparison;
    }

    @Nonnull
    public Comparison getComparison() {
        return comparison;
    }

    @Nonnull
    @Override
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public ValuePredicate withValue(@Nonnull final Value value) {
        return new ValuePredicate(value, comparison);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return comparison.eval(store, context, value.eval(store, context, record, message));
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public QueryPredicate rebaseLeaf(@Nonnull final AliasMap translationMap) {
        Value rebasedValue = value.rebase(translationMap);
        // TODO rebase comparison if needed
        if (value != rebasedValue) { // reference comparison intended
            return new ValuePredicate(rebasedValue, comparison);
        }
        return this;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!PredicateWithValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ValuePredicate that = (ValuePredicate)other;
        return value.semanticEquals(that.value, equivalenceMap) &&
               comparison.equals(that.comparison);
    }
    
    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode(), comparison);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, value, comparison);
    }

    @Override
    public String toString() {
        return value.toString() + " " + comparison.toString();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        final String comparandString;
        comparandString = formatLiteral(value.getResultType(), comparison.typelessString());

        return "(" + value.explain(formatter) + " " + Comparison.typeToSymbol(comparison.getType()) + " " + comparandString + ")";
    }

    @Nonnull
    public static Optional<List<Comparison>> collectConjunctedComparisons(@Nonnull final QueryPredicate queryPredicate) {
        if (queryPredicate instanceof ValuePredicate) {
            return Optional.of(ImmutableList.of(((ValuePredicate)queryPredicate).getComparison()));
        } else if (queryPredicate instanceof AndPredicate) {
            final AndPredicate andPredicate = (AndPredicate)queryPredicate;
            final ImmutableList.Builder<Comparison> comparisonsBuilder = ImmutableList.builder();
            for (final QueryPredicate factor : andPredicate.getChildren()) {
                final Optional<List<Comparison>> nestedComparisonsMaybe = collectConjunctedComparisons(factor);
                if (!nestedComparisonsMaybe.isPresent()) {
                    return Optional.empty();
                }
                comparisonsBuilder.addAll(nestedComparisonsMaybe.get());
            }
            return Optional.of(comparisonsBuilder.build());
        }
        return Optional.empty();
    }

}
