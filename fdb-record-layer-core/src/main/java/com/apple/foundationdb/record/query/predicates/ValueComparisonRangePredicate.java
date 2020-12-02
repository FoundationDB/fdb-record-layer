/*
 * ValueComparisonRangePredicate.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueComparisonRangePredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;

    public ValueComparisonRangePredicate(@Nonnull final Value value) {
        this.value = value;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final PlannerBindings outerBindings, @Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(outerBindings, this, ImmutableList.of());
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ValueComparisonRangePredicate that = (ValueComparisonRangePredicate)other;
        return value.semanticEquals(that.value, aliasMap);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.planHash(hashKind, value);
    }

    public static Placeholder placeholder(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, parameterAlias);
    }

    public static Sargable sargable(@Nonnull Value value, @Nonnull ComparisonRange comparisonRange) {
        return new Sargable(value, comparisonRange);
    }

    /**
     * A place holder predicate solely used for index matching.
     */
    public static class Placeholder extends ValueComparisonRangePredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Place-Holder");

        private final CorrelationIdentifier parameterAlias;

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier parameterAlias) {
            super(value);
            this.parameterAlias = parameterAlias;
        }

        public CorrelationIdentifier getParameterAlias() {
            return parameterAlias;
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            throw new RecordCoreException("this method should not ever be reached");
        }

        @Override
        @SuppressWarnings({"ConstantConditions", "java:S2259"})
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (!semanticEqualsWithoutParameterAlias(other, aliasMap)) {
                return false;
            }

            return Objects.equals(parameterAlias, ((Placeholder)other).parameterAlias);
        }

        public boolean semanticEqualsWithoutParameterAlias(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return super.semanticEquals(other, aliasMap);
        }

        @Nonnull
        @Override
        public Placeholder rebase(@Nonnull final AliasMap translationMap) {
            return new Placeholder(getValue().rebase(translationMap), parameterAlias);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), parameterAlias);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, super.planHash(), BASE_HASH);
        }

        @Override
        public String toString() {
            return "(" + getValue() + " -> " + parameterAlias.toString() + ")";
        }
    }

    /**
     * A query predicate that can be used as a (s)earch (arg)ument for an index scan.
     */
    public static class Sargable extends ValueComparisonRangePredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sargable-Predicate");

        @Nonnull
        private final ComparisonRange comparisonRange;

        public Sargable(@Nonnull final Value value, @Nonnull final ComparisonRange comparisonRange) {
            super(value);
            this.comparisonRange = comparisonRange;
        }

        @Nonnull
        public ComparisonRange getComparisonRange() {
            return comparisonRange;
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            // this could just throw an exception as we shouldn't support evaluating this kind of predicate
            // as it is for index matching purposes only
            final Object eval = getValue().eval(store, context, record, message);
            return comparisonRange.eval(store, context, eval);
        }

        @Override
        @SuppressWarnings({"ConstantConditions", "java:S2259"})
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (!super.semanticEquals(other, aliasMap)) {
                return false;
            }

            return Objects.equals(comparisonRange, ((Sargable)other).comparisonRange);
        }

        @Nonnull
        @Override
        public Sargable rebase(@Nonnull final AliasMap translationMap) {
            return new Sargable(getValue().rebase(translationMap), comparisonRange);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), comparisonRange);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, super.planHash(), BASE_HASH, comparisonRange.getRangeType());
        }

        @Override
        public String toString() {
            return getValue() + " " + comparisonRange;
        }

        public List<QueryPredicate> toResiduals() {
            Verify.verify(!comparisonRange.isEmpty());

            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            if (comparisonRange.isEquality()) {
                residuals.add(new ValuePredicate(getValue(), comparisonRange.getEqualityComparison()));
            } else if (comparisonRange.isInequality()) {
                for (final Comparisons.Comparison inequalityComparison : Objects.requireNonNull(comparisonRange.getInequalityComparisons())) {
                    residuals.add(new ValuePredicate(getValue(), inequalityComparison));
                }
            }

            return residuals.build();
        }
    }
}
