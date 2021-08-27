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
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.PredicateMultiMap.PredicateMapping;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueComparisonRangePredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;

    protected ValueComparisonRangePredicate(@Nonnull final Value value) {
        this.value = value;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!PredicateWithValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ValueComparisonRangePredicate that = (ValueComparisonRangePredicate)other;
        return value.semanticEquals(that.value, equivalenceMap);
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
    @SuppressWarnings("java:S2160")
    public static class Placeholder extends ValueComparisonRangePredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Place-Holder");

        private final CorrelationIdentifier parameterAlias;

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier parameterAlias) {
            super(value);
            this.parameterAlias = parameterAlias;
        }

        @Nonnull
        @Override
        public Placeholder withValue(@Nonnull final Value value) {
            return new Placeholder(value, parameterAlias);
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
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            return Objects.equals(parameterAlias, ((Placeholder)other).parameterAlias);
        }

        public boolean semanticEqualsWithoutParameterAlias(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return super.semanticEquals(other, aliasMap);
        }

        @Nonnull
        @Override
        public Placeholder rebaseLeaf(@Nonnull final AliasMap translationMap) {
            return new Placeholder(getValue().rebase(translationMap), parameterAlias);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), parameterAlias);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
        }

        @Override
        public String toString() {
            return "(" + getValue() + " -> " + parameterAlias.toString() + ")";
        }
    }

    /**
     * A query predicate that can be used as a (s)earch (arg)ument for an index scan.
     */
    @SuppressWarnings("java:S2160")
    public static class Sargable extends ValueComparisonRangePredicate {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sargable-Predicate");

        @Nonnull
        private final ComparisonRange comparisonRange;

        public Sargable(@Nonnull final Value value, @Nonnull final ComparisonRange comparisonRange) {
            super(value);
            this.comparisonRange = comparisonRange;
        }

        @Nonnull
        @Override
        public Sargable withValue(@Nonnull final Value value) {
            return new Sargable(value, comparisonRange);
        }

        @Nonnull
        public ComparisonRange getComparisonRange() {
            return comparisonRange;
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            throw new RecordCoreException("search arguments should never be evaluated");
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            return Objects.equals(comparisonRange, ((Sargable)other).comparisonRange);
        }

        @Nonnull
        @Override
        public Sargable rebaseLeaf(@Nonnull final AliasMap translationMap) {
            return new Sargable(getValue().rebase(translationMap), comparisonRange);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), comparisonRange);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, comparisonRange.getRangeType());
        }

        @Nonnull
        @Override
        public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap,
                                                                    @Nonnull final QueryPredicate candidatePredicate) {
            if (candidatePredicate instanceof Placeholder) {
                final Placeholder placeHolderPredicate = (Placeholder)candidatePredicate;
                if (!getValue().semanticEquals(placeHolderPredicate.getValue(), aliasMap)) {
                    return Optional.empty();
                }

                // we found a compatible association between a comparison range in the query and a
                // parameter placeholder in the candidate
                return Optional.of(new PredicateMapping(this,
                        candidatePredicate,
                        ((matchInfo, boundParameterPrefixMap) -> reapplyPredicateMaybe(boundParameterPrefixMap, placeHolderPredicate)),
                        placeHolderPredicate.getParameterAlias()));
            } else if (candidatePredicate.isTautology()) {
                return Optional.of(new PredicateMapping(this,
                        candidatePredicate,
                        ((matchInfo, boundParameterPrefixMap) -> Optional.of(reapplyPredicate()))));

            }

            return Optional.empty();
        }

        private Optional<QueryPredicate> reapplyPredicateMaybe(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                               @Nonnull final Placeholder placeholderPredicate) {
            if (boundParameterPrefixMap.containsKey(placeholderPredicate.getParameterAlias())) {
                return Optional.empty();
            }

            return Optional.of(reapplyPredicate());
        }

        private QueryPredicate reapplyPredicate() {
            return toResidualPredicate();
        }

        @Override
        @Nonnull
        public QueryPredicate toResidualPredicate() {
            Verify.verify(!comparisonRange.isEmpty());

            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            if (comparisonRange.isEquality()) {
                residuals.add(getValue().withComparison(comparisonRange.getEqualityComparison()));
            } else if (comparisonRange.isInequality()) {
                for (final Comparisons.Comparison inequalityComparison : Objects.requireNonNull(comparisonRange.getInequalityComparisons())) {
                    residuals.add(getValue().withComparison(inequalityComparison));
                }
            }

            return AndPredicate.and(residuals.build());
        }

        @Override
        public String toString() {
            return getValue() + " " + comparisonRange;
        }
    }
}
