/*
 * ValueComparisonRangePredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange.EvalResult.FALSE;
import static com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange.EvalResult.TRUE;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueWithRanges implements PredicateWithValue {
    @Nonnull
    private final Value value;

    protected ValueWithRanges(@Nonnull final Value value) {
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
        final ValueWithRanges that = (ValueWithRanges)other;
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

    @Nonnull
    public abstract Set<CompileTimeEvaluableRange> getRanges();

    public boolean impliedBy(@NonNull final AliasMap aliasMap,
                             @Nonnull final ValueWithRanges other) {
        if (!value.semanticEquals(other.value, aliasMap)) {
            return false;
        }
        final var leftDisjunction = getRanges();
        final var rightDisjunction = other.getRanges();
        for (final var left : leftDisjunction) {
            if (!left.isCompileTimeEvaluable() || !left.isEmpty().equals(FALSE) || rightDisjunction.stream().noneMatch(right -> right.implies(left).equals(TRUE))) {
                return false;
            }
        }
        return true;
    }

    public boolean isCompileTimeEvaluable() {
        return getRanges().stream().allMatch(CompileTimeEvaluableRange::isCompileTimeEvaluable);
    }

    @Nonnull
    public static Placeholder placeholder(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, parameterAlias);
    }

    @Nonnull
    public static Sargable sargable(@Nonnull Value value, @Nonnull final CompileTimeEvaluableRange range) {
        return new Sargable(value, range);
    }

    @Nonnull
    public static ValueConstraint constraint(@Nonnull final Value value, @Nonnull final Set<CompileTimeEvaluableRange> ranges) {
        return new ValueConstraint(value, ranges);
    }

    /**
     * A placeholder predicate solely used for index matching.
     */
    @SuppressWarnings("java:S2160")
    public static class Placeholder extends ValueWithRanges {

        @Nonnull
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Place-Holder");

        @Nonnull
        private final CorrelationIdentifier alias;

        @Nonnull
        private final Set<CompileTimeEvaluableRange> compileTimeEvaluableRanges;

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier alias) {
            this(value, alias, Set.of());
        }

        public Placeholder(@Nonnull final Value value, @Nonnull final CorrelationIdentifier alias, @Nonnull final Set<CompileTimeEvaluableRange> compileTimeEvaluableRange) {
            super(value);
            this.alias = alias;
            this.compileTimeEvaluableRanges = ImmutableSet.copyOf(compileTimeEvaluableRange);
        }

        @Nonnull
        @Override
        public Placeholder withValue(@Nonnull final Value value) {
            return new Placeholder(value, alias);
        }

        @Nonnull
        public Placeholder withCompileTimeRanges(@Nonnull final Set<CompileTimeEvaluableRange> ranges) {
            return new Placeholder(getValue(), alias, ranges);
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public Set<CompileTimeEvaluableRange> getRanges() {
            return compileTimeEvaluableRanges;
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("this method should not ever be reached");
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            return Objects.equals(alias, ((Placeholder)other).alias);
        }

        public boolean semanticEqualsWithoutParameterAlias(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return super.semanticEquals(other, aliasMap);
        }

        @Nonnull
        @Override
        public Placeholder translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
            return new Placeholder(getValue().translateCorrelations(translationMap), alias);
        }

        @Nonnull
        @Override
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap, @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
            throw new RecordCoreException("this method should not ever be reached");
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), alias);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
        }

        @Override
        public String toString() {
            final var result = new StringBuilder();
            result.append("(").append(getValue()).append(" -> ").append(alias);
            if (compileTimeEvaluableRanges != null) {
                result.append(compileTimeEvaluableRanges.stream().map(CompileTimeEvaluableRange::toString).collect(Collectors.joining("||")));
            }
            result.append(")");
            return result.toString();
        }
    }

    /**
     * A query predicate that can be used as a (s)earch (arg)ument for an index scan.
     */
    @SuppressWarnings("java:S2160")
    public static class Sargable extends ValueWithRanges {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sargable-Predicate");

        @Nonnull
        private final CompileTimeEvaluableRange range;

        public Sargable(@Nonnull final Value value, @Nonnull final CompileTimeEvaluableRange range) {
            super(value);
            this.range = range;
        }

        @Nonnull
        @Override
        public Sargable withValue(@Nonnull final Value value) {
            return new Sargable(value, range);
        }

        @Nonnull
        public Set<CompileTimeEvaluableRange> getRanges() {
            return Set.of(range);
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
            return ImmutableSet.<CorrelationIdentifier>builder()
                    .addAll(super.getCorrelatedToWithoutChildren())
                    .addAll(range.getCorrelatedTo())
                    .build();
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("search arguments should never be evaluated");
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (this == other) {
                return true;
            }

            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            if (!(other instanceof Sargable)) {
                return false;
            }
            final var that = (Sargable)other;
            return range.semanticEquals(that.range, equivalenceMap);
        }

        @Nonnull
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Sargable translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
            final var translatedValue = getValue().translateCorrelations(translationMap);
            final var newRange = range.translateCorrelations(translationMap);
            if (translatedValue != getValue() || newRange != range) {
                return new Sargable(translatedValue, newRange);
            }
            return this;
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), range.semanticHashCode());
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, range);
        }

        @Nonnull
        @Override
        public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap,
                                                                    @Nonnull final QueryPredicate candidatePredicate) {
            if (candidatePredicate.isContradiction()) {
                return Optional.empty();
            }

            if (candidatePredicate.isTautology()) {
                return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, alsoIgnore) -> injectCompensationFunctionMaybe()));
            }

            if (candidatePredicate instanceof ValueWithRanges) {
                final var can = (ValueWithRanges)candidatePredicate;

                // the value on which the candidate is defined must be the same as the _this_'s value.
                if (!getValue().semanticEquals(can.getValue(), aliasMap)) {
                    return Optional.empty();
                }

                // if the candidate predicate has a compile-time range (filtered index) check to see whether it implies
                // (some) of the predicate comparisons.
                if (can.getRanges().isEmpty() || (!(can.isCompileTimeEvaluable() && isCompileTimeEvaluable())) || impliedBy(aliasMap, can)) {
                    // create a compensation function
                    PredicateMultiMap.CompensatePredicateFunction compensation;
                    Optional<CorrelationIdentifier> parameterAlias = Optional.empty();
                    if (candidatePredicate instanceof Placeholder) {
                        final var placeholder = (Placeholder)candidatePredicate;
                        compensation = (ignore, boundParameterPrefixMap) -> {
                            if (boundParameterPrefixMap.containsKey(placeholder.getAlias())) {
                                return Optional.empty();
                            }
                            return injectCompensationFunctionMaybe();
                        };
                        parameterAlias = Optional.of(placeholder.alias);
                    } else {
                        compensation = (ignore, alsoIgnore) -> {
                            // no need for compensation if range boundaries match between candidate constraint and query sargable
                            if (isCompileTimeEvaluable() && can.isCompileTimeEvaluable() && can.getRanges().stream().anyMatch(constraintRange -> range.implies(constraintRange).equals(TRUE))) {
                                return Optional.empty();
                            }
                            if (getRanges().stream().allMatch(left -> can.getRanges().stream().anyMatch(right -> left.semanticEquals(right, aliasMap)))) {
                                return Optional.empty();
                            }
                            return injectCompensationFunctionMaybe();
                        };
                    }
                    // we found a compatible association between a comparison range in the query and a
                    // parameter placeholder in the candidate
                    return Optional.of(new PredicateMapping(this, candidatePredicate, compensation, parameterAlias));
                } else {
                    return Optional.empty();
                }
            } else if (candidatePredicate.isTautology()) {
                return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, alsoIgnore) -> injectCompensationFunctionMaybe()));
            }

            //
            // The candidate predicate is not a placeholder which means that the match candidate can not be
            // parameterized by a mapping of this to the candidate predicate. Therefore, in order to match at all,
            // it must be semantically equivalent.
            //
            if (semanticEquals(candidatePredicate, aliasMap)) {
                // Note that we never have to reapply the predicate as both sides are always semantically
                // equivalent.
                return Optional.of(new PredicateMapping(this, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.EMPTY));
            }

            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                    @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                    @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
            Verify.verify(childrenResults.isEmpty());
            return injectCompensationFunctionMaybe();
        }

        @Nonnull
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe() {
            return Optional.of(reapplyPredicate());
        }

        private ExpandCompensationFunction reapplyPredicate() {
            return translationMap -> GraphExpansion.ofPredicate(toResidualPredicate().translateCorrelations(translationMap));
        }

        /**
         * transforms this Sargable into a conjunction of equality and non-equality predicates.
         * @return a conjunction of equality and non-equality predicates.
         */
        @Override
        @Nonnull
        public QueryPredicate toResidualPredicate() {
            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            residuals.addAll(range.getComparisons().stream().map(c -> getValue().withComparison(c)).collect(Collectors.toList()));
            return AndPredicate.and(residuals.build());
        }

        @Override
        public String toString() {
            return getValue() + " " + range;
        }
    }

    /**
     * Value with a disjunction of ranges, used only for matching.
     */
    @SuppressWarnings(("java:S2160"))
    public static class ValueConstraint extends ValueWithRanges {

        /**
         * Collection (disjunction) of ranges.
         */
        @Nonnull
        private final Set<CompileTimeEvaluableRange> ranges;

        /**
         * Constructs a new instance of {@link ValueConstraint}.
         *
         * @param value The value on which the predicated ranges are defined.
         * @param ranges Disjunction of ranges.
         */
        public ValueConstraint(@Nonnull final Value value, @Nonnull final Set<CompileTimeEvaluableRange> ranges) {
            super(value);
            this.ranges = ImmutableSet.copyOf(ranges);
        }

        @Nonnull
        public Set<CompileTimeEvaluableRange> getRanges() {
            return ranges;
        }

        @Nonnull
        @Override
        public PredicateWithValue withValue(@Nonnull final Value value) {
            return new ValueConstraint(value, ranges);
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("constraints are used for matching and should never be evaluated");
        }

        /**
         * Returns an equivalent disjoint normal form (DNF) representation of {@code this} {@link ValueConstraint} to be used
         * as a residual.
         *
         * @return an equivalent disjoint normal form (DNF) representation.
         */
        @Override
        @Nonnull
        public QueryPredicate toResidualPredicate() {
            final ImmutableList.Builder<QueryPredicate> dnfParts = ImmutableList.builder();
            for (final var range : ranges) {
                final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
                residuals.addAll(range.getComparisons().stream().map(c -> getValue().withComparison(c)).collect(Collectors.toList()));
                dnfParts.add(AndPredicate.and(residuals.build()));
            }
            return OrPredicate.or(dnfParts.build());
        }

        @Override
        public String toString() {
            return "(" + getValue() +
                   ranges.stream().map(CompileTimeEvaluableRange::toString).collect(Collectors.joining("||")) +
                   ")";
        }
    }
}
