/*
 * ValueWithRanges.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
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
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class associates a {@link Value} with a set of ranges ({@link RangeConstraints}). Each one of the ranges refers
 * to a conjunction of:
 *  - a contiguous compile-time evaluable range.
 *  - a set of non-compile-time (deferred) ranges.
 *<br>
 * The set here represents a disjunction of these ranges. So, in a way, this class represents a boolean expression in
 * DNF form defined on the associated {@link Value}.
 *<br>
 * It is mainly used for index matching, i.e. it is not evaluable at runtime. On the query side it is normally used to
 * represent a search-argument (sargable). On the candidate side, it is normally used to either represent a restriction
 * on a specific attribute of a scan.
 * <br>
 * If the attribute is indexes, we use the {@link Placeholder} subtype to represent it along with its alias used later
 * on for substituting one of the index scan search prefix) and an (optional) range(s) defined on it to semantically
 * represent the filtering nature of the associated index and use it to plan accordingly.
 * <br>
 * If the attribute, however, is not indexed, then we use an instance of {@code this} class as a restriction on that
 * particular attribute.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class ValueWithRanges implements PredicateWithValue {

    /**
     * The value associated with the {@code ranges}.
     */
    @Nonnull
    private final Value value;

    /**
     * A list of ranges, implicitly defining a boolean predicate in DNF form defined on the {@code value}.
     */
    @Nonnull
    private final Set<RangeConstraints> ranges;

    /**
     * Creates a new instance of {@link ValueWithRanges}.
     *
     * @param value The value.
     * @param ranges A set of ranges defined on the value (can be empty).
     */
    protected ValueWithRanges(@Nonnull final Value value, @Nonnull final Set<RangeConstraints> ranges) {
        this.value = value;
        this.ranges = ImmutableSet.copyOf(ranges);
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public ValueWithRanges withValue(@Nonnull final Value value) {
        return new ValueWithRanges(value, ranges);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new RecordCoreException("this method should not ever be reached");
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Streams.concat(value.getCorrelatedTo().stream(),
                ranges.stream().flatMap(r -> r.getCorrelatedTo().stream()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    /**
     * Performs algebraic equality between {@code this} and {@code other}, if {@code other} is also a {@link ValueWithRanges}.
     *
     * @param other The other predicate
     * @param equivalenceMap The alias equivalence map.
     * @return {@code true} if both predicates are equal, otherwise {@code false}.
     */
    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!PredicateWithValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ValueWithRanges that = (ValueWithRanges)other;
        final var inverseEquivalenceMap = equivalenceMap.inverse();
        return value.semanticEquals(that.value, equivalenceMap) &&
               ranges.stream().allMatch(left -> that.ranges.stream().anyMatch(right -> left.semanticEquals(right, equivalenceMap))) &&
               that.ranges.stream().allMatch(left -> ranges.stream().anyMatch(right -> left.semanticEquals(right, inverseEquivalenceMap)));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return value.semanticHashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        throw new RecordCoreException("this method should not ever be reached");
    }

    @Nonnull
    public Set<RangeConstraints> getRanges() {
        return ranges;
    }

    public boolean isSargable() {
        return ranges.size() == 1;
    }

    @Nonnull
    @Override
    public ValueWithRanges translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return new ValueWithRanges(value.translateCorrelations(translationMap), ranges.stream().map(range -> range.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet()));
    }

    private boolean impliedBy(@Nonnull final ValueWithRanges other) {
        final var leftRanges = getRanges();
        final var rightRanges = other.getRanges();
        for (final var left : leftRanges) {
            if (!left.isCompileTimeEvaluable() || !(left.isEmpty() == Proposition.FALSE) || rightRanges.stream().noneMatch(right -> right.encloses(left) == Proposition.TRUE)) {
                return false;
            }
        }
        return true;
    }

    public boolean equalsValueOnly(@Nonnull final QueryPredicate other) {
        return (other instanceof ValueWithRanges) && value.equals(((ValueWithRanges)other).value);
    }

    @Nonnull
    public static ValueWithRanges sargable(@Nonnull Value value, @Nonnull final RangeConstraints range) {
        return new ValueWithRanges(value, ImmutableSet.of(range));
    }

    @Nonnull
    public static ValueWithRanges constraint(@Nonnull final Value value, @Nonnull final Set<RangeConstraints> ranges) {
        return new ValueWithRanges(value, ranges);
    }

    /**
     * Checks whether this predicate implies a {@code candidatePredicate}, if so, we return a {@link PredicateMapping}
     * reflecting the implication itself in addition to a context needed to effectively implement the mapping (i.e. the
     * necessity to apply a residual on top).
     * <br>
     * The implication is done by pattern matching the following cases:
     * <ul>
     *  <li>If {@code candidatePredicate} is a contradiction, we do not have an implication, so we return an empty
     *  mapping.</li>
     *  <li>If {@code candidatePredicate} is a tautology, we always have an implication, so we return a mapping with an application
     *  of residual on top</li>
     *  <li>If {@code candidatePredicate} is a {@link ValueWithRanges} and the values on both sides are semantically equal
     *  to each other and the candidate's domain is unbounded, we have an implication, so
     *  we return a mapping with a residual application on top depending on whether the candidate's alias can be used in
     *  the index scan prefix or not.</li>
     *  <li>If {@code candidatePredicate} is a {@link ValueWithRanges} and the values on both sides are semantically equal
     *  to each other and the candidate domain is bound, then we check if {@code this} range is enclosed by the candidate's
     *  range, if so, we have an implication and we proceed to create a mapping similar to the above logic.</li>
     * </ul>
     * @param aliasMap the current alias map
     * @param candidatePredicate another predicate (usually in a match candidate)
     * @return an optional {@link PredicateMapping} representing the result of the implication.
     */
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
            final var candidate = (ValueWithRanges)candidatePredicate;

            // the value on which the candidate is defined must be the same as the _this_'s value.
            if (!getValue().semanticEquals(candidate.getValue(), aliasMap)) {
                return Optional.empty();
            }

            // candidate has no ranges (i.e. it is not filtered).
            if (candidate.getRanges().isEmpty()) {
                if (candidate instanceof WithAlias) {
                    final var alias = ((WithAlias)candidate).getParameterAlias();
                    return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, boundParameterPrefixMap) -> {
                        if (boundParameterPrefixMap.containsKey(alias)) {
                            return Optional.empty();
                        }
                        return injectCompensationFunctionMaybe();
                    }, alias));
                } else {
                    return Optional.empty();
                }
            }

            if (impliedBy(candidate)) {
                if (candidate instanceof WithAlias) {
                    final var alias = ((WithAlias)candidate).getParameterAlias();
                    return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, boundParameterPrefixMap) -> {
                        if (boundParameterPrefixMap.containsKey(alias)) {
                            return Optional.empty();
                        }
                        return injectCompensationFunctionMaybe();
                    }, alias));
                } else {
                    return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, alsoIgnore) -> {
                        // no need for compensation if range boundaries match between candidate constraint and query sargable
                        if (candidate.impliedBy(this)) {
                            return Optional.empty();
                        }
                        // check if ranges are semantically equal.
                        if (getRanges().stream().allMatch(left -> candidate.getRanges().stream().anyMatch(right -> left.semanticEquals(right, aliasMap)))) {
                            return Optional.empty();
                        }
                        return injectCompensationFunctionMaybe();
                    }));
                }
            }
        }

        //
        // The candidate predicate is not a placeholder which means that the match candidate can not be
        // parameterized by a mapping of this to the candidate predicate. Therefore, in order to match at all,
        // it must be semantically equivalent.
        //
        if (semanticEquals(candidatePredicate, aliasMap)) {
            // Note that we never have to reapply the predicate as both sides are always semantically
            // equivalent.
            return Optional.of(new PredicateMapping(this, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.noCompensationNeeded()));
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
        // todo: check if we have single range and no ranges.
        final ImmutableList.Builder<QueryPredicate> dnfParts = ImmutableList.builder();
        for (final var range : ranges) {
            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            residuals.addAll(range.getComparisons().stream().map(c -> getValue().withComparison(c)).collect(ImmutableList.toImmutableList()));
            dnfParts.add(AndPredicate.and(residuals.build()));
        }
        return OrPredicate.or(dnfParts.build());
    }

    @Nonnull
    @Override
    public Optional<ValueWithRanges> toValueWithRangesMaybe() {
        return Optional.of(this);
    }

    @Override
    public String toString() {
        return "(" + getValue() + ranges.stream().map(RangeConstraints::toString).collect(Collectors.joining("||")) + ")";
    }
}
