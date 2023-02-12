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
import com.google.common.collect.Streams;
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
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange.EvalResult.FALSE;
import static com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange.EvalResult.TRUE;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class ValueWithRanges implements PredicateWithValue {
    @Nonnull
    private final Value value;

    @Nonnull
    private final Optional<CorrelationIdentifier> alias;

    @Nonnull
    private final Set<CompileTimeEvaluableRange> ranges;

    private ValueWithRanges(@Nonnull final Value value, @Nonnull final Set<CompileTimeEvaluableRange> ranges, @Nonnull final Optional<CorrelationIdentifier> alias) {
        this.value = value;
        this.ranges = ImmutableSet.copyOf(ranges);
        this.alias = alias;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    public boolean semanticEqualsWithoutParameterAlias(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        return semanticEquals(other, aliasMap);
    }

    @Nonnull
    @Override
    public ValueWithRanges withValue(@Nonnull final Value value) {
        return new ValueWithRanges(value, ranges, alias);
    }

    @Nonnull
    public ValueWithRanges withExtraRanges(@Nonnull final Set<CompileTimeEvaluableRange> ranges) {
        return new ValueWithRanges(value, Stream.concat(ranges.stream(), this.ranges.stream()).collect(Collectors.toSet()), alias);
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
                .collect(Collectors.toSet());
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
        return Objects.hash(value.semanticHashCode());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.planHash(hashKind, value);
    }

    @Nonnull
    public Set<CompileTimeEvaluableRange> getRanges() {
        return ranges;
    }

    @Nonnull
    public Optional<CorrelationIdentifier> getAliasMaybe() {
        return alias;
    }

    public boolean isSargable() {
        return ranges.size() == 1 && !hasAlias();
    }

    public boolean hasAlias() {
        return alias.isPresent();
    }

    @Nonnull
    public CorrelationIdentifier getAlias() {
        Verify.verify(alias.isPresent(), "attempt to retrieve non-existing alias");
        return alias.get();
    }

    @Nonnull
    @Override
    public ValueWithRanges translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return new ValueWithRanges(value.translateCorrelations(translationMap), ranges.stream().map(range -> range.translateCorrelations(translationMap)).collect(Collectors.toSet()), alias);
    }

    private boolean impliedBy(@Nonnull final ValueWithRanges other) {
        final var leftDisjunction = getRanges();
        final var rightDisjunction = other.getRanges();
        for (final var left : leftDisjunction) {
            if (!left.isCompileTimeEvaluable() || !left.isEmpty().equals(FALSE) || rightDisjunction.stream().noneMatch(right -> right.implies(left).equals(TRUE))) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    public static ValueWithRanges placeholder(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new ValueWithRanges(value, Set.of(), Optional.of(parameterAlias));
    }

    @Nonnull
    public static ValueWithRanges sargable(@Nonnull Value value, @Nonnull final CompileTimeEvaluableRange range) {
        return new ValueWithRanges(value, Set.of(range), Optional.empty());
    }

    @Nonnull
    public static ValueWithRanges constraint(@Nonnull final Value value, @Nonnull final Set<CompileTimeEvaluableRange> ranges) {
        return new ValueWithRanges(value, ranges, Optional.empty());
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
            final var candidate = (ValueWithRanges)candidatePredicate;

            // the value on which the candidate is defined must be the same as the _this_'s value.
            if (!getValue().semanticEquals(candidate.getValue(), aliasMap)) {
                return Optional.empty();
            }

            // candidate has no ranges (i.e. it is not filtered).
            if (candidate.getRanges().isEmpty()) {
                if (candidate.hasAlias()) {
                    return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, boundParameterPrefixMap) -> {
                        if (boundParameterPrefixMap.containsKey(candidate.getAlias())) {
                            return Optional.empty();
                        }
                        return injectCompensationFunctionMaybe();
                    }, candidate.getAlias()));
                } else {
                    return Optional.empty(); // we should probably throw.
                }
            }

            // if this and candidate and compile-time evaluable.
            if (impliedBy(candidate)) {
                if (candidate.hasAlias()) {
                    return Optional.of(new PredicateMapping(this, candidatePredicate, (ignore, boundParameterPrefixMap) -> {
                        if (boundParameterPrefixMap.containsKey(candidate.getAlias())) {
                            return Optional.empty();
                        }
                        return injectCompensationFunctionMaybe();
                    }, candidate.getAliasMaybe()));
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
                    }, candidate.getAliasMaybe()));
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
        // todo: check if we have single range and no ranges.
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
        final var stringBuilder = new StringBuilder();
        stringBuilder.append("(").append(getValue()).append(ranges.stream().map(CompileTimeEvaluableRange::toString).collect(Collectors.joining("||"))).append(")");
        if (hasAlias()) {
            stringBuilder.append(" -> ").append(getAlias());
        }
        return stringBuilder.toString();
    }
}
