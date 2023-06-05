/*
 * OrPredicate.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link QueryPredicate} that is satisfied when any of its child components is satisfied.
 * <br>
 * For tri-valued logic:
 * <ul>
 * <li>If any child is {@code true}, then {@code true}.</li>
 * <li>If all children are {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class OrPredicate extends AndOrPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Or-Predicate");

    private OrPredicate(@Nonnull final List<QueryPredicate> operands, final boolean isAtomic) {
        super(operands, isAtomic);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        Boolean defaultValue = Boolean.FALSE;
        for (QueryPredicate child : getChildren()) {
            final Boolean val = child.eval(store, context);
            if (val == null) {
                defaultValue = null;
            } else if (val) {
                return true;
            }
        }
        return defaultValue;
    }

    @Override
    public String toString() {
        return getChildren()
                .stream()
                .map(child -> "(" + child + ")")
                .collect(Collectors.joining(" or "));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                List<PlanHashable> hashables = new ArrayList<>(getChildren().size() + 1);
                hashables.add(BASE_HASH);
                hashables.addAll(getChildren());
                return PlanHashable.planHashUnordered(hashKind, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public OrPredicate withChildren(final Iterable<? extends QueryPredicate> newChildren) {
        return new OrPredicate(ImmutableList.copyOf(newChildren), isAtomic());
    }

    @Nonnull
    @Override
    public Optional<PredicateWithValueAndRanges> toValueWithRangesMaybe(final @Nonnull EvaluationContext evaluationContext) {
        // expression hierarchy must be of a single level, all children must be simple .
        if (!getChildren().stream().allMatch(child -> child instanceof PredicateWithValue)) {
            return Optional.empty();
        }
        // all children must reference the same value
        if (getChildren().stream().map(c -> ((PredicateWithValue)c).getValue()).distinct().count() > 1) {
            return Optional.empty();
        }

        final var value = ((PredicateWithValue)getChildren().stream().findFirst().orElseThrow()).getValue();
        final ImmutableSet.Builder<RangeConstraints> rangesSet = ImmutableSet.builder();

        for (final var child : getChildren()) {
            final var rangesBuilder = RangeConstraints.newBuilder();
            if (child instanceof ValuePredicate) {
                final var valuePredicate = (ValuePredicate)child;
                if (!rangesBuilder.addComparisonMaybe(valuePredicate.getComparison())) {
                    return Optional.empty();
                }
            } else if (child instanceof PredicateWithValueAndRanges) {
                rangesSet.addAll(((PredicateWithValueAndRanges)child).getRanges());
            } else {
                // unknown child type.
                return Optional.empty();
            }

            final var range = rangesBuilder.build();
            if (range.isEmpty()) {
                return Optional.empty();
            }
            rangesSet.add(range.get());
        }

        return Optional.of(PredicateWithValueAndRanges.ofRanges(value, rangesSet.build()));
    }

    /**
     * Checks for implication with a match candidate predicate by constructing a disjunction set of compile-time
     * ranges of the {@code this} and the candidate predicate and, if the construction is possible, matches them.
     * Matching the disjunction sets works as the following:
     * <br>
     * given an LHS that is: range(x1,x2) ∪ range(x3, x4) ∪ range(x5, x6) and RHS that is range(y1, y2) ∪ range (y3,
     * y4):
     * - each range in the LHS must find a companion range in RHS that implies it, if not, we reject the candidate
     * predicate.
     * <br>
     * - if each companion range is _also_ implied by the LHS range, we have a match that does not require any
     * compensation.
     * <br>
     * - otherwise, we match with a compensation that is effectively the reapplication of the entire LHS on top.
     * <br>
     * <b>example 1:</b>
     * - LHS: range(3, 10) ∪ range(15, 20), range (50, 60)
     * - RHS: range(0, 50) ∪ range (51,200)
     * result:
     * - match with {@code this} applied as a residual.
     * <br>
     * <b>example 2:</b>
     * - LHS: range(3,10) ∪ range(15,20)
     * - RHS: range(15,20) ∪ range(3,10)
     * result:
     * - exact match (no compensation required)
     * <br>
     * <b>example 3:</b>
     * - LHS: range(3,10) ∪ range(15,20)
     * - RHS: range(3,17)
     * result:
     * - no match.
     *
     * @param aliasMap the current alias map.
     * @param candidatePredicate another predicate to match.
     * @param evaluationContext the evaluation context used to evaluate any compile-time constants when examining predicate
     * implication.
     *
     * @return optional match mapping.
     */
    @Nonnull
    @Override
    public Optional<PredicateMultiMap.PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap,
                                                                                  @Nonnull final QueryPredicate candidatePredicate,
                                                                                  @Nonnull final EvaluationContext evaluationContext) {
        Optional<PredicateMultiMap.PredicateMapping> mappingsOptional = super.impliesCandidatePredicate(aliasMap, candidatePredicate, evaluationContext);
        if (mappingsOptional.isPresent()) {
            return mappingsOptional;
        }

        final var valueWithRangesOptional = toValueWithRangesMaybe(evaluationContext);
        if (valueWithRangesOptional.isPresent()) {
            final var leftValueWithRanges = valueWithRangesOptional.get();

            final var candidateValueWithRangesOptional = candidatePredicate.toValueWithRangesMaybe(evaluationContext);
            if (candidateValueWithRangesOptional.isPresent()) {
                final var rightValueWithRanges = candidateValueWithRangesOptional.get();
                mappingsOptional = impliesWithValuesAndRanges(aliasMap, candidatePredicate, evaluationContext, leftValueWithRanges, rightValueWithRanges);
            }
        }

        if (mappingsOptional.isEmpty() && candidatePredicate instanceof Placeholder) {
            final var candidateValue = ((Placeholder)candidatePredicate).getValue();
            final var anyMatchingLeafPredicate =
                    Streams.stream(inPreOrder())
                            .filter(predicate -> predicate instanceof LeafQueryPredicate)
                            .anyMatch(predicate -> {
                                if (predicate instanceof PredicateWithValue) {
                                    final var queryValue = ((ValuePredicate)predicate).getValue();
                                    return queryValue.semanticEquals(candidateValue, aliasMap);
                                }
                                return false;
                            });
            if (anyMatchingLeafPredicate) {
                //
                // There is a sub-term that could be matched if the OR was broken into a UNION. Mark this as a
                // special mapping.
                //
                return Optional.of(PredicateMultiMap.PredicateMapping.orTermMapping(this,
                        new ConstantPredicate(true),
                        getDefaultCompensatePredicateFunction()));
            }
        }

        return mappingsOptional;
    }

    @Nonnull
    private Optional<PredicateMultiMap.PredicateMapping> impliesWithValuesAndRanges(@Nonnull final AliasMap aliasMap,
                                                                                    @Nonnull final QueryPredicate candidatePredicate,
                                                                                    @Nonnull final EvaluationContext evaluationContext,
                                                                                    @Nonnull final PredicateWithValueAndRanges leftValueWithRanges,
                                                                                    @Nonnull final PredicateWithValueAndRanges rightValueWithRanges) {
        if (!leftValueWithRanges.getValue().semanticEquals(rightValueWithRanges.getValue(), aliasMap)) {
            return Optional.empty();
        }

        // each leg of this must match a companion from the candidate.
        // also check if we can get an exact match, because if so, we do not need to generate a compensation.
        var requiresCompensation = false;
        for (final var leftRange : leftValueWithRanges.getRanges()) {
            boolean termRequiresCompensation = true;
            boolean foundMatch = false;
            for (final var rightRange : rightValueWithRanges.getRanges()) {
                final var evaledLeft = leftRange.compileTimeEval(evaluationContext);
                if (rightRange.encloses(evaledLeft, evaluationContext) == Proposition.TRUE) {
                    foundMatch = true;
                    if (evaledLeft.encloses(rightRange, evaluationContext) == Proposition.TRUE) {
                        termRequiresCompensation = false;
                        break;
                    }
                }
            }
            if (!foundMatch) {
                return Optional.empty();
            }
            requiresCompensation = requiresCompensation || termRequiresCompensation;
        }

        // need a compensation, because at least one leg did not find an exactly-matching companion, in this case,
        // add this predicate as a residual on top.
        if (requiresCompensation) {
            return Optional.of(PredicateMultiMap.PredicateMapping.regularMapping(this,
                    candidatePredicate,
                    ((partialMatch, boundParameterPrefixMap) ->
                             Objects.requireNonNull(foldNullable(Function.identity(),
                                     (queryPredicate, childFunctions) -> queryPredicate.injectCompensationFunctionMaybe(partialMatch,
                                             boundParameterPrefixMap,
                                             ImmutableList.copyOf(childFunctions)))))));
        } else {
            return Optional.of(PredicateMultiMap.PredicateMapping.regularMapping(this, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.noCompensationNeeded()));
        }
    }

    @Nonnull
    @Override
    public Optional<PredicateMultiMap.ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                                  @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                                  @Nonnull final List<Optional<PredicateMultiMap.ExpandCompensationFunction>> childrenResults) {
        final var childrenInjectCompensationFunctions =
                childrenResults.stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(ImmutableList.toImmutableList());
        if (childrenInjectCompensationFunctions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(translationMap -> {
            final var childPredicatesList = childrenInjectCompensationFunctions.stream()
                    .map(childrenInjectCompensationFunction -> childrenInjectCompensationFunction.applyCompensationForPredicate(translationMap))
                    .collect(ImmutableList.toImmutableList());
            // take the predicates from each individual expansion, "and" them, and then "or" them
            final var predicates = LinkedIdentitySet.<QueryPredicate>of();
            for (final var childPredicates : childPredicatesList) {
                predicates.add(AndPredicate.andOrTrue(childPredicates));
            }
            return LinkedIdentitySet.of(OrPredicate.or(predicates));
        });
    }

    @Nonnull
    @Override
    public OrPredicate withAtomicity(final boolean isAtomic) {
        return new OrPredicate(ImmutableList.copyOf(getChildren()), isAtomic);
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull QueryPredicate first, @Nonnull QueryPredicate second,
                                    @Nonnull QueryPredicate... operands) {
        return of(toList(first, second, operands), false);
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull final Collection<? extends QueryPredicate> children) {
        return of(children, false);
    }

    @Nonnull
    public static QueryPredicate orOrTrue(@Nonnull final Collection<? extends QueryPredicate> disjuncts) {
        if (disjuncts.isEmpty()) {
            return ConstantPredicate.TRUE;
        }
        return of(disjuncts, false);
    }

    @Nonnull
    public static QueryPredicate orOrFalse(@Nonnull final Collection<? extends QueryPredicate> disjuncts) {
        if (disjuncts.isEmpty()) {
            return ConstantPredicate.FALSE;
        }
        return of(disjuncts, false);
    }

    @Nonnull
    public static QueryPredicate of(@Nonnull final Collection<? extends QueryPredicate> disjuncts, final boolean isAtomic) {
        Verify.verify(!disjuncts.isEmpty());
        if (disjuncts.size() == 1) {
            return Iterables.getOnlyElement(disjuncts);
        }

        return new OrPredicate(ImmutableList.copyOf(disjuncts), isAtomic);
    }
}
