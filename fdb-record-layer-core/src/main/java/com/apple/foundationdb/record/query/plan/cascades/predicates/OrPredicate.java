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
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
 *
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

    public OrPredicate(@Nonnull List<QueryPredicate> operands) {
        super(operands);
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
        return new OrPredicate(ImmutableList.copyOf(newChildren));
    }

    @Nonnull
    @Override
    public Optional<PredicateMultiMap.PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap, @Nonnull final QueryPredicate candidatePredicate) {
        if (!(candidatePredicate instanceof OrPredicate)) {
            return super.impliesCandidatePredicate(aliasMap, candidatePredicate);
        }

        final var candidateOr = (OrPredicate)candidatePredicate;

        // if both sides are semantically equal, return a match.
        if (this.semanticEquals(candidateOr, aliasMap)) {
            return Optional.of(new PredicateMultiMap.PredicateMapping(this, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.EMPTY));
        }

        // if the child is not in DNF form, bail out.
        final var candidateChildren = candidateOr.getChildren();
        if (!candidateChildren.stream().allMatch(candidateChild -> candidateChild instanceof ValueWithRanges)) {
            return super.impliesCandidatePredicate(aliasMap, candidatePredicate);
        }

        // ... and same for side
        if (!getChildren().stream().allMatch(child -> child instanceof ValueWithRanges)) {
            return super.impliesCandidatePredicate(aliasMap, candidatePredicate);
        }

        // each leg of this must match a companion from the candidate.
        // also check if we can get an exact match, because if so, we do not need to generate a compnesation.
        var requiresCompensation = false;
        for (final var child : getChildren()) {
            final var leftPredicate = (ValueWithRanges)child;
            for (final var candidateChild : candidateChildren) {
                final var leftRange = Iterables.getOnlyElement(leftPredicate.getRanges()); // sargable
                final var rightRanges = ((ValueWithRanges)candidateChild).getRanges();
                boolean foundMatch = false;
                boolean termRequiresCompensation = true;
                for (final var rightRange : rightRanges) {
                    if (rightRange.implies(leftRange).equals(CompileTimeEvaluableRange.EvalResult.TRUE)) {
                        foundMatch = true;
                        if (leftRange.implies(rightRange).equals(CompileTimeEvaluableRange.EvalResult.TRUE)) {
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
        }
        // need a compensation, because at least one leg did not find an exactly-matching companion, in this case,
        // add this predicate as a residual on top.
        if (requiresCompensation) {
            return Optional.of(new PredicateMultiMap.PredicateMapping(this,
                    candidatePredicate,
                    ((partialMatch, boundParameterPrefixMap) ->
                             Objects.requireNonNull(foldNullable(Function.identity(),
                                     (queryPredicate, childFunctions) -> queryPredicate.injectCompensationFunctionMaybe(partialMatch,
                                             boundParameterPrefixMap,
                                             ImmutableList.copyOf(childFunctions)))))));
        } else {
            return Optional.of(new PredicateMultiMap.PredicateMapping(this, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.EMPTY));
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
            final var childGraphExpansions = childrenInjectCompensationFunctions.stream()
                    .map(childrenInjectCompensationFunction -> childrenInjectCompensationFunction.applyCompensation(translationMap))
                    .collect(ImmutableList.toImmutableList());
            // take the predicates from each individual expansion, "and" them, and then "or" them
            final var quantifiersBuilder = ImmutableList.<Quantifier>builder();
            final var predicatesBuilder = ImmutableList.<QueryPredicate>builder();
            for (final var childGraphExpansion : childGraphExpansions) {
                quantifiersBuilder.addAll(childGraphExpansion.getQuantifiers());
                predicatesBuilder.add(childGraphExpansion.asAndPredicate());
            }

            return GraphExpansion.of(ImmutableList.of(),
                    ImmutableList.of(or(predicatesBuilder.build())),
                    quantifiersBuilder.build(),
                    ImmutableList.of());
        });
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull QueryPredicate first, @Nonnull QueryPredicate second,
                                    @Nonnull QueryPredicate... operands) {
        return or(toList(first, second, operands));
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull Collection<? extends QueryPredicate> children) {
        Verify.verify(!children.isEmpty());
        if (children.size() == 1) {
            return Iterables.getOnlyElement(children);
        }

        return new OrPredicate(ImmutableList.copyOf(children));
    }
}
