/*
 * AndPredicate.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link QueryPredicate} that is satisfied when all of its child components are.
 *
 * For tri-valued logic:
 * <ul>
 * <li>If all children are {@code true}, then {@code true}.</li>
 * <li>If any child is {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
public class AndPredicate extends AndOrPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("And-Predicate");

    public AndPredicate(@Nonnull List<QueryPredicate> children) {
        super(children);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        Boolean defaultValue = Boolean.TRUE;
        for (QueryPredicate child : getChildren()) {
            final Boolean val = child.eval(store, context);
            if (val == null) {
                defaultValue = null;
            } else if (!val) {
                return false;
            }
        }
        return defaultValue;
    }

    @Override
    public String toString() {
        return getChildren()
                .stream()
                .map(child -> "(" + child + ")")
                .collect(Collectors.joining(" and "));
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
    public AndPredicate withChildren(final Iterable<? extends QueryPredicate> newChildren) {
        return new AndPredicate(ImmutableList.copyOf(newChildren));
    }

    @Nonnull
    @Override
    public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
        final var childrenInjectCompensationFunctions=
                childrenResults.stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(ImmutableList.toImmutableList());
        if (childrenInjectCompensationFunctions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(translationMap -> {
            final var childrenGraphExpansions = childrenInjectCompensationFunctions.stream()
                    .map(childrenInjectCompensationFunction -> childrenInjectCompensationFunction.applyCompensation(translationMap))
                    .collect(ImmutableList.toImmutableList());
            return GraphExpansion.ofOthers(childrenGraphExpansions);
        });
    }

    public static QueryPredicate and(@Nonnull QueryPredicate first, @Nonnull QueryPredicate second,
                                     @Nonnull QueryPredicate... operands) {
        return and(toList(first, second, operands));
    }

    @Nonnull
    public static QueryPredicate and(@Nonnull Collection<? extends QueryPredicate> conjuncts) {
        if (conjuncts.isEmpty()) {
            return ConstantPredicate.TRUE;
        }
        if (conjuncts.size() == 1) {
            return Iterables.getOnlyElement(conjuncts);
        }

        return new AndPredicate(ImmutableList.copyOf(conjuncts));
    }

    @Nonnull
    public static List<? extends QueryPredicate> conjuncts(@Nonnull final QueryPredicate queryPredicate) {
        if (queryPredicate.isTautology()) {
            return ImmutableList.of();
        }

        if (queryPredicate instanceof AndPredicate) {
            return ((AndPredicate)queryPredicate).getChildren();
        }

        return ImmutableList.of(queryPredicate);
    }
}
