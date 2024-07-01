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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PAndPredicate;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link QueryPredicate} that is satisfied when all of its child components are.
 * <br>
 * For tri-valued logic:
 * <ul>
 * <li>If all children are {@code true}, then {@code true}.</li>
 * <li>If any child is {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
public class AndPredicate extends AndOrPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("And-Predicate");

    private AndPredicate(@Nonnull final PlanSerializationContext serializationContext,
                         @Nonnull final PAndPredicate andPredicateProto) {
        super(serializationContext, Objects.requireNonNull(andPredicateProto.getSuper()));
    }

    private AndPredicate(@Nonnull final List<? extends QueryPredicate> children, final boolean isAtomic) {
        super(children, isAtomic);
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
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                List<PlanHashable> hashables = new ArrayList<>(getChildren().size() + 1);
                hashables.add(BASE_HASH);
                hashables.addAll(getChildren());
                return PlanHashable.planHashUnordered(mode, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public AndPredicate withChildren(final Iterable<? extends QueryPredicate> newChildren) {
        return new AndPredicate(ImmutableList.copyOf(newChildren), isAtomic());
    }

    @Nonnull
    @Override
    public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
        final var childrenInjectCompensationFunctions =
                childrenResults.stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(ImmutableList.toImmutableList());
        if (childrenInjectCompensationFunctions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(translationMap -> childrenInjectCompensationFunctions.stream()
                .flatMap(childrenInjectCompensationFunction -> childrenInjectCompensationFunction.applyCompensationForPredicate(translationMap).stream())
                .collect(LinkedIdentitySet.toLinkedIdentitySet()));
    }

    @Nonnull
    @Override
    public AndPredicate withAtomicity(final boolean isAtomic) {
        return new AndPredicate(ImmutableList.copyOf(getChildren()), isAtomic);
    }

    @Nonnull
    @Override
    public PAndPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PAndPredicate.newBuilder().setSuper(toAndOrPredicateProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setAndPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static AndPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PAndPredicate andPredicateProto) {
        return new AndPredicate(serializationContext, andPredicateProto);
    }

    public static QueryPredicate and(@Nonnull QueryPredicate first, @Nonnull QueryPredicate second,
                                     @Nonnull QueryPredicate... operands) {
        return and(toList(first, second, operands), false);
    }

    @Nonnull
    public static QueryPredicate and(@Nonnull final Collection<? extends QueryPredicate> conjuncts) {
        return and(conjuncts, false);
    }

    @Nonnull
    public static QueryPredicate and(@Nonnull final Collection<? extends QueryPredicate> conjuncts, final boolean isAtomic) {
        final var filteredConjuncts =
                conjuncts.stream()
                        .filter(queryPredicate -> !queryPredicate.isTautology())
                        .collect(ImmutableList.toImmutableList());

        if (filteredConjuncts.isEmpty()) {
            return ConstantPredicate.TRUE;
        }

        if (filteredConjuncts.size() == 1) {
            return Iterables.getOnlyElement(filteredConjuncts);
        }

        return new AndPredicate(filteredConjuncts, isAtomic);
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

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PAndPredicate, AndPredicate> {
        @Nonnull
        @Override
        public Class<PAndPredicate> getProtoMessageClass() {
            return PAndPredicate.class;
        }

        @Nonnull
        @Override
        public AndPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PAndPredicate andPredicateProto) {
            return AndPredicate.fromProto(serializationContext, andPredicateProto);
        }
    }
}
