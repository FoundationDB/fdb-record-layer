/*
 * WindowOrderingPart.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PWindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class WindowOrderingPart {
    @Nonnull
    private final Value value;

    @Nonnull
    private final OrderingPart.RequestedSortOrder sortOrder;

    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    public WindowOrderingPart(@Nonnull final Value value, @Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        this.value = value;
        this.sortOrder = sortOrder;
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder getSortOrder() {
        return sortOrder;
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder getDirectionalSortOrderOrDefault(@Nonnull final OrderingPart.RequestedSortOrder defaultSortOrder) {
        if (sortOrder.isDirectional()) {
            return sortOrder;
        }
        return defaultSortOrder;
    }

    @Nonnull
    public PWindowOrderingPart toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PWindowOrderingPart.newBuilder()
                .setValue(value.toValueProto(serializationContext))
                .setSortOrder(sortOrderToProto(sortOrder))
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof final WindowOrderingPart keyPart)) {
            return false;
        }
        return getValue().equals(keyPart.getValue()) &&
               getSortOrder() == keyPart.getSortOrder();
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    public int computeHashCode() {
        return Objects.hash(getValue(), getSortOrder().name());
    }

    @Override
    public String toString() {
        return getValue() + getSortOrder().getArrowIndicator();
    }


    @Nonnull
    public static WindowOrderingPart fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PWindowOrderingPart proto) {
        return new WindowOrderingPart(
                Value.fromValueProto(serializationContext, proto.getValue()),
                sortOrderFromProto(proto.getSortOrder()));
    }

    @Nonnull
    private static PWindowOrderingPart.PSortOrder sortOrderToProto(@Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        return switch (sortOrder) {
            case ASCENDING -> PWindowOrderingPart.PSortOrder.ASCENDING;
            case DESCENDING -> PWindowOrderingPart.PSortOrder.DESCENDING;
            case ASCENDING_NULLS_LAST -> PWindowOrderingPart.PSortOrder.ASCENDING_NULLS_LAST;
            case DESCENDING_NULLS_FIRST -> PWindowOrderingPart.PSortOrder.DESCENDING_NULLS_FIRST;
            case ANY -> PWindowOrderingPart.PSortOrder.ANY;
        };
    }

    @Nonnull
    private static OrderingPart.RequestedSortOrder sortOrderFromProto(@Nonnull final PWindowOrderingPart.PSortOrder proto) {
        return switch (proto) {
            case ASCENDING -> OrderingPart.RequestedSortOrder.ASCENDING;
            case DESCENDING -> OrderingPart.RequestedSortOrder.DESCENDING;
            case ASCENDING_NULLS_LAST -> OrderingPart.RequestedSortOrder.ASCENDING_NULLS_LAST;
            case DESCENDING_NULLS_FIRST -> OrderingPart.RequestedSortOrder.DESCENDING_NULLS_FIRST;
            case ANY -> OrderingPart.RequestedSortOrder.ANY;
        };
    }

    /**
     * Converts a sequence of {@link WindowOrderingPart}s into a {@link RequestedOrdering} suitable for the planner.
     * The values are rebased from their correlated alias to {@link Quantifier#current()} so that the resulting ordering
     * can be matched against candidate access paths.
     *
     * @param windowOrderingParts the ordering parts to convert
     * @param constantAliases aliases that are considered constant (bound by equality predicates)
     * @return a {@link RequestedOrdering} representing the required sort order, or
     *         {@link RequestedOrdering#preserve()} if no ordering is needed
     */
    @Nonnull
    public static RequestedOrdering toRequestedOrdering(@Nonnull final Iterable<WindowOrderingPart> windowOrderingParts,
                                                        @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        if (Iterables.isEmpty(windowOrderingParts)) {
            return RequestedOrdering.preserve();
        }

        final var correlatedTos = Streams.stream(windowOrderingParts)
                .flatMap(w -> w.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
        Verify.verify(correlatedTos.size() <= 1);
        if (correlatedTos.isEmpty()) {
            return RequestedOrdering.preserve();
        }

        final var aliasMap = AliasMap.ofAliases(Iterables.getOnlyElement(correlatedTos), Quantifier.current());
        final var orderingParts = Streams.stream(windowOrderingParts)
                .map(wop -> new OrderingPart.RequestedOrderingPart(wop.getValue().rebase(aliasMap), wop.getSortOrder()))
                .collect(ImmutableList.toImmutableList());
        return RequestedOrdering.ofParts(orderingParts, RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS, false, constantAliases);
    }
}
