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
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An ordering column of a window specification: a {@link Value} paired with its requested sort direction. Carried
 * compile-time on {@link CallSiteArguments.WindowSpecification} so a windowed call site can describe its {@code ORDER
 * BY} columns together with their sort directions.
 */
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public WindowOrderingPart withValue(@Nonnull final Value value) {
        if (value == getValue()) {
            return this;
        }
        return new WindowOrderingPart(value, sortOrder);
    }

    @Nonnull
    public WindowOrderingPart translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplify) {
        final var translatedValue = getValue().translateCorrelations(translationMap, shouldSimplify);
        return withValue(translatedValue);
    }

    @Nonnull
    public WindowOrderingPart rebase(@Nonnull final AliasMap aliasMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(aliasMap), false);
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
    public OrderingPart.RequestedOrderingPart toRequestedOrderingPart() {
        return new OrderingPart.RequestedOrderingPart(getValue(), getSortOrder());
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder getDirectionalSortOrderOrDefault(@Nonnull final OrderingPart.RequestedSortOrder defaultSortOrder) {
        if (sortOrder.isDirectional()) {
            return sortOrder;
        }
        return defaultSortOrder;
    }

    /**
     * Serializes this ordering part.
     *
     * @param serializationContext the serialization context
     *
     * @return the serialized form of this ordering part
     */
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

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other,
                                   @Nonnull final AliasMap aliasMap) {
        if (other == null) {
            return false;
        }

        if (this == other) {
            return true;
        }

        if (!(other instanceof WindowOrderingPart otherWindowOrderingPart)) {
            return false;
        }

        if (getSortOrder() != otherWindowOrderingPart.getSortOrder()) {
            return false;
        }

        return getValue().semanticEquals(otherWindowOrderingPart.getValue(), ValueEquivalence.fromAliasMap(aliasMap))
                .isTrue();
    }

    @Override
    public String toString() {
        return getValue() + getSortOrder().getArrowIndicator();
    }

    /**
     * Deserializes an ordering part.
     *
     * @param serializationContext the serialization context
     * @param proto the serialized form of the ordering part
     *
     * @return the deserialized ordering part
     */
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
}
