/*
 * OrderingPart.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that is used to express ordered-ness.
 * @param <S> the sort order that is being used
 */
public class OrderingPart<S extends OrderingPart.SortOrder> {
    @Nonnull
    private final Value value;

    private final S sortOrder;

    private final Supplier<Integer> hashCodeSupplier;

    protected OrderingPart(@Nonnull final Value value, final S sortOrder) {
        this.value = checkValue(value);
        this.sortOrder = sortOrder;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    public S getSortOrder() {
        return sortOrder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderingPart)) {
            return false;
        }
        final var keyPart = (OrderingPart<?>)o;
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
    public static <S extends SortOrder> List<OrderingPart<S>> prefix(@Nonnull final List<? extends OrderingPart<S>> keyParts, final int endExclusive) {
        return ImmutableList.copyOf(keyParts.subList(0, endExclusive));
    }

    @Nonnull
    private static Value checkValue(@Nonnull final Value value) {
        final var correlatedTo = value.getCorrelatedTo();
        Verify.verify(correlatedTo.size() <= 1);
        Verify.verify(correlatedTo.isEmpty() || Iterables.getOnlyElement(correlatedTo).equals(Quantifier.current()));
        return value;
    }

    /**
     * TODO.
     */
    public interface SortOrder {
        // nothing

        @Nonnull
        String name();

        @Nonnull
        String getArrowIndicator();
    }

    /**
     * TODO.
     */
    public enum ProvidedSortOrder implements SortOrder {
        ASCENDING("↑"),
        DESCENDING("↓"),
        FIXED("=");

        @Nonnull
        private final String arrowIndicator;

        ProvidedSortOrder(@Nonnull final String arrowIndicator) {
            this.arrowIndicator = arrowIndicator;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return arrowIndicator;
        }

        public boolean isReverse() {
            if (this == FIXED) {
                throw new RecordCoreException("cannot determine if this is reverse or not");
            }
            return this == DESCENDING;
        }

        public boolean isDirectional() {
            return this == ASCENDING || this == DESCENDING;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isCompatibleWithRequestedSortOrder(@Nonnull final RequestedSortOrder requestedSortOrder) {
            if (requestedSortOrder == RequestedSortOrder.ANY || !isDirectional()) {
                return true;
            }

            return this == ASCENDING && requestedSortOrder == RequestedSortOrder.ASCENDING ||
                    this == DESCENDING && requestedSortOrder == RequestedSortOrder.DESCENDING;
        }

        public RequestedSortOrder toRequestedSortOrder() {
            switch (this) {
                case ASCENDING:
                    return RequestedSortOrder.ASCENDING;
                case DESCENDING:
                    return RequestedSortOrder.DESCENDING;
                default:
                    throw new RecordCoreException("cannot translate this sort order to requested sort order");
            }
        }

        @Nonnull
        public static ProvidedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }
    }

    /**
     * TODO.
     */
    public enum RequestedSortOrder implements SortOrder {
        ASCENDING("↑"),
        DESCENDING("↓"),
        ANY("↑↓");

        @Nonnull
        private final String arrowIndicator;

        RequestedSortOrder(@Nonnull final String arrowIndicator) {
            this.arrowIndicator = arrowIndicator;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return arrowIndicator;
        }

        public boolean isReverse() {
            if (this == ANY) {
                throw new RecordCoreException("cannot determine if this is reverse or not");
            }
            return this == DESCENDING;
        }

        public boolean isDirectional() {
            return this == ASCENDING || this == DESCENDING;
        }

        @Nonnull
        public static RequestedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }

        public ProvidedSortOrder toProvidedSortOrder() {
            switch (this) {
                case ASCENDING:
                    return ProvidedSortOrder.ASCENDING;
                case DESCENDING:
                    return ProvidedSortOrder.DESCENDING;
                default:
                    throw new RecordCoreException("cannot translate this sort order to provided sort order");
            }
        }
    }

    /**
     * TODO.
     */
    public static class ProvidedOrderingPart extends OrderingPart<ProvidedSortOrder> {
        public ProvidedOrderingPart(@Nonnull final Value value, final ProvidedSortOrder sortOrder) {
            super(value, sortOrder);
        }
    }

    /**
     * TODO.
     */
    public static class RequestedOrderingPart extends OrderingPart<RequestedSortOrder> {
        public RequestedOrderingPart(@Nonnull final Value value, final RequestedSortOrder sortOrder) {
            super(value, sortOrder);
        }
    }

    /**
     * An {@link OrderingPart} that is bound by a comparison during graph matching.
     */
    public static class MatchedOrderingPart extends OrderingPart<ProvidedSortOrder> {
        @Nonnull
        private final ComparisonRange comparisonRange;

        /**
         * Constructor.
         * @param orderByValue value that defines what to order by
         * @param comparisonRange comparison used to match this ordering part
         */
        private MatchedOrderingPart(@Nonnull final Value orderByValue,
                                    @Nonnull final ComparisonRange comparisonRange,
                                    final boolean isReverse) {
            super(orderByValue, ProvidedSortOrder.fromIsReverse(isReverse));
            this.comparisonRange = comparisonRange;
        }

        @Nonnull
        public ComparisonRange getComparisonRange() {
            return comparisonRange;
        }

        @Nonnull
        public ComparisonRange.Type getComparisonRangeType() {
            return comparisonRange.getRangeType();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MatchedOrderingPart)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            final MatchedOrderingPart that = (MatchedOrderingPart)o;
            return Objects.equals(comparisonRange, that.comparisonRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), comparisonRange);
        }

        @Nonnull
        public static MatchedOrderingPart of(@Nonnull final Value orderByValue,
                                             @Nullable final ComparisonRange comparisonRange,
                                             final boolean isReverse) {
            return new MatchedOrderingPart(orderByValue,
                    comparisonRange == null ? ComparisonRange.EMPTY : comparisonRange,
                    isReverse);
        }
    }
}
