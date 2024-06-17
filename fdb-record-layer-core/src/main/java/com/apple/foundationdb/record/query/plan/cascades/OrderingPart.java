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
 * A value that is used to express ordered-ness. The base class {@code OrderingPart} itself only has a protected
 * constructor. All subclasses are also final static nested classes of {@code OrderingPart}, thus emulating a sealed
 * trait.
 * @param <S> the sort order that is being used
 */
public class OrderingPart<S extends OrderingPart.SortOrder> {
    @Nonnull
    private final Value value;

    @Nonnull
    private final S sortOrder;

    private final Supplier<Integer> hashCodeSupplier;

    protected OrderingPart(@Nonnull final Value value, @Nonnull final S sortOrder) {
        this.value = checkValue(value);
        this.sortOrder = sortOrder;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    public S getSortOrder() {
        return sortOrder;
    }

    @Nonnull
    public S getDirectionalSortOrderOrDefault(@Nonnull final S defaultSortOrder) {
        if (sortOrder.isDirectional()) {
            return sortOrder;
        }
        return defaultSortOrder;
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
     * A common interface all sort orders have to implement. Although, all sort orders offer enums for ascending and
     * descending order and are thus somewhat overlapping in nature, different use cases may warrant subtle differences.
     * For instance, a {@link ProvidedSortOrder} uses a {@link ProvidedSortOrder#FIXED} to indicate that an item is
     * a fixed value (effectively a constant), however, a {@link RequestedSortOrder} does not support to request for
     * fixed values. Conversely, a {@link RequestedSortOrder} can indicate {@link RequestedSortOrder#ANY} which
     * indicates a sort order that is either ascending or descending (but not nothing). In an earlier iteration of this
     * logic all sort orders where in one enum which led to problems as unexpected sort orders enum values were passed
     * to logic that was not able to digest it. Making it separate classes based on use case allows us to have the Java
     * compiler to ensure that only meaningful sort orders are processed by the right logic.
     */
    public interface SortOrder {
        /**
         * Name of the sort order; is implemented by the enum implementing this interface.
         * @return the name of this enum value
         */
        @Nonnull
        String name();

        /**
         * Arrow indicator for pretty-printing the sort order.
         * @return a string containing an arrow indicator
         */
        @Nonnull
        String getArrowIndicator();

        /**
         * All sort orders should represent some notion of ascending and descending. While the client of this
         * interface should always use the actual enum values for ascending and descending in the implementing enum,
         * this method just returns an indicator if the sort order is directional, i.e. it is either ascending or
         * descending.
         * @return a boolean indicator; {@code true} iff this sort order is ascending or descending.
         */
        boolean isDirectional();
    }

    /**
     * TODO.
     */
    public enum ProvidedSortOrder implements SortOrder {
        ASCENDING("↑"),
        DESCENDING("↓"),
        FIXED("="),
        CHOOSE("?");

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

        @Override
        public boolean isDirectional() {
            return this == ASCENDING || this == DESCENDING || this == CHOOSE;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isCompatibleWithRequestedSortOrder(@Nonnull final RequestedSortOrder requestedSortOrder) {
            if (requestedSortOrder == RequestedSortOrder.ANY || this == CHOOSE || !isDirectional()) {
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
    public enum MatchedSortOrder implements SortOrder {
        ASCENDING("↑"),
        DESCENDING("↓");

        @Nonnull
        private final String arrowIndicator;

        MatchedSortOrder(@Nonnull final String arrowIndicator) {
            this.arrowIndicator = arrowIndicator;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return arrowIndicator;
        }

        public boolean isReverse() {
            return this == DESCENDING;
        }

        @Override
        public boolean isDirectional() {
            return true;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isCompatibleWithRequestedSortOrder(@Nonnull final RequestedSortOrder requestedSortOrder) {
            if (requestedSortOrder == RequestedSortOrder.ANY || !isDirectional()) {
                return true;
            }

            return this == ASCENDING && requestedSortOrder == RequestedSortOrder.ASCENDING ||
                    this == DESCENDING && requestedSortOrder == RequestedSortOrder.DESCENDING;
        }

        @Nonnull
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

        @Nonnull
        public static MatchedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }
    }

    /**
     * TODO.
     */
    public enum RequestedSortOrder implements SortOrder {
        ASCENDING("↑"),
        DESCENDING("↓"),
        ANY("↕");

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

        @Override
        public boolean isDirectional() {
            return this == ASCENDING || this == DESCENDING;
        }

        @Nonnull
        public static RequestedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }

        @Nonnull
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
    public static final class ProvidedOrderingPart extends OrderingPart<ProvidedSortOrder> {
        public ProvidedOrderingPart(@Nonnull final Value value, final ProvidedSortOrder sortOrder) {
            super(value, sortOrder);
        }
    }

    /**
     * TODO.
     */
    public static final class RequestedOrderingPart extends OrderingPart<RequestedSortOrder> {
        public RequestedOrderingPart(@Nonnull final Value value, final RequestedSortOrder sortOrder) {
            super(value, sortOrder);
        }
    }

    /**
     * An {@link OrderingPart} that is bound by a comparison during graph matching.
     */
    public static final class MatchedOrderingPart extends OrderingPart<MatchedSortOrder> {
        @Nonnull
        private final CorrelationIdentifier parameterId;

        @Nonnull
        private final ComparisonRange comparisonRange;

        /**
         * Constructor.
         * @param parameterId the unique identifier of this part in the match candidate
         * @param orderByValue value that defines what to order by
         * @param comparisonRange comparison used to match this ordering part
         */
        private MatchedOrderingPart(@Nonnull final CorrelationIdentifier parameterId,
                                    @Nonnull final Value orderByValue,
                                    @Nonnull final ComparisonRange comparisonRange,
                                    @Nonnull final MatchedSortOrder matchedSortOrder) {
            super(orderByValue, matchedSortOrder);
            this.parameterId = parameterId;
            this.comparisonRange = comparisonRange;
        }

        @Nonnull
        public CorrelationIdentifier getParameterId() {
            return parameterId;
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
            return Objects.equals(parameterId, that.parameterId) && Objects.equals(comparisonRange, that.comparisonRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), parameterId, comparisonRange);
        }

        @Nonnull
        public MatchedOrderingPart demote() {
            Verify.verify(getComparisonRange().isEquality());
            return new MatchedOrderingPart(getParameterId(), getValue(), ComparisonRange.EMPTY, getSortOrder());
        }

        @Nonnull
        public static MatchedOrderingPart of(@Nonnull final CorrelationIdentifier parameterId,
                                             @Nonnull final Value orderByValue,
                                             @Nullable final ComparisonRange comparisonRange,
                                             @Nonnull final MatchedSortOrder matchedSortOrder) {
            return new MatchedOrderingPart(parameterId, orderByValue,
                    comparisonRange == null ? ComparisonRange.EMPTY : comparisonRange, matchedSortOrder);
        }
    }
}
