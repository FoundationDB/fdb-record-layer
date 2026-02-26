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

import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.TupleOrdering.Direction;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A class that is used to express ordered-ness. The base class {@code OrderingPart} itself only has a protected
 * constructor. All subclasses are also final static nested classes of {@code OrderingPart}, thus emulating a sealed
 * trait.
 * @param <S> the type sort order that is being used
 */
public class OrderingPart<S extends OrderingPart.SortOrder> {
    @Nonnull
    private final Value value;

    @Nonnull
    private final S sortOrder;

    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    protected OrderingPart(@Nonnull final Value value, @Nonnull final S sortOrder) {
        this.value = checkValue(value);
        this.sortOrder = sortOrder;
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

    @Nonnull
    public static <O extends OrderingPart<S>, S extends SortOrder> Map<Value, O> toOrderingPartMap(@Nonnull final Iterable<O> orderingParts) {
        final var resultMapBuilder = ImmutableMap.<Value, O>builder();
        for (final O orderingPart : orderingParts) {
            resultMapBuilder.put(orderingPart.getValue(), orderingPart);
        }
        return resultMapBuilder.build();
    }

    @Nonnull
    public static List<Value> toValues(@Nonnull final Iterable<? extends OrderingPart<?>> orderingParts) {
        final var resultsBuilder = ImmutableList.<Value>builder();
        for (final var orderingPart : orderingParts) {
            resultsBuilder.add(orderingPart.getValue());
        }
        return resultsBuilder.build();
    }

    /**
     * A common interface all sort orders have to implement. Although, all sort orders offer enums for ascending and
     * descending order and are thus somewhat overlapping in nature, different use cases may warrant subtle differences.
     * For instance, a {@link ProvidedSortOrder} uses a {@link ProvidedSortOrder#FIXED} to indicate that an item is
     * a fixed value (effectively a constant), however, a {@link RequestedSortOrder} does not support to request for
     * fixed values. Conversely, a {@link RequestedSortOrder} can indicate {@link RequestedSortOrder#ANY} which
     * indicates a sort order that is either ascending or descending (but not nothing). In an earlier iteration of this
     * logic all sort orders were in one enum which led to problems as unexpected sort orders enum values were passed
     * to logic that was not able to digest it. Making it separate classes based on use case allows us to have the Java
     * compiler ensure that only meaningful sort orders are processed by the right logic.
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

        /**
         * Get the corresponding tuple ordering direction.
         * @return the corresponding {@link Direction}.
         */
        @Nonnull
        Direction getTupleDirection();

        default boolean isAnyAscending() {
            Verify.verify(isDirectional());
            return getTupleDirection().isAscending();
        }

        default boolean isAnyDescending() {
            Verify.verify(isDirectional());
            return getTupleDirection().isDescending();
        }

        default boolean isNullsFirst() {
            Verify.verify(isDirectional());
            return getTupleDirection().isNullsFirst();
        }

        default boolean isNullsLast() {
            Verify.verify(isDirectional());
            return getTupleDirection().isNullsLast();
        }

        default boolean isCounterflowNulls() {
            Verify.verify(isDirectional());
            return getTupleDirection().isCounterflowNulls();
        }

        @Nonnull
        static <SO extends SortOrder> EnumMap<Direction, SO> computeDirectionToSortOrder(@Nonnull final Class<SO> soClass) {
            final EnumMap<Direction, SO> directionToSortOrderMap = new EnumMap<>(Direction.class);

            final var values = soClass.getEnumConstants();
            for (final var value : values) {
                if (value.isDirectional()) {
                    directionToSortOrderMap.put(value.getTupleDirection(), value);
                }
            }
            return directionToSortOrderMap;
        }

        @Nonnull
        static <FO extends SortOrder, TO extends SortOrder> TO mapToSortOrder(@Nonnull final FO fo,
                                                                              @Nonnull final EnumMap<Direction, TO> directionToSortOrderMap) {
            Verify.verify(fo.isDirectional());
            return Objects.requireNonNull(directionToSortOrderMap.get(fo.getTupleDirection()));
        }

        @Nonnull
        static <FO extends SortOrder, TO extends SortOrder> TO mapToReverseSortOrder(@Nonnull final FO fo,
                                                                                     @Nonnull final EnumMap<Direction, TO> directionToSortOrderMap) {
            Verify.verify(fo.isDirectional());
            return Objects.requireNonNull(directionToSortOrderMap.get(fo.getTupleDirection().reverseDirection()));
        }
    }

    /**
     * Enum implementing {@link SortOrder} that provides sort orders that for instance a plan can <em>provide</em> to
     * downstream operators.
     */
    public enum ProvidedSortOrder implements SortOrder {
        /**
         * Ascending.
         */
        ASCENDING(Direction.ASC_NULLS_FIRST),
        /**
         * Descending.
         */
        DESCENDING(Direction.DESC_NULLS_LAST),
        /**
         * Ascending but with nulls after regular values.
         */
        ASCENDING_NULLS_LAST(Direction.ASC_NULLS_LAST),
        /**
         * Descending but with nulls before regular values.
         */
        DESCENDING_NULLS_FIRST(Direction.DESC_NULLS_FIRST),
        /**
         * Fixed sort order which indicates that something restrict records to only ever be of exactly one value.
         */
        FIXED("="),
        /**
         * Choose the sort order. This is only ever used by intermediate orderings such as
         * {@link Ordering.SetOperationsOrdering}. This enum value indicates that the sort order of this part can be
         * chosen by e.g. applying a comparison key. It usually means that there were multiple fixed values that
         * a comparison key can order freely in any way (ascending or descending). When this enum value is set we
         * do not know yet if the order eventually will become ascending or descending. Note that {@code CHOOSE} does
         * not mean that to just ignore the associated ordering part when enumerating comparison keys. It will
         * have to be either ascending or descending; it cannot be nothing.
         */
        CHOOSE("?");

        private static final EnumMap<Direction, ProvidedSortOrder> directionToSortOrderMap;

        static {
            directionToSortOrderMap = SortOrder.computeDirectionToSortOrder(ProvidedSortOrder.class);
        }

        @Nullable
        private final Direction tupleDirection;
        @Nonnull
        private final String arrowIndicator;

        ProvidedSortOrder(@Nonnull final Direction tupleDirection) {
            this.tupleDirection = tupleDirection;
            this.arrowIndicator = tupleDirection.getArrowIndicator();
        }

        ProvidedSortOrder(@Nonnull final String arrowIndicator) {
            this.tupleDirection = null;
            this.arrowIndicator = arrowIndicator;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return arrowIndicator;
        }

        @Override
        public boolean isDirectional() {
            switch (this) {
                case ASCENDING:
                case DESCENDING:
                case ASCENDING_NULLS_LAST:
                case DESCENDING_NULLS_FIRST:
                    return true;
                default:
                    return false;
            }
        }

        @Nonnull
        @Override
        public Direction getTupleDirection() {
            Verify.verify(isDirectional());
            return Objects.requireNonNull(tupleDirection);
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isCompatibleWithRequestedSortOrder(@Nonnull final RequestedSortOrder requestedSortOrder) {
            if (requestedSortOrder == RequestedSortOrder.ANY || this == CHOOSE || this == FIXED) {
                return true;
            }

            if (isCounterflowNulls() != requestedSortOrder.isCounterflowNulls()) {
                return false;
            }

            return this.isAnyAscending() == requestedSortOrder.isAnyAscending();
        }

        @Nonnull
        public ProvidedSortOrder flipIfReverse(final boolean isReverse) {
            if (isReverse) {
                return SortOrder.mapToReverseSortOrder(this, ProvidedSortOrder.getDirectionToSortOrderMap());
            }
            return this;
        }

        @Nonnull
        public MatchedSortOrder toMatchedSortOrder() {
            return SortOrder.mapToSortOrder(this, MatchedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        public RequestedSortOrder toRequestedSortOrder() {
            return SortOrder.mapToSortOrder(this, RequestedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        static EnumMap<Direction, ProvidedSortOrder> getDirectionToSortOrderMap() {
            return directionToSortOrderMap;
        }

        @Nonnull
        public static ProvidedSortOrder fromDirection(@Nonnull final Direction direction) {
            return Objects.requireNonNull(directionToSortOrderMap.get(direction));
        }

        @Nonnull
        public static ProvidedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }
    }

    /**
     * Sort order that can be assigned during index matching. Note that this sort only distinguishes between ascending
     * and descending. Ascending and descending are pretty much meaningless here without knowing what the scan direction
     * will be which is not known during matching. The only important semantic meaning that is imposed is that
     * the sort order ascending and the sort order descending are polar opposites of each other. These enum values
     * could also be named regular and inverse, or black and white. The reason they are named ascending and descending
     * is that by convention the ascending sort order becomes the actual ascending sort order when a forward scan is
     * used; the descending sort order becomes descending sort order when a forward scan is used. On the contrary,
     * ascending becomes descending, and descending becomes ascending if a reverse scan is used.
     * The matched sort order is only ever set to descending when modelling and inverse ordering values, i.e.
     * the sort order of {@code inverse(fieldValue(q, x))} is descending iff the sort order of
     * {@code fieldValue(q, x)} is ascending.
     */
    public enum MatchedSortOrder implements SortOrder {
        ASCENDING(Direction.ASC_NULLS_FIRST),
        DESCENDING(Direction.DESC_NULLS_LAST),
        ASCENDING_NULLS_LAST(Direction.ASC_NULLS_LAST),
        DESCENDING_NULLS_FIRST(Direction.DESC_NULLS_FIRST);

        private static final EnumMap<Direction, MatchedSortOrder> directionToSortOrderMap;

        static {
            directionToSortOrderMap = SortOrder.computeDirectionToSortOrder(MatchedSortOrder.class);
        }

        @Nonnull
        private final Direction tupleDirection;

        MatchedSortOrder(@Nonnull final Direction tupleDirection) {
            this.tupleDirection = tupleDirection;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return tupleDirection.getArrowIndicator();
        }

        @Override
        public boolean isDirectional() {
            return true;
        }

        @Nonnull
        @Override
        public Direction getTupleDirection() {
            return tupleDirection;
        }

        @Nonnull
        public ProvidedSortOrder toProvidedSortOrder() {
            return toProvidedSortOrder(false);
        }

        @Nonnull
        public ProvidedSortOrder toProvidedSortOrder(final boolean isReverse) {
            if (isReverse) {
                return SortOrder.mapToReverseSortOrder(this, ProvidedSortOrder.getDirectionToSortOrderMap());
            }
            return SortOrder.mapToSortOrder(this, ProvidedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        public RequestedSortOrder toRequestedSortOrder() {
            return SortOrder.mapToSortOrder(this, RequestedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        static EnumMap<Direction, MatchedSortOrder> getDirectionToSortOrderMap() {
            return directionToSortOrderMap;
        }

        @Nonnull
        public static MatchedSortOrder fromDirection(@Nonnull final Direction direction) {
            return Objects.requireNonNull(directionToSortOrderMap.get(direction));
        }
    }

    /**
     * Sort order used to model requested orderings, that is orderings that a down stream operator or the client
     * requires the result set of the upstream operator to adhere to.
     */
    public enum RequestedSortOrder implements SortOrder {
        /**
         * Ascending.
         */
        ASCENDING(Direction.ASC_NULLS_FIRST),
        /**
         * Descending.
         */
        DESCENDING(Direction.DESC_NULLS_LAST),
        /**
         * Ascending but with nulls after regular values.
         */
        ASCENDING_NULLS_LAST(Direction.ASC_NULLS_LAST),
        /**
         * Descending but with nulls before regular values.
         */
        DESCENDING_NULLS_FIRST(Direction.DESC_NULLS_FIRST),
        /**
         * Any ordering. This requested ordering still needs an actual produced order that can either be ascending or
         * descending. It cannot be unordered.
         */
        ANY("↕");

        private static final EnumMap<Direction, RequestedSortOrder> directionToSortOrderMap;

        static {
            directionToSortOrderMap = SortOrder.computeDirectionToSortOrder(RequestedSortOrder.class);
        }

        @Nullable
        private final Direction tupleDirection;
        @Nonnull
        private final String arrowIndicator;

        RequestedSortOrder(@Nonnull final Direction tupleDirection) {
            this.tupleDirection = tupleDirection;
            this.arrowIndicator = tupleDirection.getArrowIndicator();
        }

        RequestedSortOrder(@Nonnull final String arrowIndicator) {
            this.tupleDirection = null;
            this.arrowIndicator = arrowIndicator;
        }

        @Nonnull
        @Override
        public String getArrowIndicator() {
            return arrowIndicator;
        }

        @Override
        public boolean isDirectional() {
            return this == ASCENDING || this == DESCENDING ||
                    this == ASCENDING_NULLS_LAST || this == DESCENDING_NULLS_FIRST;
        }

        @Nonnull
        @Override
        public Direction getTupleDirection() {
            Verify.verify(isDirectional());
            return Objects.requireNonNull(tupleDirection);
        }

        @Nonnull
        public static RequestedSortOrder fromIsReverse(final boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }

        @Nonnull
        public ProvidedSortOrder toProvidedSortOrder() {
            return SortOrder.mapToSortOrder(this, ProvidedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        public MatchedSortOrder toMatchedSortOrder() {
            return SortOrder.mapToSortOrder(this, MatchedSortOrder.getDirectionToSortOrderMap());
        }

        @Nonnull
        static EnumMap<Direction, RequestedSortOrder> getDirectionToSortOrderMap() {
            return directionToSortOrderMap;
        }

        @Nonnull
        public static RequestedSortOrder fromDirection(@Nonnull final Direction direction) {
            return Objects.requireNonNull(directionToSortOrderMap.get(direction));
        }
    }

    /**
     * Final class to tag provided ordering parts and to seal {@link OrderingPart}.
     */
    public static final class ProvidedOrderingPart extends OrderingPart<ProvidedSortOrder> {
        public ProvidedOrderingPart(@Nonnull final Value value, final ProvidedSortOrder sortOrder) {
            super(value, sortOrder);
        }

        /**
         * Compute the comparison key value for a set operation from this ordering part and a reversed-ness indicator.
         * <pre>
         * {@code
         * forward:                                           reverse:
         *                      a     tob(a, "↑")           |                      a     tob(a, "↓")
         *                   null         -inf              |                   null          inf
         *                     -2           -2              |                     -2            2
         *                      3            3              |                      3           -3
         *                      5            5              |                      5           -5
         *                     10           10              |                     10          -10
         *                ------------------------          |                ------------------------
         * resulting order    "↑"        compare asc        | resulting order    "↑"        compare desc
         *                                                  |
         *                      a     tob(a, "↓")           |                      a     tob(a, "↑")
         *                     10          -10              |                     10           10
         *                      5           -5              |                      5            5
         *                      3           -3              |                      3            3
         *                     -2            2              |                     -2            2
         *                   null          inf              |                   null         -inf
         *                ------------------------          |                ------------------------
         * resulting order    "↓"        compare asc        | resulting order    "↓"        compare desc
         *                                                  |
         *                      a     tob(a, "↙")           |                      a     tob(a, "↗")
         *                   null         -inf              |                   null          inf
         *                     10          -10              |                     10           10
         *                      5           -5              |                      5            5
         *                      3           -3              |                      3            3
         *                     -2            2              |                     -2           -2
         *                ------------------------          |                ------------------------
         * resulting order    "↙"        compare asc        | resulting order    "↙"         compare desc
         *                                                  |
         *                      a     tob(a, "↗")           |                      a     tob(a, "↙")
         *                     -2           -2              |                     -2            2
         *                      3            3              |                      3           -3
         *                      5            5              |                      5           -5
         *                     10           10              |                     10          -10
         *                   null          inf              |                   null         -inf
         *                ------------------------          |                --------------------------
         * resulting order    "↗"        compare asc        | resulting order    "↗"         compare desc
         * }
         * </pre>
         * @param isReverse indicator if the set operation is using a forward or a reverse comparison function
         * @return a {@link Value} that represents the <em>physical</em> comparison kay part for this ordering part
         */
        @Nonnull
        public Value comparisonKeyValue(final boolean isReverse) {
            final var tupleDirection = toTupleDirection(isReverse);
            final var value = getValue();
            if (tupleDirection == Direction.ASC_NULLS_FIRST) {
                return value;
            }
            return new ToOrderedBytesValue(value, tupleDirection);
        }

        @Nonnull
        private Direction toTupleDirection(final boolean isReverse) {
            return isReverse
                   ? getSortOrder().getTupleDirection().reverseDirection()
                   : getSortOrder().getTupleDirection();
        }

        @Nonnull
        public static List<Value> comparisonKeyValues(@Nonnull final Iterable<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                      final boolean isReverse) {
            return Streams.stream(comparisonKeyOrderingParts)
                    .map(providedOrderingPart -> providedOrderingPart.comparisonKeyValue(isReverse))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * Final class to tag requested ordering parts and to seal {@link OrderingPart}.
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
            if (!(o instanceof OrderingPart.MatchedOrderingPart)) {
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

    /**
     * Functional interface to be used to create instances of a particular kind of {@link OrderingPart}.
     * @param <O> type or specific sort order
     * @param <P> type of specific ordering part
     */
    @FunctionalInterface
    public interface OrderingPartCreator<O extends SortOrder, P extends OrderingPart<O>> {
        P create(@Nonnull Value value, @Nonnull O sortOrder);
    }
}
