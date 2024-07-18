/*
 * RequestedOrdering.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueSimplificationRuleSet;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class captures a requested ordering. Instances of this class are used to communicate ordering properties
 * towards the sources during planning.
 * <br>
 * There are two flows of information at work during planning:
 * <ul>
 *     <li>
 *         a {@link RequestedOrdering} is used to convey to an upstream expression (closer to source) that a particular
 *         order is useful or even required in order for the operation downstream to properly plan
 *     </li>
 *     <li>
 *         an {@link Ordering} which e.g. is attached to plans (or can be computed of plans) that declares the ordering
 *         of that plan
 *     </li>
 * </ul>
 *
 * An {@link Ordering} satisfies a {@link RequestedOrdering} iff it adheres to the constraints given in the request.
 */
public class RequestedOrdering {
    /**
     * A list of {@link KeyExpression}s where none of the contained expressions is equality-bound. This list
     * defines the actual order of records.
     */
    @Nonnull
    private final List<RequestedOrderingPart> orderingParts;

    private final Distinctness distinctness;

    @Nonnull
    private final Supplier<Map<Value, RequestedSortOrder>> valueRequestedSortOrderMapSupplier;

    public RequestedOrdering(@Nonnull final List<RequestedOrderingPart> orderingParts, final Distinctness distinctness) {
        this.orderingParts = ImmutableList.copyOf(orderingParts);
        this.distinctness = distinctness;
        this.valueRequestedSortOrderMapSupplier = Suppliers.memoize(this::computeValueSortOrderMap);
    }

    public Distinctness getDistinctness() {
        return distinctness;
    }

    public boolean isDistinct() {
        return distinctness == Distinctness.DISTINCT;
    }

    /**
     * When expressing a requirement (see also {@link RequestedOrderingConstraint}), the requirement may be to preserve
     * the order of records that are being encountered. This is represented by a special value here.
     * @return {@code true} if the ordering needs to be preserved
     */
    public boolean isPreserve() {
        return orderingParts.isEmpty();
    }

    @Nonnull
    public List<RequestedOrderingPart> getOrderingParts() {
        return orderingParts;
    }

    /**
     * Returns a map from {@link Value} to {@link RequestedSortOrder}. It is meant for quick lookups of the
     * sort order of a given value.
     * @return a map from {@link Value} to {@link RequestedSortOrder}. It is lazily computed and memoized. Subsequent
     *         calls return instantaneously.
     */
    @Nonnull
    public Map<Value, RequestedSortOrder> getValueRequestedSortOrderMap() {
        return valueRequestedSortOrderMapSupplier.get();
    }

    public int size() {
        return orderingParts.size();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RequestedOrdering)) {
            return false;
        }
        final RequestedOrdering ordering = (RequestedOrdering)o;
        return getOrderingParts().equals(ordering.getOrderingParts()) &&
               getDistinctness() == ordering.getDistinctness();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderingParts(), getDistinctness().name());
    }

    @Override
    public String toString() {
        return orderingParts.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    /**
     * Method to push this requested ordering through the value that is passed in. The method delegates the actual
     * process of pushing the constituent parts of this requested ordering to {@link Value}
     * (which are also {@link Value}s). The rule set for simplifications of this requested ordering that is used is
     * created by specific ordering simplification rule sets.
     * <br>
     * Examples:
     * <br>
     * This example highlights that references get translated by the push down logic:
     * <pre>
     *     {@code
     *     this: requested ordering [_.a, _.b.i]
     *     value: (x as a, (y as i, z as j) as b, w + v as c)
     *     result: requested ordering [_.x, _y]
     *     }
     * </pre>
     * <br>
     * This example highlights that certain arithmetic operations (involving constants in a limited fashion) can
     * be ignored and therefore can be removed by the push down logic:
     * <pre>
     *     {@code
     *     this: requested ordering [_.a, _.b]
     *     value: ((x + 3) as a, (2 * y) as b)
     *     result: requested ordering [_.x, _.y]
     *     }
     * </pre>
     * @param value the value this requested value should be pushed down through
     * @param lowerBaseAlias the alias that the new values should be referring to
     * @param aliasMap an {@link AliasMap} of equalities
     * @param constantAliases a set of aliases that can be considered constant for the purpose of this push down
     * @return a new requested ordering whose constituent values are expressed in terms of quantifiers prior to the
     *         computation of the {@link Value} passed in.
     */
    @Nonnull
    public RequestedOrdering pushDown(@Nonnull Value value,
                                      @Nonnull CorrelationIdentifier lowerBaseAlias,
                                      @Nonnull AliasMap aliasMap,
                                      @Nonnull Set<CorrelationIdentifier> constantAliases) {
        //
        // Need to push every participating value of this requested ordering through the value.
        //
        final var orderingKeyValues =
                orderingParts
                        .stream()
                        .map(OrderingPart::getValue)
                        .collect(ImmutableList.toImmutableList());

        final var pushedDownOrderingValues =
                value.pushDown(orderingKeyValues, OrderingValueSimplificationRuleSet.ofOrderingSimplificationRules(), aliasMap, constantAliases, Quantifier.current());

        final var translationMap = AliasMap.ofAliases(lowerBaseAlias, Quantifier.current());

        final var pushedDownOrderingPartsBuilder = ImmutableList.<RequestedOrderingPart>builder();
        for (int i = 0; i < orderingParts.size(); i++) {
            final var orderingPart = orderingParts.get(i);
            final var orderingValue = Objects.requireNonNull(pushedDownOrderingValues.get(i));
            final var rebasedOrderingValue = orderingValue.rebase(translationMap);
            pushedDownOrderingPartsBuilder.add(new RequestedOrderingPart(rebasedOrderingValue, orderingPart.getSortOrder()));
        }
        return new RequestedOrdering(pushedDownOrderingPartsBuilder.build(), Distinctness.PRESERVE_DISTINCTNESS);
    }

    @Nonnull
    private Map<Value, RequestedSortOrder> computeValueSortOrderMap() {
        return getOrderingParts()
                .stream()
                .collect(Collectors.toMap(OrderingPart::getValue, OrderingPart::getSortOrder,
                        (left, right) -> {
                            if (left == right) {
                                return left;
                            }
                            return RequestedSortOrder.ANY;
                        }, LinkedHashMap::new));
    }

    /**
     * Method to create an ordering instance that preserves the order of records.
     * @return a new ordering that preserves the order of records
     */
    @Nonnull
    public static RequestedOrdering preserve() {
        return new RequestedOrdering(ImmutableList.of(), Distinctness.PRESERVE_DISTINCTNESS);
    }

    @Nonnull
    public RequestedOrdering withDistinctness(@Nonnull final Distinctness distinctness) {
        if (this.distinctness == distinctness) {
            return this;
        }
        return new RequestedOrdering(orderingParts, distinctness);
    }

    /**
     * Whether the ordered records are distinct.
     */
    public enum Distinctness {
        DISTINCT,
        NOT_DISTINCT,
        PRESERVE_DISTINCTNESS
    }
}
