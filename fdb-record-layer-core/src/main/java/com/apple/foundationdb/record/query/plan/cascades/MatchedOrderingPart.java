/*
 * MatchedOrderingPart.java
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

import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * An {@link OrderingPart} that is bound by a comparison during graph matching.
 */
public class MatchedOrderingPart {
    @Nonnull
    private final OrderingPart orderingPart;

    @Nonnull
    private final ComparisonRange.Type comparisonRangeType;

    /**
     * Constructor.
     * @param orderByValue value that defines what to order by
     * @param comparisonRangeType type of comparison
     */
    private MatchedOrderingPart(@Nonnull final Value orderByValue,
                                @Nonnull final ComparisonRange.Type comparisonRangeType,
                                final boolean isReverse) {

        this.orderingPart = OrderingPart.of(orderByValue, isReverse);
        this.comparisonRangeType = comparisonRangeType;
    }

    @Nonnull
    public OrderingPart getOrderingPart() {
        return orderingPart;
    }

    @Nonnull
    public Value getValue() {
        return orderingPart.getValue();
    }

    public boolean isReverse() {
        return orderingPart.isReverse();
    }

    @Nonnull
    public ComparisonRange.Type getComparisonRangeType() {
        return comparisonRangeType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MatchedOrderingPart)) {
            return false;
        }
        final MatchedOrderingPart that = (MatchedOrderingPart)o;
        return getOrderingPart().equals(that.getOrderingPart()) &&
               comparisonRangeType.equals(that.comparisonRangeType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderingPart(), comparisonRangeType);
    }

    @Nonnull
    public static MatchedOrderingPart of(@Nonnull final Value orderByValue,
                                         @Nonnull final ComparisonRange.Type comparisonRangeType,
                                         final boolean isReverse) {
        return new MatchedOrderingPart(orderByValue, comparisonRangeType, isReverse);
    }
}
