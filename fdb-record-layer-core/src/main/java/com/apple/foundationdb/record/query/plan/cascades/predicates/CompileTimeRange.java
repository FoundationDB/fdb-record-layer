/*
 * CompileTimeRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Range;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a compile-time range that can be evaluated against other compile-time ranges.
 */
public class CompileTimeRange {

    @Nonnull
    private final Range<Boundary> range;

    CompileTimeRange(@Nonnull final Range<Boundary> range) {
        this.range = range;
    }

    @Nonnull
    public ComparisonRange toComparisonRange() {
        ComparisonRange result = ComparisonRange.EMPTY;
        if (range.isEmpty()) {
            return result;
        }
        if (range.hasUpperBound()) {
            result = result.tryToAdd(range.upperEndpoint().getComparison()).orElseThrow();
        }
        if (range.hasLowerBound()) {
            result = result.tryToAdd(range.lowerEndpoint().getComparison()).orElseThrow();
        }
        return result;
    }

    public boolean isEmpty() {
        return range.isEmpty();
    }

    static class Boundary implements Comparable<Boundary> {

        @Nonnull
        private final Tuple tuple;

        @Nonnull
        private final Comparisons.Comparison comparison;

        Boundary(@Nonnull final Tuple tuple, @Nonnull final Comparisons.Comparison comparison) {
            this.tuple = tuple;
            this.comparison = comparison;
        }

        @Override
        public int compareTo(final Boundary other) {
            return tuple.compareTo(other.tuple);
        }

        // mainly for backward compatibility.

        @Nonnull
        public Comparisons.Comparison getComparison() {
            return comparison;
        }

        public static class Builder {
            private Tuple tuple = null;
            private Comparisons.Comparison comparison;

            @Nonnull
            public Boundary.Builder setTuple(@Nonnull final Tuple tuple) {
                this.tuple = tuple;
                return this;
            }

            @Nonnull
            public Boundary.Builder setComparison(@Nonnull final Comparisons.Comparison comparison) {
                this.comparison = comparison;
                return this;
            }

            @Nonnull
            public Boundary build() {
                Verify.verifyNotNull(comparison, String.format("can not create '%s' without a '%s'", Boundary.class, Comparisons.Comparison.class));

                if (tuple == null) {
                    tuple = toTuple(comparison);
                }

                return new Boundary(tuple, comparison);
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final Builder builder = (Builder)o;
                return tuple.equals(builder.tuple);
            }

            @Override
            public int hashCode() {
                return Objects.hash(tuple);
            }
        }

        @Nonnull
        private static Tuple toTuple(@Nonnull final Comparisons.Comparison comparison) {
            final List<Object> items = new ArrayList<>();
            if (comparison.hasMultiColumnComparand()) {
                items.addAll(((Tuple)comparison.getComparand(null, null)).getItems());
            } else {
                items.add(ScanComparisons.toTupleItem(comparison.getComparand(null, null)));
            }
            final Tuple result = Tuple.fromList(items);
            return result;
        }

        @Nonnull
        public static Boundary.Builder newBuilder() {
            return new Boundary.Builder();
        }
    }

    /**
     * Builds an instance of {@link CompileTimeRange}.
     */
    public static class Builder {

        Range<Boundary> range;

        private Builder() {
            this.range = null;
        }

        public boolean canAdd(@Nonnull final Comparisons.Comparison comparison) {
            return allowedComparisonTypes.contains(comparison.getType());
        }

        public boolean tryAdd(@Nonnull final Comparisons.Comparison comparison) {
            if (!canAdd(comparison)) {
                return false;
            }
            final var range = toRange(comparison);
            if (this.range == null) {
                this.range = range;
            } else {
                if (this.range.isConnected(range)) {
                    this.range = this.range.intersection(range);
                } else {
                    this.range = CompileTimeRange.empty().range;
                }
            }
            return true;
        }

        @Nonnull
        public Range<Boundary> toRange(@Nonnull Comparisons.Comparison comparison) {
            final var boundary = Boundary.newBuilder().setComparison(comparison).build();
            switch (comparison.getType()) {
                case GREATER_THAN:
                    return Range.greaterThan(boundary);
                case GREATER_THAN_OR_EQUALS:
                    return Range.atLeast(boundary);
                case LESS_THAN:
                    return Range.lessThan(boundary);
                case LESS_THAN_OR_EQUALS:
                    return Range.atMost(boundary);
                case EQUALS:
                    return Range.singleton(boundary);
                default:
                    throw new RecordCoreException(String.format("can not transform '%s' to compile-time range", comparison));
            }
        }

        @Nonnull
        public CompileTimeRange build() {
            Verify.verifyNotNull(range);
            return new CompileTimeRange(range);
        }

        private static Set<Comparisons.Type> allowedComparisonTypes = new LinkedHashSet<>();

        static {
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN);
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.EQUALS);
        }

    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    private static CompileTimeRange emptyRange = new CompileTimeRange(Range.closedOpen(
            Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0)).build(),
            Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0)).build()));

    @Nonnull
    public static CompileTimeRange empty() {
        return emptyRange;
    }

    public boolean implies(@Nonnull final CompileTimeRange other) {
        return range.encloses(other.range);
    }
}
