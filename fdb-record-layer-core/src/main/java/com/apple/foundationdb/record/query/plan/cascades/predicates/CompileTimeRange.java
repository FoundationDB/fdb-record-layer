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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a compile-time range that can be evaluated against other compile-time ranges.
 */
public class CompileTimeRange implements PlanHashable, Correlated<CompileTimeRange> {

    @Nonnull
    private final Range<Boundary> range;

    CompileTimeRange(@Nonnull final Range<Boundary> range) {
        this.range = range;
    }

    @Nonnull
    List<Comparisons.Comparison> getComparisons() {
        if (isEquality()) {
            return List.of(range.upperEndpoint().comparison);
        }
        ImmutableList.Builder<Comparisons.Comparison> result = ImmutableList.builder();
        if (range.hasUpperBound()) {
            result.add(range.upperEndpoint().getComparison());
        }
        if (range.hasLowerBound()) {
            result.add(range.lowerEndpoint().getComparison());
        }
        return result.build();
    }

    /**
     * Returns an equivalent {@link ComparisonRange}.
     * Note: This method is created for compatibility reasons.
     * @return An equivalent {@link ComparisonRange}.
     */
    @Nonnull
    public ComparisonRange asComparisonRange() {
        var resultRange = ComparisonRange.EMPTY;
        for (final var comparison : getComparisons()) {
            resultRange = resultRange.merge(comparison).getComparisonRange();
        }
        return resultRange;
    }

    // todo: memoize
    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> result = ImmutableSet.builder();
        if (range.hasLowerBound()) {
            result.addAll(range.lowerEndpoint().getComparison().getCorrelatedTo());
        }
        if (range.hasUpperBound()) {
            result.addAll(range.upperEndpoint().getComparison().getCorrelatedTo());
        }
        return result.build();
    }

    @Nonnull
    @Override
    public CompileTimeRange rebase(@Nonnull final AliasMap translationMap) {
        if (isEmpty()) {
            return this;
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (range.hasLowerBound()) {
                final var origLower = range.lowerEndpoint().comparison;
                final var newLower = origLower.rebase(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (range.hasUpperBound()) {
                final var origUpper = range.upperEndpoint().comparison;
                final var newUpper = origUpper.rebase(translationMap);
                if (origUpper == newUpper) {
                    hasNewUpperComparison = true;
                    upper = newUpper;
                }
            }

            if (!hasNewUpperComparison && !hasNewLowerComparison) {
                return this;
            }

            final var resultBuilder = CompileTimeRange.newBuilder();
            if (hasNewLowerComparison) {
                resultBuilder.tryAdd(lower);
            }
            if (hasNewUpperComparison) {
                resultBuilder.tryAdd(upper);
            }
            return resultBuilder.build().orElseThrow();
        }
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final var that = (CompileTimeRange)other;

        if (this.isEmpty() && that.isEmpty()) {
            return true;
        }

        if (this.isEmpty() || that.isEmpty()) {
            return false;
        }

        if (range.hasUpperBound()) {
            if (!that.range.hasUpperBound()) {
                return false;
            }
            if (!this.range.upperEndpoint().getComparison().semanticEquals(that.range.upperEndpoint().getComparison(), aliasMap)) {
                return false;
            }
        }

        if (range.hasLowerBound()) {
            if (!that.range.hasLowerBound()) {
                return false;
            }
            return this.range.lowerEndpoint().getComparison().semanticEquals(that.range.lowerEndpoint().getComparison(), aliasMap);
        }

        return true;
    }

    @Override
    public int semanticHashCode() {
        return range.hashCode();
    }

    public boolean isEmpty() {
        return range.isEmpty();
    }

    public boolean isEquality() {
        return range.hasLowerBound() && range.hasUpperBound() && range.lowerEndpoint().equals(range.upperEndpoint());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, range);
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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Boundary boundary = (Boundary)o;
            return tuple.equals(boundary.tuple);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tuple);
        }

        // mainly for backward compatibility.

        @Nonnull
        public Comparisons.Comparison getComparison() {
            return comparison;
        }

        @Override
        public String toString() {
            return comparison.getComparand() == null ? "<NULL>" : comparison.getComparand().toString();
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
            return Tuple.fromList(items);
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

        @Nullable
        private Range<Boundary> range;

        @Nonnull
        private static final Set<Comparisons.Type> allowedComparisonTypes = new LinkedHashSet<>();

        @Nonnull
        private static final CompileTimeRange emptyRange = new CompileTimeRange(Range.closedOpen(
                Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0)).build(),
                Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0)).build()));

        static {
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN);
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.EQUALS);
        }

        private Builder() {
            this.range = null;
        }

        public boolean canAdd(@Nonnull final Comparisons.Comparison comparison) {
            return comparison instanceof Comparisons.SimpleComparison && allowedComparisonTypes.contains(comparison.getType());
        }

        public boolean tryAdd(@Nonnull final Comparisons.Comparison comparison) {
            if (!canAdd(comparison)) {
                return false;
            }
            return tryAdd(toRange(comparison));
        }

        public boolean tryAdd(@Nonnull final CompileTimeRange compileTimeRange) {
            return tryAdd(compileTimeRange.range);
        }

        public boolean tryAdd(@Nonnull final Range<Boundary> range) {
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
        public Optional<CompileTimeRange> build() {
            if (range == null) {
                return Optional.empty();
            }
            return Optional.of(new CompileTimeRange(range));
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }


    @Nonnull
    public static CompileTimeRange empty() {
        return Builder.emptyRange;
    }

    public boolean implies(@Nonnull final CompileTimeRange other) {
        return range.encloses(other.range);
    }
}
