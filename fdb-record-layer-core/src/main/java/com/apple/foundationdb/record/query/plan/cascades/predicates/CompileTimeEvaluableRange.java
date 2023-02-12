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
import com.apple.foundationdb.record.metadata.IndexComparison;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
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
import java.util.stream.Collectors;

/**
 * Represents a compile-time range that can be evaluated against other compile-time ranges.
 */
public class CompileTimeEvaluableRange implements PlanHashable, Correlated<CompileTimeEvaluableRange> {

    /**
     * Evaluation result, ternary logic.
     */
    public enum EvalResult {
        TRUE,
        FALSE,
        UNKNOWN
    }


    @Nullable
    private final Range<Boundary> range; // null = entire range.

    @Nonnull
    private final Set<Comparisons.Comparison> nonCompileTimeComparisons;

    CompileTimeEvaluableRange(@Nullable final Range<Boundary> range,
                              @Nonnull final Set<Comparisons.Comparison> nonCompileTimeComparisons) {
        this.range = range;
        this.nonCompileTimeComparisons = nonCompileTimeComparisons;
    }

    CompileTimeEvaluableRange(@Nonnull final Range<Boundary> range) {
        this(range, Set.of());
    }

    public boolean isCompileTimeEvaluable() {
        return nonCompileTimeComparisons.isEmpty();
    }

    @Nonnull
    List<Comparisons.Comparison> getComparisons() {
        if (isEquality().equals(EvalResult.TRUE)) {
            return List.of(Objects.requireNonNull(range).upperEndpoint().comparison);
        }
        ImmutableList.Builder<Comparisons.Comparison> result = ImmutableList.builder();
        result.addAll(nonCompileTimeComparisons);
        if (range != null) {
            if (range.hasUpperBound()) {
                result.add(range.upperEndpoint().getComparison());
            }
            if (range.hasLowerBound()) {
                result.add(range.lowerEndpoint().getComparison());
            }
        }
        return result.build();
    }

    @Nonnull
    public Set<Comparisons.Comparison> getNonCompileTimeComparisons() {
        return nonCompileTimeComparisons;
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
        nonCompileTimeComparisons.forEach(c -> result.addAll(c.getCorrelatedTo()));
        if (range != null) {
            if (range.hasLowerBound()) {
                result.addAll(range.lowerEndpoint().getComparison().getCorrelatedTo());
            }
            if (range.hasUpperBound()) {
                result.addAll(range.upperEndpoint().getComparison().getCorrelatedTo());
            }
        }
        return result.build();
    }

    @Nonnull
    @Override
    public CompileTimeEvaluableRange rebase(@Nonnull final AliasMap translationMap) {
        if (isEmpty().equals(EvalResult.TRUE)) {
            return this;
        } else if (range == null) {
            if (nonCompileTimeComparisons.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = nonCompileTimeComparisons.stream().map(c -> c.rebase(translationMap)).collect(Collectors.toSet());
                return new CompileTimeEvaluableRange(null, newNonCompileTimeComparisons);
            }
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

            if (!hasNewUpperComparison && !hasNewLowerComparison && nonCompileTimeComparisons.isEmpty()) {
                return this;
            }

            final var resultBuilder = CompileTimeEvaluableRange.newBuilder();
            if (hasNewLowerComparison) {
                resultBuilder.addMaybe(lower);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addMaybe(upper);
            }
            for (final var nonCompilableComparison : nonCompileTimeComparisons) {
                resultBuilder.addMaybe(nonCompilableComparison);
            }
            return resultBuilder.build().orElseThrow();
        }
    }

    @Nonnull
    public CompileTimeEvaluableRange translateCorrelations(@Nonnull final TranslationMap translationMap) {
        if (isEmpty().equals(EvalResult.TRUE)) {
            return this;
        } else if (range == null) {
            if (nonCompileTimeComparisons.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = nonCompileTimeComparisons.stream().map(c -> c.translateCorrelations(translationMap)).collect(Collectors.toSet());
                return new CompileTimeEvaluableRange(null, newNonCompileTimeComparisons);
            }
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (range.hasLowerBound()) {
                final var origLower = range.lowerEndpoint().comparison;
                final var newLower = origLower.translateCorrelations(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (range.hasUpperBound()) {
                final var origUpper = range.upperEndpoint().comparison;
                final var newUpper = origUpper.translateCorrelations(translationMap);
                if (origUpper == newUpper) {
                    hasNewUpperComparison = true;
                    upper = newUpper;
                }
            }

            if (!hasNewUpperComparison && !hasNewLowerComparison && nonCompileTimeComparisons.isEmpty()) {
                return this;
            }

            final var resultBuilder = CompileTimeEvaluableRange.newBuilder();
            if (hasNewLowerComparison) {
                resultBuilder.addMaybe(lower);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addMaybe(upper);
            }
            for (final var nonCompilableComparison : nonCompileTimeComparisons) {
                resultBuilder.addMaybe(nonCompilableComparison);
            }
            return resultBuilder.build().orElseThrow();
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final var that = (CompileTimeEvaluableRange)other;

        if (nonCompileTimeComparisons.size() != that.nonCompileTimeComparisons.size()) {
            return false;
        }

        if (!nonCompileTimeComparisons.stream().allMatch(left -> that.nonCompileTimeComparisons.stream().anyMatch(right -> left.semanticEquals(right, aliasMap)))) {
            return false;
        }

        final var inverseAliasMap = aliasMap.inverse();

        if (!that.nonCompileTimeComparisons.stream().allMatch(left -> nonCompileTimeComparisons.stream().anyMatch(right -> left.semanticEquals(right, inverseAliasMap)))) {
            return false;
        }

        if (range == null && that.range == null) {
            return true;
        }

        if (range == null || that.range == null) {
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
        return Objects.hash(range, nonCompileTimeComparisons);
    }

    @Nonnull
    public EvalResult isEmpty() {
        if (nonCompileTimeComparisons.isEmpty()) {
            if (Objects.requireNonNull(range).isEmpty()) {
                return EvalResult.TRUE;
            } else {
                return EvalResult.FALSE;
            }
        }
        return EvalResult.UNKNOWN;
    }

    @Nonnull
    public EvalResult isEquality() {
        if (nonCompileTimeComparisons.isEmpty()) {
            if (Objects.requireNonNull(range).hasLowerBound() && range.hasUpperBound() && range.lowerEndpoint().equals(range.upperEndpoint())) {
                return EvalResult.TRUE;
            } else {
                return EvalResult.FALSE;
            }
        }
        return EvalResult.UNKNOWN;
    }

    @Nonnull
    public EvalResult implies(@Nonnull final CompileTimeEvaluableRange other) {
        if (!isCompileTimeEvaluable() || !other.isCompileTimeEvaluable()) {
            return EvalResult.UNKNOWN;
        } else if (range == null) { // full range implies everything
            return EvalResult.TRUE;
        } else if (other.range == null) { // other is full range, we are not -> false
            return EvalResult.FALSE;
        } else if (range.encloses(other.range)) {
            return EvalResult.TRUE;
        } else {
            return EvalResult.FALSE;
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, range);
    }

    @Override
    @Nonnull
    public String toString() {
        final var result = new StringBuilder();
        if (range == null) {
            result.append("(-∞..+∞)");
        } else {
            result.append(range);
        }
        if (!nonCompileTimeComparisons.isEmpty()) {
            result.append(nonCompileTimeComparisons.stream().map(Comparisons.Comparison::toString)
                    .collect(Collectors.joining(" && " )));
        }
        return result.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CompileTimeEvaluableRange that = (CompileTimeEvaluableRange)o;
        return Objects.equals(range, that.range) && nonCompileTimeComparisons.equals(that.nonCompileTimeComparisons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range, nonCompileTimeComparisons);
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
     * Builds an instance of {@link CompileTimeEvaluableRange}.
     */
    public static class Builder {

        @Nullable
        private Range<Boundary> range;

        @Nonnull
        private ImmutableSet.Builder<Comparisons.Comparison> nonCompilableComparisons;

        @Nonnull
        private static final Set<Comparisons.Type> allowedComparisonTypes = new LinkedHashSet<>();

        @Nonnull
        private static final CompileTimeEvaluableRange emptyRange = new CompileTimeEvaluableRange(Range.closedOpen(
                Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0)).build(),
                Boundary.newBuilder().setComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0)).build()));

        static {
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN);
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.IS_NULL);
            allowedComparisonTypes.add(Comparisons.Type.NOT_NULL);
        }

        @Nullable
        private static Range<Boundary> intersect(@Nullable final Range<Boundary> left, @Nullable final Range<Boundary> right) {
            if (left == null && right == null) {
                return null; // all range
            }
            if (left == null || (right != null && right.isEmpty())) {
                return right;
            }
            if (right == null || left.isEmpty()) {
                return left;
            }
            if (left.isConnected(right)) {
                return left.intersection(right);
            } else {
                return CompileTimeEvaluableRange.empty().range;
            }
        }

        @Nonnull
        private static Set<Comparisons.Comparison> intersect(@Nonnull final Set<Comparisons.Comparison> left,
                                                             @Nonnull final Set<Comparisons.Comparison> right) {
            ImmutableSet.Builder<Comparisons.Comparison> newNonCompileTimeComparisons = ImmutableSet.builder();
            newNonCompileTimeComparisons.addAll(left);
            newNonCompileTimeComparisons.addAll(right);
            return newNonCompileTimeComparisons.build();
        }

        private Builder() {
            this.range = null;
            this.nonCompilableComparisons = ImmutableSet.builder();
        }

        public boolean isCompileTime(@Nonnull final Comparisons.Comparison comparison) {
            return IndexComparison.isSupported(comparison) && allowedComparisonTypes.contains(comparison.getType());
        }

        private boolean canBeUsedInScanPrefix(@Nonnull final Comparisons.Comparison comparison) {
            switch (comparison.getType()) {
                case EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUALS:
                case STARTS_WITH:
                case NOT_NULL:
                case IS_NULL:
                    return true;
                case TEXT_CONTAINS_ALL:
                case TEXT_CONTAINS_ALL_WITHIN:
                case TEXT_CONTAINS_ANY:
                case TEXT_CONTAINS_PHRASE:
                case TEXT_CONTAINS_PREFIX:
                case TEXT_CONTAINS_ALL_PREFIXES:
                case TEXT_CONTAINS_ANY_PREFIX:
                case IN:
                case NOT_EQUALS:
                case SORT:
                    return false;
                default:
                    throw new RecordCoreException(String.format("unexpected comparison type '%s'", comparison.getType()));
            }
        }

        public boolean addMaybe(@Nonnull final Comparisons.Comparison comparison) {
            if (!canBeUsedInScanPrefix(comparison)) {
                return false;
            }
            if (!isCompileTime(comparison)) {
                nonCompilableComparisons.add(comparison);
            } else {
                addMaybe(toRange(comparison));
            }
            return true;
        }

        private boolean addMaybe(@Nonnull final Range<Boundary> range) {
            if (this.range == null) {
                this.range = range;
            } else {
                if (this.range.isConnected(range)) {
                    this.range = this.range.intersection(range);
                } else {
                    this.range = CompileTimeEvaluableRange.empty().range;
                }
            }
            return true;
        }

        public void add(@Nonnull final CompileTimeEvaluableRange compileTimeEvaluableRange) {
            this.range = intersect(range, compileTimeEvaluableRange.range);
            this.nonCompilableComparisons = ImmutableSet.<Comparisons.Comparison>builder().addAll(intersect(nonCompilableComparisons.build(), compileTimeEvaluableRange.nonCompileTimeComparisons));
        }

        @Nonnull
        public Range<Boundary> toRange(@Nonnull Comparisons.Comparison comparison) {
            final var boundary = Boundary.newBuilder().setComparison(comparison).build();
            switch (comparison.getType()) {
                case GREATER_THAN: // fallthrough
                case NOT_NULL:
                    return Range.greaterThan(boundary);
                case GREATER_THAN_OR_EQUALS:
                    return Range.atLeast(boundary);
                case LESS_THAN:
                    return Range.lessThan(boundary);
                case LESS_THAN_OR_EQUALS:
                    return Range.atMost(boundary);
                case EQUALS: // fallthrough
                case IS_NULL:
                    return Range.singleton(boundary);
                default:
                    throw new RecordCoreException(String.format("can not transform '%s' to range", comparison));
            }
        }

        @Nonnull
        public Optional<CompileTimeEvaluableRange> build() {
            final var nonCompilableComparisonsList = nonCompilableComparisons.build();
            if (range == null && nonCompilableComparisonsList.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new CompileTimeEvaluableRange(range, nonCompilableComparisonsList));
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public static CompileTimeEvaluableRange empty() {
        return Builder.emptyRange;
    }
}
