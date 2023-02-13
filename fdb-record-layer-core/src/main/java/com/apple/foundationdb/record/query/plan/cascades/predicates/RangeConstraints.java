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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
 * Represents a compile-time range that can be evaluated against other compile-time ranges with an optional list
 * of deferred ranges that can not be evaluated at compile-time but can still be used as scan index prefix.
 */
public class RangeConstraints implements PlanHashable, Correlated<RangeConstraints> {

    /**
     * Evaluation result, ternary logic.
     */
    public enum EvalResult {
        TRUE,
        FALSE,
        UNKNOWN
    }

    @Nonnull
    private final Supplier<List<Comparisons.Comparison>> comparisonsCalculator;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsCalculator;

    @Nullable
    private final Range<Boundary> evaluableRanges; // null = entire range (if no deferred ranges are defined).

    @Nonnull
    private final Set<Comparisons.Comparison> deferredRanges;

    /**
     * Creates a new instance of the {@link RangeConstraints}.
     *
     * @param evaluableRanges the compile-time evaluable range.
     * @param deferredRanges a list of none compile-time evaluable ranges but can still be used in a scan prefix.
     */
    private RangeConstraints(@Nullable final Range<Boundary> evaluableRanges,
                     @Nonnull final Set<Comparisons.Comparison> deferredRanges) {
        this.evaluableRanges = evaluableRanges;
        this.deferredRanges = deferredRanges;
        this.comparisonsCalculator = Suppliers.memoize(this::computeComparisons);
        this.correlationsCalculator = Suppliers.memoize(this::computeCorrelations);
    }

    /**
     * Creates a new instance of {@link RangeConstraints}.
     *
     * @param evaluableRanges the compile-time evaluable range.
     */
    private RangeConstraints(@Nonnull final Range<Boundary> evaluableRanges) {
        this(evaluableRanges, Set.of());
    }

    /**
     * Computes a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     *
     * @return a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     */
    @Nonnull
    private List<Comparisons.Comparison> computeComparisons() {
        if (isEquality().equals(EvalResult.TRUE)) {
            return List.of(Objects.requireNonNull(evaluableRanges).upperEndpoint().comparison);
        }
        ImmutableList.Builder<Comparisons.Comparison> result = ImmutableList.builder();
        result.addAll(deferredRanges);
        if (evaluableRanges != null) {
            if (evaluableRanges.hasUpperBound()) {
                result.add(evaluableRanges.upperEndpoint().getComparison());
            }
            if (evaluableRanges.hasLowerBound()) {
                result.add(evaluableRanges.lowerEndpoint().getComparison());
            }
        }
        return result.build();
    }

    /**
     * Computes a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     *
     * @return a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     */
    @Nonnull
    public List<Comparisons.Comparison> getComparisons() {
        return comparisonsCalculator.get();
    }

    /**
     * Returns deferred ranges, i.e. ranges that can not be evaluated at compile-time.
     *
     * @return a set of deferred ranges.
     */
    @Nonnull
    public Set<Comparisons.Comparison> getDeferredRanges() {
        return deferredRanges;
    }

    /**
     * Returns an equivalent {@link ComparisonRange}.
     * Note: This method is created for compatibility reasons.
     *
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

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelations() {
        final ImmutableSet.Builder<CorrelationIdentifier> result = ImmutableSet.builder();
        deferredRanges.forEach(c -> result.addAll(c.getCorrelatedTo()));
        if (evaluableRanges != null) {
            if (evaluableRanges.hasLowerBound()) {
                result.addAll(evaluableRanges.lowerEndpoint().getComparison().getCorrelatedTo());
            }
            if (evaluableRanges.hasUpperBound()) {
                result.addAll(evaluableRanges.upperEndpoint().getComparison().getCorrelatedTo());
            }
        }
        return result.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return correlationsCalculator.get();
    }


    /**
     * Checks whether the range can be determined at compile-time or not.
     *
     * @return {@code true} if the range can be determined at compile-time, otherwise {@code false}.
     */
    public boolean isCompileTimeEvaluable() {
        return deferredRanges.isEmpty();
    }

    /**
     * checks whether the range is known to be empty at compile-time or not.

     * @return if the range is compile-time it returns {@code true} if it is empty or {@code false} if it is not,
     * otherwise {@code unknown}.
     */
    @Nonnull
    public EvalResult isEmpty() {
        if (deferredRanges.isEmpty()) {
            if (Objects.requireNonNull(evaluableRanges).isEmpty()) {
                return EvalResult.TRUE;
            } else {
                return EvalResult.FALSE;
            }
        }
        return EvalResult.UNKNOWN;
    }

    /**
     * checks whether the range is known to contain a single value at compile-time or not.

     * @return if the range is known to contain a single value at compile-time it returns {@code true} or {@code false} if it is not,
     * otherwise {@code unknown}.
     */
    @Nonnull
    public EvalResult isEquality() {
        if (deferredRanges.isEmpty()) {
            if (Objects.requireNonNull(evaluableRanges).hasLowerBound() && evaluableRanges.hasUpperBound() &&
                    evaluableRanges.lowerEndpoint().equals(evaluableRanges.upperEndpoint())) {
                return EvalResult.TRUE;
            } else {
                return EvalResult.FALSE;
            }
        }
        return EvalResult.UNKNOWN;
    }

    /**
     * checks whether {@code this} range implies another {@link RangeConstraints}. Both ranges <b>must</b> be compile-time.
     * <br>
     * <b>Examples</b>:
     * <ul>
     * <li>{@code this = [1, 10), other = (2, 5) => this.implies(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = (0, 5) => this.implies(other) = FALSE}</li>
     * <li>{@code this = [1, 10), other = [1, 10) => this.implies(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = (1, 10) => this.implies(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = () => this.implies(other) = FALSE}</li>
     * <li>{@code this = {[1, 10), deferred = STARTS_WITH}, other = (2, 5) => this.implies(other) = UNKNOWN}</li>
     * </ul>
     *
     * @param other The other range to compare against.
     * @return {@code true} if the range is known, at compil-time, to imply {@code other} or {@code false} if not. Otherwise
     * it returns {@code unknown}.
     */
    @Nonnull
    public EvalResult implies(@Nonnull final RangeConstraints other) {
        if (!isCompileTimeEvaluable() || !other.isCompileTimeEvaluable()) {
            return EvalResult.UNKNOWN;
        } else if (evaluableRanges == null) { // full range implies everything
            return EvalResult.TRUE;
        } else if (other.evaluableRanges == null) { // other is full range, we are not -> false
            return EvalResult.FALSE;
        } else if (evaluableRanges.encloses(other.evaluableRanges)) {
            return EvalResult.TRUE;
        } else {
            return EvalResult.FALSE;
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, evaluableRanges);
    }

    @Override
    @Nonnull
    public String toString() {
        final var result = new StringBuilder();
        if (evaluableRanges == null) {
            result.append("(-∞..+∞)");
        } else {
            result.append(evaluableRanges);
        }
        if (!deferredRanges.isEmpty()) {
            result.append(deferredRanges.stream().map(Comparisons.Comparison::toString)
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
        final RangeConstraints that = (RangeConstraints)o;
        return Objects.equals(evaluableRanges, that.evaluableRanges) && deferredRanges.equals(that.deferredRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evaluableRanges, deferredRanges);
    }

    @Nonnull
    @Override
    public RangeConstraints rebase(@Nonnull final AliasMap translationMap) {
        if (isEmpty().equals(EvalResult.TRUE)) {
            return this;
        } else if (evaluableRanges == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.rebase(translationMap)).collect(Collectors.toSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (evaluableRanges.hasLowerBound()) {
                final var origLower = evaluableRanges.lowerEndpoint().comparison;
                final var newLower = origLower.rebase(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (evaluableRanges.hasUpperBound()) {
                final var origUpper = evaluableRanges.upperEndpoint().comparison;
                final var newUpper = origUpper.rebase(translationMap);
                if (origUpper == newUpper) {
                    hasNewUpperComparison = true;
                    upper = newUpper;
                }
            }

            if (!hasNewUpperComparison && !hasNewLowerComparison && deferredRanges.isEmpty()) {
                return this;
            }

            final var resultBuilder = RangeConstraints.newBuilder();
            if (hasNewLowerComparison) {
                resultBuilder.addComparisonMaybe(lower);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addComparisonMaybe(upper);
            }
            for (final var nonCompilableComparison : deferredRanges) {
                resultBuilder.addComparisonMaybe(nonCompilableComparison);
            }
            return resultBuilder.build().orElseThrow();
        }
    }

    @Nonnull
    public RangeConstraints translateCorrelations(@Nonnull final TranslationMap translationMap) {
        if (isEmpty().equals(EvalResult.TRUE)) {
            return this;
        } else if (evaluableRanges == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.translateCorrelations(translationMap)).collect(Collectors.toSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (evaluableRanges.hasLowerBound()) {
                final var origLower = evaluableRanges.lowerEndpoint().comparison;
                final var newLower = origLower.translateCorrelations(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (evaluableRanges.hasUpperBound()) {
                final var origUpper = evaluableRanges.upperEndpoint().comparison;
                final var newUpper = origUpper.translateCorrelations(translationMap);
                if (origUpper == newUpper) {
                    hasNewUpperComparison = true;
                    upper = newUpper;
                }
            }

            if (!hasNewUpperComparison && !hasNewLowerComparison && deferredRanges.isEmpty()) {
                return this;
            }

            final var resultBuilder = RangeConstraints.newBuilder();
            if (hasNewLowerComparison) {
                resultBuilder.addComparisonMaybe(lower);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addComparisonMaybe(upper);
            }
            for (final var nonCompilableComparison : deferredRanges) {
                resultBuilder.addComparisonMaybe(nonCompilableComparison);
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

        final var that = (RangeConstraints)other;

        if (deferredRanges.size() != that.deferredRanges.size()) {
            return false;
        }

        if (!deferredRanges.stream().allMatch(left -> that.deferredRanges.stream().anyMatch(right -> left.semanticEquals(right, aliasMap)))) {
            return false;
        }

        final var inverseAliasMap = aliasMap.inverse();

        if (!that.deferredRanges.stream().allMatch(left -> deferredRanges.stream().anyMatch(right -> left.semanticEquals(right, inverseAliasMap)))) {
            return false;
        }

        if (evaluableRanges == null && that.evaluableRanges == null) {
            return true;
        }

        if (evaluableRanges == null || that.evaluableRanges == null) {
            return false;
        }

        if (evaluableRanges.hasUpperBound()) {
            if (!that.evaluableRanges.hasUpperBound()) {
                return false;
            }
            if (!this.evaluableRanges.upperEndpoint().getComparison().semanticEquals(that.evaluableRanges.upperEndpoint().getComparison(), aliasMap)) {
                return false;
            }
        }

        if (evaluableRanges.hasLowerBound()) {
            if (!that.evaluableRanges.hasLowerBound()) {
                return false;
            }
            return this.evaluableRanges.lowerEndpoint().getComparison().semanticEquals(that.evaluableRanges.lowerEndpoint().getComparison(), aliasMap);
        }

        return true;
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(evaluableRanges, deferredRanges);
    }

    /**
     * Represents a range boundary.
     */
    private static class Boundary implements Comparable<Boundary> {

        @Nonnull
        private final Tuple tuple;

        @Nonnull
        private final Comparisons.Comparison comparison;

        private Boundary(@Nonnull final Tuple tuple, @Nonnull final Comparisons.Comparison comparison) {
            this.tuple = tuple;
            this.comparison = comparison;
        }

        @Nonnull
        private Comparisons.Comparison getComparison() {
            return comparison;
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

        @Override
        public String toString() {
            return comparison.getComparand() == null ? "<NULL>" : comparison.getComparand().toString();
        }

        @Nonnull
        private static Boundary from(@Nonnull final Comparisons.Comparison comparison) {
            return from(toTuple(comparison), comparison);
        }

        @Nonnull
        private static Boundary from(@Nonnull final Tuple tuple, @Nonnull final Comparisons.Comparison comparison) {
            return new Boundary(tuple, comparison);
        }

        /**
         * Gets the comparand of a {@link Comparisons.Comparison}.
         *
         * @param comparison The comparison to convert.
         * @return the comparison's comparand.
         */
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
    }

    /**
     * Builds an instance of {@link RangeConstraints}.
     */
    public static class Builder {

        @Nullable
        private Range<Boundary> range;

        @Nonnull
        private ImmutableSet.Builder<Comparisons.Comparison> nonCompilableComparisons;

        @Nonnull
        private static final Set<Comparisons.Type> allowedComparisonTypes = new LinkedHashSet<>();

        @Nonnull
        private static final RangeConstraints emptyRange = new RangeConstraints(Range.closedOpen(
                Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0)),
                Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0))));

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
                return RangeConstraints.emptyRange().evaluableRanges;
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

        private boolean isCompileTime(@Nonnull final Comparisons.Comparison comparison) {
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

        public boolean addComparisonMaybe(@Nonnull final Comparisons.Comparison comparison) {
            if (!canBeUsedInScanPrefix(comparison)) {
                return false;
            }
            if (!isCompileTime(comparison)) {
                nonCompilableComparisons.add(comparison);
            } else {
                addRange(toRange(comparison));
            }
            return true;
        }

        private void addRange(@Nonnull final Range<Boundary> range) {
            if (this.range == null) {
                this.range = range;
            } else {
                if (this.range.isConnected(range)) {
                    this.range = this.range.intersection(range);
                } else {
                    this.range = RangeConstraints.emptyRange().evaluableRanges;
                }
            }
        }

        public void add(@Nonnull final RangeConstraints rangeConstraints) {
            this.range = intersect(range, rangeConstraints.evaluableRanges);
            this.nonCompilableComparisons = ImmutableSet.<Comparisons.Comparison>builder().addAll(intersect(nonCompilableComparisons.build(), rangeConstraints.deferredRanges));
        }

        @Nonnull
        public Range<Boundary> toRange(@Nonnull Comparisons.Comparison comparison) {
            final var boundary = Boundary.from(comparison);
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
        public Optional<RangeConstraints> build() {
            final var nonCompilableComparisonsList = nonCompilableComparisons.build();
            if (range == null && nonCompilableComparisonsList.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new RangeConstraints(range, nonCompilableComparisonsList));
        }
    }

    /**
     * Creates a new instance of {@link Builder}.
     *
     * @return a new instance of {@link Builder}
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns an empty {@link RangeConstraints} instance, i.e. a range that contains no values.
     *
     * @return an empty {@link RangeConstraints} instance.
     */
    @Nonnull
    public static RangeConstraints emptyRange() {
        return Builder.emptyRange;
    }
}
