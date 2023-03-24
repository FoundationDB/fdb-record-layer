/*
 * RangeConstraints.java
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

import com.apple.foundationdb.record.EvaluationContext;
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
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
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

    @Nonnull
    private final Supplier<List<Comparisons.Comparison>> comparisonsCalculator;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsCalculator;

    @Nonnull
    private final Supplier<Boolean> compileTimeChecker;

    @Nullable
    private final Range<Boundary> evaluableRange; // null = entire range (if no deferred ranges are defined).

    @Nonnull
    private final Set<Comparisons.Comparison> deferredRanges;

    /**
     * Creates a new instance of the {@link RangeConstraints}.
     *
     * @param evaluableRange the compile-time evaluable range.
     * @param deferredRanges a list of none compile-time evaluable ranges but can still be used in a scan prefix.
     */
    private RangeConstraints(@Nullable final Range<Boundary> evaluableRange,
                             @Nonnull final Set<Comparisons.Comparison> deferredRanges) {
        this.evaluableRange = evaluableRange;
        this.deferredRanges = ImmutableSet.copyOf(deferredRanges);
        this.comparisonsCalculator = Suppliers.memoize(this::computeComparisons);
        this.correlationsCalculator = Suppliers.memoize(this::computeCorrelations);
        this.compileTimeChecker = () -> {
            boolean foundConstantValue = false;
            for (final var comparison : getComparisons()) {
                if (comparison.getComparand() instanceof ConstantObjectValue) {
                    foundConstantValue = true;
                    break;
                }
                if (comparison instanceof Comparisons.ValueComparison &&
                        ((Comparisons.ValueComparison)comparison).getComparandValue() instanceof ConstantObjectValue) {
                    foundConstantValue = true;
                    break;
                }
            }
            return foundConstantValue;
        };
    }

    /**
     * Creates a new instance of {@link RangeConstraints}.
     *
     * @param evaluableRange the compile-time evaluable range.
     */
    private RangeConstraints(@Nonnull final Range<Boundary> evaluableRange) {
        this(evaluableRange, ImmutableSet.of());
    }

    /**
     * Computes a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     *
     * @return a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     */
    @Nonnull
    private List<Comparisons.Comparison> computeComparisons() {
        if (isEquality() == Proposition.TRUE) {
            return List.of(Objects.requireNonNull(evaluableRange).upperEndpoint().comparison);
        }
        ImmutableList.Builder<Comparisons.Comparison> result = ImmutableList.builder();
        result.addAll(deferredRanges);
        if (evaluableRange != null) {
            if (evaluableRange.hasUpperBound()) {
                result.add(evaluableRange.upperEndpoint().getComparison());
            }
            if (evaluableRange.hasLowerBound()) {
                result.add(evaluableRange.lowerEndpoint().getComparison());
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
    public RangeConstraints compileTimeEval(@Nonnull final EvaluationContext context) {
        if (compileTimeChecker.get()) {
            return this;
        }

        final var builder = newBuilder();
        for (final var comparison : getComparisons()) {
            if (comparison.getComparand() instanceof ConstantObjectValue) {
                final var newComparand = ((ConstantObjectValue)comparison.getComparand()).compileTimeEval(context);
                if (comparison instanceof Comparisons.ValueComparison) {
                    builder.addComparisonMaybe(new Comparisons.SimpleComparison(comparison.getType(), newComparand), context);
                } else {
                    builder.addComparisonMaybe(comparison.withComparand(newComparand), context);
                }
            } else {
                builder.addComparisonMaybe(comparison, context);
            }
        }

        return builder.build().orElseThrow(() -> new RecordCoreException("could not build compile-time range constraint"));
    }

    public boolean isCompileTime() {
        return compileTimeChecker.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelations() {
        final ImmutableSet.Builder<CorrelationIdentifier> result = ImmutableSet.builder();
        deferredRanges.forEach(c -> result.addAll(c.getCorrelatedTo()));
        if (evaluableRange != null) {
            if (evaluableRange.hasLowerBound()) {
                result.addAll(evaluableRange.lowerEndpoint().getComparison().getCorrelatedTo());
            }
            if (evaluableRange.hasUpperBound()) {
                result.addAll(evaluableRange.upperEndpoint().getComparison().getCorrelatedTo());
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
    public Proposition isEmpty() {
        if (deferredRanges.isEmpty()) {
            if (Objects.requireNonNull(evaluableRange).isEmpty()) {
                return Proposition.TRUE;
            } else {
                return Proposition.FALSE;
            }
        }
        return Proposition.UNKNOWN;
    }

    /**
     * checks whether the range is known to contain a single value at compile-time or not.

     * @return if the range is known to contain a single value at compile-time it returns {@code true} or {@code false} if it is not,
     * otherwise {@code unknown}.
     */
    @Nonnull
    public Proposition isEquality() {
        if (deferredRanges.isEmpty()) {
            if (Objects.requireNonNull(evaluableRange).hasLowerBound() && evaluableRange.hasUpperBound() &&
                    evaluableRange.lowerEndpoint().equals(evaluableRange.upperEndpoint())) {
                return Proposition.TRUE;
            } else {
                return Proposition.FALSE;
            }
        }
        return Proposition.UNKNOWN;
    }

    /**
     * checks whether {@code this} range encloses another {@link RangeConstraints}. Both ranges <b>must</b> be compile-time.
     * <br>
     * <b>Examples</b>:
     * <ul>
     * <li>{@code this = [1, 10), other = (2, 5) => this.encloses(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = (0, 5) => this.encloses(other) = FALSE}</li>
     * <li>{@code this = [1, 10), other = [1, 10) => this.encloses(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = (1, 10) => this.encloses(other) = TRUE}</li>
     * <li>{@code this = [1, 10), other = () => this.encloses(other) = FALSE}</li>
     * <li>{@code this = {[1, 10), deferred = STARTS_WITH}, other = (2, 5) => this.encloses(other) = UNKNOWN}</li>
     * </ul>
     *
     * @param other The other range to compare against.
     * @return {@code true} if the range is known, at compile-time, to enclose {@code other} or {@code false} if not. Otherwise
     * it returns {@code unknown}.
     */
    @Nonnull
    public Proposition encloses(@Nonnull final RangeConstraints other) {
        if (!isCompileTimeEvaluable() || !other.isCompileTimeEvaluable()) {
            return Proposition.UNKNOWN;
        } else if (evaluableRange == null) { // full range implies everything
            return Proposition.TRUE;
        } else if (other.evaluableRange == null) { // other is full range, we are not -> false
            return Proposition.FALSE;
        } else if (evaluableRange.encloses(other.evaluableRange)) {
            return Proposition.TRUE;
        } else {
            return Proposition.FALSE;
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, evaluableRange);
    }

    @Override
    @Nonnull
    public String toString() {
        final var result = new StringBuilder();
        if (evaluableRange == null) {
            result.append("(-∞..+∞)");
        } else {
            result.append(evaluableRange);
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
        return Objects.equals(evaluableRange, that.evaluableRange) && deferredRanges.equals(that.deferredRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evaluableRange, deferredRanges);
    }

    @Nonnull
    @Override
    public RangeConstraints rebase(@Nonnull final AliasMap translationMap) {
        if (isEmpty().equals(Proposition.TRUE)) {
            return this;
        } else if (evaluableRange == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.rebase(translationMap)).collect(ImmutableSet.toImmutableSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (evaluableRange.hasLowerBound()) {
                final var origLower = evaluableRange.lowerEndpoint().comparison;
                final var newLower = origLower.rebase(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (evaluableRange.hasUpperBound()) {
                final var origUpper = evaluableRange.upperEndpoint().comparison;
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
                resultBuilder.addComparisonMaybe(lower, null);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addComparisonMaybe(upper, null);
            }
            for (final var nonCompilableComparison : deferredRanges) {
                resultBuilder.addComparisonMaybe(nonCompilableComparison, null);
            }
            return resultBuilder.build().orElseThrow();
        }
    }

    @Nonnull
    public RangeConstraints translateCorrelations(@Nonnull final TranslationMap translationMap) {
        if (isEmpty().equals(Proposition.TRUE)) {
            return this;
        } else if (evaluableRange == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            boolean hasNewLowerComparison = false;
            Comparisons.Comparison lower = null;
            if (evaluableRange.hasLowerBound()) {
                final var origLower = evaluableRange.lowerEndpoint().comparison;
                final var newLower = origLower.translateCorrelations(translationMap);
                if (origLower == newLower) {
                    hasNewLowerComparison = true;
                    lower = newLower;
                }
            }

            boolean hasNewUpperComparison = false;
            Comparisons.Comparison upper = null;
            if (evaluableRange.hasUpperBound()) {
                final var origUpper = evaluableRange.upperEndpoint().comparison;
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
                resultBuilder.addComparisonMaybe(lower, null);
            }
            if (hasNewUpperComparison) {
                resultBuilder.addComparisonMaybe(upper, null);
            }
            for (final var nonCompilableComparison : deferredRanges) {
                resultBuilder.addComparisonMaybe(nonCompilableComparison, null);
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

        if (evaluableRange == null && that.evaluableRange == null) {
            return true;
        }

        if (evaluableRange == null || that.evaluableRange == null) {
            return false;
        }

        if (evaluableRange.hasUpperBound()) {
            if (!that.evaluableRange.hasUpperBound()) {
                return false;
            }
            if (!this.evaluableRange.upperEndpoint().getComparison().semanticEquals(that.evaluableRange.upperEndpoint().getComparison(), aliasMap)) {
                return false;
            }
        }

        if (evaluableRange.hasLowerBound()) {
            if (!that.evaluableRange.hasLowerBound()) {
                return false;
            }
            return this.evaluableRange.lowerEndpoint().getComparison().semanticEquals(that.evaluableRange.lowerEndpoint().getComparison(), aliasMap);
        }

        return true;
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(evaluableRange, deferredRanges);
    }

    /**
     * Represents a range boundary.
     */
    static class Boundary implements Comparable<Boundary> {

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
        private static Boundary from(@Nonnull final Comparisons.Comparison comparison, @Nullable final EvaluationContext evaluationContext) {
            return from(toTuple(comparison, evaluationContext), comparison);
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
        private static Tuple toTuple(@Nonnull final Comparisons.Comparison comparison, @Nonnull final EvaluationContext evaluationContext) {
            final List<Object> items = new ArrayList<>();
            var comparand = comparison.getComparand(null, evaluationContext);
            if (comparand instanceof ConstantObjectValue) {
                comparand = ((ConstantObjectValue)comparand).eval(null, evaluationContext);
            }
            if (comparison.hasMultiColumnComparand()) {
                items.addAll(((Tuple)comparand).getItems());
            } else {
                items.add(ScanComparisons.toTupleItem(comparand));
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
                Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0), null),
                Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0), null)));

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
                return RangeConstraints.emptyRange().evaluableRange;
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

        public boolean addComparisonMaybe(@Nonnull final Comparisons.Comparison comparison, @Nullable final EvaluationContext evaluationContext) {
            if (!canBeUsedInScanPrefix(comparison)) {
                return false;
            }
            if (!isCompileTime(comparison)) {
                nonCompilableComparisons.add(comparison);
            } else {
                addRange(toRange(comparison, evaluationContext), evaluationContext);
            }
            return true;
        }

        private void addRange(@Nonnull final Range<Boundary> range, @Nonnull final EvaluationContext evaluationContext) {
            if (this.range == null) {
                this.range = range;
            } else {
                if (this.range.isConnected(range)) {
                    this.range = this.range.intersection(range);
                } else {
                    this.range = RangeConstraints.emptyRange().evaluableRange;
                }
            }
        }

        public void add(@Nonnull final RangeConstraints rangeConstraints) {
            this.range = intersect(range, rangeConstraints.evaluableRange);
            this.nonCompilableComparisons = ImmutableSet.<Comparisons.Comparison>builder().addAll(intersect(nonCompilableComparisons.build(), rangeConstraints.deferredRanges));
        }

        @Nonnull
        public Range<Boundary> toRange(@Nonnull Comparisons.Comparison comparison, @Nullable final EvaluationContext evaluationContext) {
            final var boundary = Boundary.from(comparison, evaluationContext);
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
