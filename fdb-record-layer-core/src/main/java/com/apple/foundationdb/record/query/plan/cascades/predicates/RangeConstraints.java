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
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexComparison;
import com.apple.foundationdb.record.planprotos.PCompilableRange;
import com.apple.foundationdb.record.planprotos.PRangeConstraints;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.UsesValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.tuple.Tuple;
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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Represents a compile-time range that can be evaluated against other compile-time ranges with an optional list
 * of deferred ranges that cannot be evaluated at compile-time but can still be used as scan index prefix.
 */
public class RangeConstraints implements PlanHashable, Correlated<RangeConstraints>, UsesValueEquivalence<RangeConstraints>, PlanSerializable {

    @Nonnull
    private final Supplier<List<Comparisons.Comparison>> comparisonsCalculator;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsCalculator;

    /**
     * Amortized checker which checks whether any of the underlying range {@link Comparisons.Comparison}
     * is a {@link Comparisons.ValueComparison} with a
     * {@link ConstantObjectValue} comparand. This check is needed for situations where we want to compile-time
     * evaluate this {@link RangeConstraints}.
     */
    @Nonnull
    private final Supplier<Boolean> constantValueComparandsChecker;

    @Nullable
    private final CompilableRange evaluableRange; // null = entire range (if no deferred ranges are defined).

    @Nonnull
    private final Set<Comparisons.Comparison> deferredRanges;

    @Nonnull
    private static final Range<Boundary> emptyRange = Range.closedOpen(
            Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0), EvaluationContext.empty()),
            Boundary.from(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0), EvaluationContext.empty()));

    /**
     * Creates a new instance of the {@link RangeConstraints}.
     *
     * @param evaluableRange the compile-time evaluable range.
     * @param deferredRanges a list of ranges that are not compile-time evaluable but can still be used in a scan prefix.
     */
    private RangeConstraints(@Nullable final CompilableRange evaluableRange,
                             @Nonnull final Set<Comparisons.Comparison> deferredRanges) {
        this.evaluableRange = evaluableRange;
        this.deferredRanges = ImmutableSet.copyOf(deferredRanges);
        this.comparisonsCalculator = Suppliers.memoize(this::computeComparisons);
        this.correlationsCalculator = Suppliers.memoize(this::computeCorrelations);
        this.constantValueComparandsChecker = Suppliers.memoize(this::checkConstantValueComparands);
    }

    /**
     * Computes a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     *
     * @return a list of {@link Comparisons.Comparison} from this {@link RangeConstraints}.
     */
    @Nonnull
    private List<Comparisons.Comparison> computeComparisons() {
        final ImmutableList.Builder<Comparisons.Comparison> result = ImmutableList.builder();
        result.addAll(deferredRanges);
        if (evaluableRange != null) {
            result.addAll(evaluableRange.compilableComparisons);
        }
        return result.build();
    }

    public boolean isConstraining() {
        return evaluableRange != null || !deferredRanges.isEmpty();
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
        if (constantValueComparandsChecker.get()) {
            return this;
        }

        final var builder = newBuilder();
        for (final var comparison : getComparisons()) {
            if (comparison instanceof Comparisons.ValueComparison) {
                final var newComparand = ((Comparisons.ValueComparison)comparison).getComparandValue().compileTimeEval(context);
                builder.addComparisonMaybe(new Comparisons.SimpleComparison(comparison.getType(), newComparand));
            } else {
                builder.addComparisonMaybe(comparison);
            }
        }

        return builder.build().orElseThrow(() -> new RecordCoreException("could not build compile-time range constraint"));
    }

    public boolean isCompileTime() {
        return constantValueComparandsChecker.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelations() {
        final ImmutableSet.Builder<CorrelationIdentifier> result = ImmutableSet.builder();
        deferredRanges.forEach(c -> result.addAll(c.getCorrelatedTo()));
        if (evaluableRange != null) {
            evaluableRange.compilableComparisons.forEach(c -> result.addAll(c.getCorrelatedTo()));
        }
        return result.build();
    }

    private boolean checkConstantValueComparands() {
        boolean foundConstantValue = false;
        for (final var comparison : getComparisons()) {
            if (comparison instanceof Comparisons.ValueComparison &&
                    ((Comparisons.ValueComparison)comparison).getComparandValue() instanceof ConstantObjectValue) {
                foundConstantValue = true;
                break;
            }
        }
        return foundConstantValue;
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
     * @param evaluationContext The evaluation context required for compiling this range.
     * @return if the range is compile-time it returns {@code true} if it is empty or {@code false} if it is not
     * otherwise {@code unknown}, under a given evaluation context.
     */
    @Nonnull
    public Proposition isEmpty(@Nonnull final EvaluationContext evaluationContext) {
        if (deferredRanges.isEmpty()) {
            final var range = Objects.requireNonNull(evaluableRange).compile(evaluationContext);
            if (range != null && range.isEmpty()) {
                return Proposition.TRUE;
            } else {
                return Proposition.FALSE;
            }
        }
        return Proposition.UNKNOWN;
    }

    /**
     * checks whether {@code this} range encloses another {@link RangeConstraints} under an {@link EvaluationContext}.
     * Both ranges <b>must</b> be compile-time.
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
     * @param evaluationContext The evaluation context for compiling {@code this} and {@code other} range constraints.
     * @return {@code true} if the range is known, at compile-time, to enclose {@code other} or {@code false} if not.
     * Otherwise, it returns {@code unknown}.
     */
    @Nonnull
    public Proposition encloses(@Nonnull final RangeConstraints other, @Nonnull final EvaluationContext evaluationContext) {
        if (!isCompileTimeEvaluable() || !other.isCompileTimeEvaluable()) {
            return Proposition.UNKNOWN;
        } else if (evaluableRange == null) { // full range implies everything
            return Proposition.TRUE;
        } else if (other.evaluableRange == null) { // other is full range, we are not -> false
            return Proposition.FALSE;
        } else {
            final var thisRange = evaluableRange.compile(evaluationContext);
            if (thisRange == null) {
                return Proposition.TRUE; // full range implies everything
            }
            final var otherRange = other.evaluableRange.compile(evaluationContext);
            if (otherRange == null) {
                return Proposition.FALSE;
            } else if (thisRange.encloses(otherRange)) {
                return Proposition.TRUE;
            } else {
                return Proposition.FALSE;
            }
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, evaluableRange);
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
        if (evaluableRange == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.rebase(translationMap)).collect(ImmutableSet.toImmutableSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.rebase(translationMap)).collect(ImmutableSet.toImmutableSet());
            var newCompilableComparisons = evaluableRange.compilableComparisons.stream().map(c -> c.rebase(translationMap)).collect(ImmutableSet.toImmutableSet());
            return new RangeConstraints(new CompilableRange(newCompilableComparisons), newNonCompileTimeComparisons);
        }
    }

    @Nonnull
    public RangeConstraints translateCorrelations(@Nonnull final TranslationMap translationMap) {
        if (evaluableRange == null) {
            if (deferredRanges.isEmpty()) {
                return this;
            } else {
                var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet());
                return new RangeConstraints(null, newNonCompileTimeComparisons);
            }
        } else {
            var newNonCompileTimeComparisons = deferredRanges.stream().map(c -> c.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet());
            var newCompilableComparisons = evaluableRange.compilableComparisons.stream().map(c -> c.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet());
            return new RangeConstraints(new CompilableRange(newCompilableComparisons), newNonCompileTimeComparisons);
        }
    }

    @Nonnull
    public RangeConstraints translateValue(@Nonnull final UnaryOperator<Value> translator) {
        final var newEvaluableRange = evaluableRange == null ? null : evaluableRange.translateValue(translator);
        final var newDeferredRange = deferredRanges.stream().map(deferredRange -> deferredRange.translateValue(translator)).collect(ImmutableSet.toImmutableSet());
        return new RangeConstraints(newEvaluableRange, newDeferredRange);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (other == null) {
            return false;
        }

        if (this == other) {
            return true;
        }

        if (this.getClass() != other.getClass()) {
            return false;
        }

        return semanticEquals(other, ValueEquivalence.fromAliasMap(aliasMap)).isTrue();
    }

    @Nonnull
    @Override
    public BooleanWithConstraint semanticEqualsTyped(@Nonnull final RangeConstraints other,
                                                     @Nonnull final ValueEquivalence valueEquivalence) {
        if (deferredRanges.size() != other.deferredRanges.size()) {
            return BooleanWithConstraint.falseValue();
        }

        var deferredRangesEquals =
                valueEquivalence.semanticEquals(deferredRanges, other.deferredRanges);
        if (deferredRangesEquals.isFalse()) {
            return BooleanWithConstraint.falseValue();
        }

        if (evaluableRange == null && other.evaluableRange == null) {
            return deferredRangesEquals;
        }

        return deferredRangesEquals
                .compose(ignored -> {
                    if (evaluableRange != null && other.evaluableRange != null) {
                        return valueEquivalence.semanticEquals(evaluableRange.compilableComparisons,
                                other.evaluableRange.compilableComparisons);
                    }
                    return BooleanWithConstraint.falseValue();
                });
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(evaluableRange, deferredRanges);
    }

    @Nonnull
    @Override
    public PRangeConstraints toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRangeConstraints.Builder builder = PRangeConstraints.newBuilder();
        if (evaluableRange != null) {
            builder.setEvaluableRange(evaluableRange.toProto(serializationContext));
        }
        for (final Comparisons.Comparison deferredRange : deferredRanges) {
            builder.addDeferredRanges(deferredRange.toComparisonProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    public static RangeConstraints fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PRangeConstraints rangeConstraintsProto) {
        final ImmutableSet.Builder<Comparisons.Comparison> deferredRangesBuilder = ImmutableSet.builder();
        for (int i = 0; i < rangeConstraintsProto.getDeferredRangesCount(); i ++) {
            deferredRangesBuilder.add(
                    Comparisons.Comparison.fromComparisonProto(serializationContext, rangeConstraintsProto.getDeferredRanges(i)));
        }
        return new RangeConstraints(
                rangeConstraintsProto.hasEvaluableRange()
                ? CompilableRange.fromProto(serializationContext, rangeConstraintsProto.getEvaluableRange()) : null,
                deferredRangesBuilder.build());
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
        private static Boundary from(@Nonnull final Comparisons.Comparison comparison,
                                     @Nonnull final EvaluationContext evaluationContext) {
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
            if (comparison.hasMultiColumnComparand()) {
                items.addAll(((Tuple)comparand).getItems());
            } else {
                items.add(ScanComparisons.toTupleItem(comparand));
            }
            return Tuple.fromList(items);
        }
    }

    /**
     * Represents a range that comprises an intersection of a set of {@link Comparisons.Comparison} with reference operands
     * (i.e. {@link ConstantObjectValue}s). The range can be evaluated at compile-time under a given {@link EvaluationContext}.
     */
    public static class CompilableRange implements PlanHashable, PlanSerializable {

        @Nonnull
        private final Set<Comparisons.Comparison> compilableComparisons;

        public CompilableRange(@Nonnull final Set<Comparisons.Comparison> compilableComparisons) {
            this.compilableComparisons = compilableComparisons;
        }

        public void intersect(@Nullable final CompilableRange other) {
            if (other == null) { // entire range
                return;
            }
            compilableComparisons.addAll(other.compilableComparisons);
        }

        /**
         * Evaluates {@code this} into a {@link Range} under the provided {@link EvaluationContext}.
         * @param evaluationContext The evaluation contextn.
         * @return a compiled {@link Range}, if no compilable ranges are found, it returns {@code null}.
         */
        @Nullable
        public Range<Boundary> compile(@Nonnull final EvaluationContext evaluationContext) {
            Range<Boundary> range = null;
            for (final var comparison : compilableComparisons) {
                final var comparisonRange = toRange(comparison, evaluationContext);
                if (range == null) {
                    range = comparisonRange;
                } else {
                    if (range.isConnected(comparisonRange)) {
                        range = range.intersection(comparisonRange);
                    } else {
                        return emptyRange;
                    }
                }
            }
            return range;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return PlanHashable.objectPlanHash(mode, compilableComparisons);
        }

        @Override
        public String toString() {
            return compilableComparisons.stream().map(Objects::toString).collect(Collectors.joining("∩"));
        }

        @Nonnull
        private static Range<Boundary> toRange(@Nonnull Comparisons.Comparison comparison,
                                               @Nonnull final EvaluationContext evaluationContext) {
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
                    throw new RecordCoreException("cannot transform comparison to range").addLogInfo(LogMessageKeys.COMPARISON_VALUE, comparison);
            }
        }

        @Nonnull
        public CompilableRange translateValue(@Nonnull final UnaryOperator<Value> translator) {
            final var newCompilableComparisons = compilableComparisons.stream().map(compilableComparison -> compilableComparison.translateValue(translator)).collect(ImmutableSet.toImmutableSet());
            return new CompilableRange(newCompilableComparisons);
        }

        @Nonnull
        @Override
        public PCompilableRange toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PCompilableRange.Builder builder = PCompilableRange.newBuilder();
            for (final Comparisons.Comparison compilableComparison : compilableComparisons) {
                builder.addCompilableComparisons(compilableComparison.toComparisonProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        public static CompilableRange fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PCompilableRange compilableRangeProto) {
            final ImmutableSet.Builder<Comparisons.Comparison> compilableComparisonsBuilder = ImmutableSet.builder();
            for (int i = 0; i < compilableRangeProto.getCompilableComparisonsCount(); i ++) {
                compilableComparisonsBuilder.add(
                        Comparisons.Comparison.fromComparisonProto(serializationContext, compilableRangeProto.getCompilableComparisons(i)));
            }
            return new CompilableRange(compilableComparisonsBuilder.build());
        }
    }

    /**
     * Builds an instance of {@link RangeConstraints}.
     */
    public static class Builder {

        @Nonnull
        private ImmutableSet.Builder<Comparisons.Comparison> compilableComparisons;

        @Nonnull
        private ImmutableSet.Builder<Comparisons.Comparison> nonCompilableComparisons;

        @Nonnull
        private static final Set<Comparisons.Type> allowedComparisonTypes = new LinkedHashSet<>();


        @Nonnull
        private static final RangeConstraints emptyRange = new RangeConstraints(
                new CompilableRange(Set.of(
                        new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0),
                        new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0))),
                Set.of());

        static {
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN);
            allowedComparisonTypes.add(Comparisons.Type.GREATER_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN);
            allowedComparisonTypes.add(Comparisons.Type.LESS_THAN_OR_EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.EQUALS);
            allowedComparisonTypes.add(Comparisons.Type.IS_NULL);
            allowedComparisonTypes.add(Comparisons.Type.NOT_NULL);
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
            this.compilableComparisons = ImmutableSet.builder();
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
                case LIKE:
                    return false;
                default:
                    throw new RecordCoreException("unexpected comparison type").addLogInfo(LogMessageKeys.COMPARISON_TYPE, comparison.getType());
            }
        }

        public boolean addComparisonMaybe(@Nonnull final Comparisons.Comparison comparison) {
            if (!canBeUsedInScanPrefix(comparison)) {
                return false;
            }
            if (isCompileTime(comparison)) {
                compilableComparisons.add(comparison);
            } else {
                nonCompilableComparisons.add(comparison);
            }
            return true;
        }

        public void add(@Nonnull final RangeConstraints rangeConstraints) {
            if (rangeConstraints.evaluableRange != null) {
                this.compilableComparisons = ImmutableSet.<Comparisons.Comparison>builder().addAll(intersect(compilableComparisons.build(), rangeConstraints.evaluableRange.compilableComparisons));
            }
            this.nonCompilableComparisons = ImmutableSet.<Comparisons.Comparison>builder().addAll(intersect(nonCompilableComparisons.build(), rangeConstraints.deferredRanges));
        }

        @Nonnull
        public Optional<RangeConstraints> build() {
            final var compilableComparisonsList = compilableComparisons.build();
            final var nonCompilableComparisonsList = nonCompilableComparisons.build();
            if (compilableComparisonsList.isEmpty() && nonCompilableComparisonsList.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new RangeConstraints(new CompilableRange(compilableComparisonsList), nonCompilableComparisonsList));
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
