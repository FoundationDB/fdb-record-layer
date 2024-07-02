/*
 * ComparisonRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A set of compatible comparisons on a single field of a {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression}
 * representing a contiguous range of values for that field.
 *
 * <p>
 * A {@code ComparisonRange} is similar to a {@link ScanComparisons} but represents a contiguous range of values for a
 * single field, rather than for an entire {@code KeyExpression}. A comparison range is effectively a sum type, with
 * the following options:
 * </p>
 * <ul>
 *     <li>An empty set of comparisons, indicating that the entire universe of possible values is in the range.</li>
 *     <li>
 *         An equality comparison on the field. There can only be a single such comparison. For example, single
 *         comparison of the form {@code EQUALS "foo"}.
 *     </li>
 *     <li>
 *         A set of inequality comparisons that define a single contiguous range of values. For example, the combination
 *         of the comparisons {@code > 8} and {@code < 30}. The comparison range may include redundant
 *         comparisons by contract, but it may or may not simplify the range into a more compact form. For example,
 *         the comparison range can include the comparisons {@code > 8}, {@code < 30}, and {@code < 20}, but it
 *         may optionally simplify this to {@code > 8} and {@code < 20}. Note that this behavior is not fully
 *         implemented right now; similarly, this implementation does not currently convert a range such as {@code < 8}
 *         and {@code > 30} to an empty range. However, this normalization logic will be added here in the future.
 *         <!-- TODO Add range validation to ensure that a {@code ComparisonRange} is contiguous. -->
 *     </li>
 * </ul>
 *
 * <p>
 * A {@code ComparisonRange} is an immutable object that provides a variety of methods for producing new range from the
 * current one and some {@link Comparisons.Comparison} objects.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ComparisonRange implements PlanHashable, Correlated<ComparisonRange> {
    public static final ComparisonRange EMPTY = new ComparisonRange();

    /**
     * Comparison ranges can be divided into three types. These types represent distinct planning behavior when
     * matching query predicates or filters to index expressions:
     * <ul>
     *     <li>Empty ranges, to which any comparison can be added.</li>
     *     <li>Equality ranges, to which only the same (equality) comparison can be added.</li>
     *     <li>Inequality ranges, to which any other comparison can be added.</li>
     * </ul>
     *
     * <p>
     * Furthermore, the planner uses this trichotomy of range types to determine other planning behavior. For example,
     * an index scan must involve (from left to right) any number of equality ranges on the fields, followed by a single,
     * optional inequality range, followed by any number of empty ranges.
     * </p>
     */
    public enum Type {
        EMPTY,
        EQUALITY,
        INEQUALITY
    }

    @Nullable
    private final Comparisons.Comparison equalityComparison;
    @Nullable
    private final List<Comparisons.Comparison> inequalityComparisons;

    private ComparisonRange() {
        this.equalityComparison = null;
        this.inequalityComparisons = null;
    }

    private ComparisonRange(@Nonnull final Comparisons.Comparison equalityComparison) {
        this.equalityComparison = equalityComparison;
        this.inequalityComparisons = null;
    }

    private ComparisonRange(@Nonnull final Iterable<Comparisons.Comparison> inequalityComparisons) {
        this.equalityComparison = null;
        this.inequalityComparisons = Lists.newArrayList(inequalityComparisons);
    }

    public boolean isEmpty() {
        return equalityComparison == null && inequalityComparisons == null;
    }

    public boolean isEquality() {
        return equalityComparison != null && inequalityComparisons == null;
    }

    public boolean isInequality() {
        return equalityComparison == null && inequalityComparisons != null;
    }

    @Nonnull
    public Type getRangeType() {
        if (isEmpty()) {
            return Type.EMPTY;
        } else if (isEquality()) {
            return Type.EQUALITY;
        } else {
            return Type.INEQUALITY;
        }
    }

    @Nonnull
    public Comparisons.Comparison getEqualityComparison() {
        if (equalityComparison == null) {
            throw new RecordCoreException("tried to get non-existent equality comparison from ComparisonRange");
        }
        return equalityComparison;
    }

    @Nonnull
    public List<Comparisons.Comparison> getInequalityComparisons() {
        if (inequalityComparisons == null) {
            throw new RecordCoreException("tried to get non-existent inequality comparisons from ComparisonRange");
        }
        return inequalityComparisons;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final var builder = ImmutableSet.<CorrelationIdentifier>builder();
        if (equalityComparison != null) {
            builder.addAll(equalityComparison.getCorrelatedTo());
        }
        if (inequalityComparisons != null) {
            for (final var inequalityComparison : inequalityComparisons) {
                builder.addAll(inequalityComparison.getCorrelatedTo());
            }
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public ComparisonRange rebase(@Nonnull final AliasMap aliasMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(aliasMap));
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ComparisonRange translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final var translatedEqualityComparison =
                equalityComparison  == null
                ? null
                : equalityComparison.translateCorrelations(translationMap);

        final List<Comparisons.Comparison> rebasedInequalityComparisons;
        if (inequalityComparisons != null) {
            boolean allRemainedSame = true;
            final var translatedInequalityComparisonsBuilder = ImmutableList.<Comparisons.Comparison>builder();
            for (final var inequalityComparison : inequalityComparisons) {
                final var translatedInequalityComparison = inequalityComparison.translateCorrelations(translationMap);
                translatedInequalityComparisonsBuilder.add(translatedInequalityComparison);
                if (inequalityComparison != translatedInequalityComparison) {
                    allRemainedSame = false;
                }
            }
            rebasedInequalityComparisons = allRemainedSame ? inequalityComparisons : translatedInequalityComparisonsBuilder.build();
        } else {
            rebasedInequalityComparisons = null;
        }

        // reference equality is intended here as we use that mechanism to detect changes in the rebased objects
        if (translatedEqualityComparison == equalityComparison &&
                rebasedInequalityComparisons == inequalityComparisons) {
            return this;
        }
        if (isEquality()) {
            Objects.requireNonNull(translatedEqualityComparison);
            return new ComparisonRange(translatedEqualityComparison);
        } else if (isInequality()) {
            Objects.requireNonNull(rebasedInequalityComparisons);
            return new ComparisonRange(rebasedInequalityComparisons);
        }
        throw new IllegalStateException("this should never be reached");
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
        ComparisonRange that = (ComparisonRange)other;

        Verify.verify(isEquality() ^ isInequality());
        Verify.verify(that.isEquality() ^ that.isInequality());
        
        if (isEquality()) {
            return Correlated.semanticEquals(this.equalityComparison, that.equalityComparison, aliasMap);
        }
        if (isInequality()) {
            if (!that.isInequality()) {
                return false;
            }
            Objects.requireNonNull(this.inequalityComparisons);
            Objects.requireNonNull(that.inequalityComparisons);
            if (inequalityComparisons.size() != that.inequalityComparisons.size()) {
                return false;
            }
            for (final var inequalityComparison : inequalityComparisons) {
                boolean found = false;
                for (final var thatInequalityComparison : that.inequalityComparisons) {
                    if (inequalityComparison.semanticEquals(thatInequalityComparison, aliasMap)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }

            return true;
        }
        throw new IllegalStateException("this should never be reached");
    }

    @Override
    public int semanticHashCode() {
        if (isEquality()) {
            Objects.requireNonNull(equalityComparison);
            return equalityComparison.semanticHashCode();
        }
        if (isInequality()) {
            Objects.requireNonNull(inequalityComparisons);
            return inequalityComparisons.stream()
                    .map(Comparisons.Comparison::semanticHashCode)
                    .collect(ImmutableList.toImmutableList())
                    .hashCode();
        }
        throw new IllegalStateException("this should never be reached");
    }

    @SuppressWarnings({"ConstantConditions", "java:S2447"})
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final Object value) {
        if (value == null) {
            return null;
        }

        if (isEquality()) {
            return equalityComparison.eval(store, context, value);
        }
        if (isInequality()) {
            for (final Comparisons.Comparison inequalityComparison : inequalityComparisons) {
                final Boolean comparisonResult = inequalityComparison.eval(store, context, value);
                if (comparisonResult == null) {
                    return null;
                }
                if (!comparisonResult) {
                    return false;
                }
            }

            return true;
        }
        throw new RecordCoreException("unknown kind of comparison range");
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, equalityComparison, inequalityComparisons);
    }

    @Nonnull
    public ScanComparisons toScanComparisons() {
        if (isEmpty()) {
            return ScanComparisons.EMPTY;
        }

        if (isEquality()) {
            return new ScanComparisons(Lists.newArrayList(getEqualityComparison()), Collections.emptySet());
        }

        return new ScanComparisons(Collections.emptyList(), Sets.newHashSet(getInequalityComparisons()));
    }

    /**
     * Checks whether the comparison is already defined in {@code this} {@link ComparisonRange}. If so, it returns a
     * {@link MergeResult} of {@code this}. Otherwise, it returns a {@link MergeResult} of {@code this} and adds the
     * {@code comparison} as a residual.
     *
     * @param comparison the comparison to check.
     * @return {@link MergeResult} of {@code this}, potentially with {@code comparison} as a residual.
     */
    @Nonnull
    public MergeResult merge(@Nonnull final Comparisons.Comparison comparison) {
        final ScanComparisons.ComparisonType comparisonType = ScanComparisons.getComparisonType(comparison);
        if (comparisonType == ScanComparisons.ComparisonType.NONE) {
            return MergeResult.of(this, comparison);
        }

        if (isEmpty()) {
            return MergeResult.of(from(comparison));
        } else if (isEquality()) {
            switch (comparisonType) {
                case INEQUALITY:
                    return MergeResult.of(this, comparison);
                case EQUALITY:
                    if (getEqualityComparison().equals(comparison)) {
                        return MergeResult.of(this);
                    } else {
                        return MergeResult.of(this, comparison);
                    }
                default:
                    break;
            }
        } else if (isInequality()) {
            Objects.requireNonNull(inequalityComparisons);
            switch (comparisonType) {
                case INEQUALITY:
                    if (inequalityComparisons.contains(comparison)) {
                        return MergeResult.of(this);
                    } else {
                        return MergeResult.of(
                                new ComparisonRange(ImmutableList.<Comparisons.Comparison>builder()
                                        .addAll(inequalityComparisons)
                                        .add(comparison)
                                        .build()));
                    }
                case EQUALITY:
                    return MergeResult.of(from(comparison), inequalityComparisons);
                default:
                    break;
            }
        }
        return MergeResult.of(this, comparison);
    }

    @Nonnull
    public MergeResult merge(@Nonnull ComparisonRange comparisonRange) {
        final ImmutableList.Builder<Comparisons.Comparison> residualPredicatesBuilder = ImmutableList.builder();
        if (comparisonRange.isEmpty()) {
            return MergeResult.of(this);
        }

        if (isEmpty()) {
            return MergeResult.of(comparisonRange);
        }

        if (isEquality()) {
            return merge(comparisonRange.getEqualityComparison());
        }

        Verify.verify(isInequality());
        final List<Comparisons.Comparison> comparisons =
                Objects.requireNonNull(comparisonRange.getInequalityComparisons());

        ComparisonRange resultRange = this;
        for (final Comparisons.Comparison comparison : comparisons) {
            MergeResult mergeResult = resultRange.merge(comparison);
            resultRange = mergeResult.getComparisonRange();
            residualPredicatesBuilder.addAll(mergeResult.getResidualComparisons());
        }

        return MergeResult.of(resultRange, residualPredicatesBuilder.build());
    }

    @Override
    public String toString() {
        if (isEquality()) {
            return getEqualityComparison().toString();
        } else if (isInequality()) {
            Objects.requireNonNull(inequalityComparisons);
            return inequalityComparisons.stream().map(Comparisons.Comparison::toString)
                            .collect(Collectors.joining(" && ", "[", "]"));
        } else {
            return "[]";
        }
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    public static ComparisonRange from(@Nonnull final Comparisons.Comparison comparison) {
        final var result = tryFrom(comparison);
        if (result == null) {
            throw new RecordCoreException("unexpected comparison type");
        }
        return result;
    }

    @Nullable
    public static ComparisonRange tryFrom(@Nonnull final Comparisons.Comparison comparison) {
        switch (ScanComparisons.getComparisonType(comparison)) {
            case EQUALITY:
                return new ComparisonRange(comparison);
            case INEQUALITY:
                return ComparisonRange.fromInequalities(Collections.singletonList(comparison));
            case NONE:
            default:
                return null;
        }
    }

    @Nonnull
    public static ComparisonRange fromInequalities(@Nonnull Iterable<Comparisons.Comparison> comparisons) {
        Verify.verify(Streams.stream(comparisons)
                .allMatch(comparison -> ScanComparisons.getComparisonType(comparison) == ScanComparisons.ComparisonType.INEQUALITY));
        return new ComparisonRange(comparisons);
    }

    /**
     * Class to represent the outcome of a merge operation.
     */
    public static class MergeResult {
        @Nonnull
        private final ComparisonRange comparisonRange;
        @Nonnull
        private final List<Comparisons.Comparison> residualComparisons;

        private MergeResult(@Nonnull final ComparisonRange comparisonRange,
                            @Nonnull final List<Comparisons.Comparison> residualComparison) {
            this.comparisonRange = comparisonRange;
            this.residualComparisons = ImmutableList.copyOf(residualComparison);
        }

        @Nonnull
        public ComparisonRange getComparisonRange() {
            return comparisonRange;
        }

        @NonNull
        public List<Comparisons.Comparison> getResidualComparisons() {
            return residualComparisons;
        }

        public static MergeResult of(@Nonnull final ComparisonRange comparisonRange) {
            return of(comparisonRange, ImmutableList.of());
        }

        public static MergeResult of(@Nonnull final ComparisonRange comparisonRange,
                                     @Nonnull final Comparisons.Comparison residualComparison) {
            return new MergeResult(comparisonRange, ImmutableList.of(residualComparison));
        }

        public static MergeResult of(@Nonnull final ComparisonRange comparisonRange,
                                     @NonNull final List<Comparisons.Comparison> residualComparisons) {
            return new MergeResult(comparisonRange, residualComparisons);
        }
    }
}
