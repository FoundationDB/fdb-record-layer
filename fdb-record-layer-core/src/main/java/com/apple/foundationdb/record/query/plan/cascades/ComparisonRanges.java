/*
 * ComparisonRanges.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class that operates on lists of {@link ComparisonRange}s.
 */
public class ComparisonRanges implements PlanHashable, Correlated<ComparisonRanges> {
    @Nonnull
    private final List<ComparisonRange> ranges;

    private int sealedSize;

    public ComparisonRanges() {
        this.ranges = Lists.newArrayList();
    }

    public ComparisonRanges(@Nonnull final List<ComparisonRange> ranges) {
        this.ranges = Lists.newArrayList(ranges);
        this.sealedSize = 0;
    }

    public void clear() {
        ranges.clear();
        this.sealedSize = 0;
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    public void commitAndAdvance() {
        sealedSize = ranges.size();
    }

    public int uncommittedComparisonRangesSize() {
        return ranges.size() - sealedSize;
    }

    @Nonnull
    public List<ComparisonRange> getUncommittedComparisonRanges() {
        return ranges.subList(sealedSize, ranges.size());
    }

    public void addEqualityComparison(@Nonnull final Comparisons.Comparison comparison) {
        Verify.verify(sealedSize == ranges.size());
        final ComparisonRange newComparisonRange = ComparisonRange.from(comparison);
        Verify.verify(newComparisonRange.isEquality());
        ranges.add(newComparisonRange);
    }

    public void addInequalityComparison(@Nonnull final Comparisons.Comparison comparison) {
        final ComparisonRange newComparisonRange = ComparisonRange.from(comparison);
        Verify.verify(newComparisonRange.isInequality());
        if (sealedSize < ranges.size()) {
            final ComparisonRange currentComparisonRange = ranges.get(sealedSize);
            Verify.verify(!currentComparisonRange.isEquality());
            final ComparisonRange.MergeResult mergeResult =
                    Objects.requireNonNull(currentComparisonRange).merge(newComparisonRange);
            Verify.verify(mergeResult.getResidualComparisons().isEmpty());
            ranges.set(sealedSize, mergeResult.getComparisonRange());
        } else {
            ranges.add(newComparisonRange);
        }
    }

    public void addEmptyRanges(final int n) {
        for (int i = 0; i < n; i++) {
            ranges.add(ComparisonRange.EMPTY);
        }
    }

    public void addAll(@Nonnull ComparisonRanges comparisonRanges) {
        Preconditions.checkArgument(isEqualities());
        ranges.addAll(comparisonRanges.ranges);
    }

    public boolean isEqualities() {
        return ranges.stream().allMatch(ComparisonRange::isEquality);
    }

    public int getEqualitiesSize() {
        int i;
        for (i = 0; i < ranges.size(); i++) {
            final ComparisonRange range = ranges.get(i);
            if (!range.isEquality()) {
                return i;
            }
        }
        return i;
    }

    @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
    boolean isPrefixRanges() {
        for (int i = 0; i < ranges.size(); i++) {
            final ComparisonRange comparisonRange = ranges.get(i);
            if (comparisonRange.isEquality()) {
                continue;
            }
            if (!comparisonRange.isInequality()) {
                // range is none
                return false;
            }
            return i + 1 == ranges.size();
        }
        return true;
    }

    @Nonnull
    public ComparisonRanges toPrefixRanges() {
        int last;
        for (last = 0; last < ranges.size(); last++) {
            final ComparisonRange comparisonRange = ranges.get(last);
            if (!comparisonRange.isEquality()) {
                // if none don't take the none; if inequality take it
                if (comparisonRange.isInequality()) {
                    last ++;
                }
                break;
            }
        }

        return new ComparisonRanges(ranges.subList(0, last));
    }

    @Nonnull
    public ScanComparisons toScanComparisons() {
        final ComparisonRanges prefixRanges = toPrefixRanges();

        final List<Comparisons.Comparison> equalityComparisons = Lists.newArrayList();
        final Set<Comparisons.Comparison> inequalityComparisons = Sets.newHashSet();

        for (final ComparisonRange range : prefixRanges.ranges) {
            if (range.isEquality()) {
                equalityComparisons.add(range.getEqualityComparison());
            } else {
                inequalityComparisons.addAll(range.getInequalityComparisons());
            }
        }

        return new ScanComparisons(equalityComparisons, inequalityComparisons);
    }

    public int size() {
        return ranges.size();
    }

    public int totalSize() {
        return ranges.stream()
                .mapToInt(range -> {
                    switch (range.getRangeType()) {
                        case EMPTY:
                            return 0;
                        case EQUALITY:
                            return 1;
                        case INEQUALITY:
                            return Objects.requireNonNull(range.getInequalityComparisons()).size();
                        default:
                            throw new RecordCoreException("unsupported range type");
                    }
                })
                .sum();
    }

    @Nonnull
    public List<ComparisonRange> getRanges() {
        return ranges;
    }

    @Nonnull
    public List<ComparisonRange> subRanges(final int startInclusive, final int endExclusive) {
        return ranges.subList(startInclusive, endExclusive);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ranges.stream()
                .flatMap(range -> range.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public ComparisonRanges rebase(@Nonnull final AliasMap aliasMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(aliasMap));
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ComparisonRanges translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final ImmutableList.Builder<ComparisonRange> rebasedRangesBuilder = ImmutableList.builder();
        boolean isSame = true;
        for (final ComparisonRange range : ranges) {
            final ComparisonRange rebasedRange = range.translateCorrelations(translationMap);
            if (rebasedRange != range) {
                isSame = false;
            }
            rebasedRangesBuilder.add(rebasedRange);
        }
        if (isSame) {
            return this;
        }
        return new ComparisonRanges(rebasedRangesBuilder.build());
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
        final ComparisonRanges otherComparisonRanges = (ComparisonRanges)other;
        if (size() != otherComparisonRanges.size()) {
            return false;
        }

        for (int i = 0; i < ranges.size(); i++) {
            final ComparisonRange range = ranges.get(i);
            final ComparisonRange otherRange = otherComparisonRanges.ranges.get(i);

            if (!range.semanticEquals(otherRange, aliasMap)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int semanticHashCode() {
        int hashCode = 0;
        for (final ComparisonRange range : ranges) {
            hashCode = 31 * hashCode + range.semanticHashCode();
        }
        return hashCode;
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return ranges.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectPlanHash(hashKind, ranges);
    }

    @Override
    public String toString() {
        return "{" + ranges.stream().map(ComparisonRange::toString).collect(Collectors.joining("; ")) + "}";
    }

    @Nonnull
    public static ComparisonRanges fromScanComparisons(@Nonnull final ScanComparisons scanComparisons) {
        final ImmutableList.Builder<ComparisonRange> rangesBuilder = ImmutableList.builder();

        for (final Comparisons.Comparison comparison : scanComparisons.getEqualityComparisons()) {
            rangesBuilder.add(ComparisonRange.from(comparison));
        }

        if (!scanComparisons.isEquality()) {
            rangesBuilder.add(ComparisonRange.fromInequalities(scanComparisons.getInequalityComparisons()));
        }

        return new ComparisonRanges(rangesBuilder.build());
    }
}
