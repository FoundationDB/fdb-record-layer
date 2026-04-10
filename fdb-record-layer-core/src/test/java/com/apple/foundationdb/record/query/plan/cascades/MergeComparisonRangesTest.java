/*
 * MergeComparisonRangesTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ComparisonRange#merge}.
 *
 * <p>
 * These tests exercise all meaningful paths through both the
 * {@link ComparisonRange#merge(Comparisons.Comparison)} and
 * {@link ComparisonRange.MergeResult#merge(Comparisons.Comparison)} methods, including cases
 * that produce residual comparisons.
 * </p>
 */
class MergeComparisonRangesTest {

    //
    // ComparisonRange.merge(Comparison)
    //

    /**
     * Merging any comparison into an EMPTY range always succeeds: the result is a range
     * derived from the comparison with no residuals.
     */
    @Test
    void mergeIntoEmptyRangeProducesRangeWithNoResiduals() {
        final Comparisons.Comparison eq = eq(42);
        final ComparisonRange.MergeResult result = ComparisonRange.EMPTY.merge(eq);

        assertThat(result.getResidualComparisons())
                .isEmpty();
        assertThat(result.getComparisonRange().isEquality())
                .isTrue();
        assertThat(result.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
    }

    /**
     * Merging an inequality comparison into an EMPTY range produces an INEQUALITY range.
     */
    @Test
    void mergeInequalityIntoEmptyRangeProducesInequalityRange() {
        final Comparisons.Comparison gt = gt(10);
        final ComparisonRange.MergeResult result = ComparisonRange.EMPTY.merge(gt);

        assertThat(result.getResidualComparisons())
                .isEmpty();
        assertThat(result.getComparisonRange().isInequality())
                .isTrue();
        assertThatThrownBy(() -> result.getComparisonRange().getEqualityComparison())
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("tried to get non-existent equality comparison from ComparisonRange");
        assertThat(result.getComparisonRange().getInequalityComparisons())
                .containsExactly(gt);
    }

    /**
     * A {@code NONE}-type comparison (e.g., NOT_EQUALS) cannot be merged into any range.
     * It is always returned as a residual and the range is unchanged.
     */
    @Test
    void mergeNoneTypeComparisonAlwaysProducesResidual() {
        final Comparisons.Comparison ne = ne(5);

        // into EMPTY
        final ComparisonRange.MergeResult fromEmpty = ComparisonRange.EMPTY.merge(ne);
        assertThat(fromEmpty.getResidualComparisons())
                .containsExactly(ne);
        assertThat(fromEmpty.getComparisonRange().isEmpty())
                .isTrue();

        // into EQUALITY
        final ComparisonRange equalityRange = ComparisonRange.from(eq(5));
        final ComparisonRange.MergeResult fromEquality = equalityRange.merge(ne);
        assertThat(fromEquality.getResidualComparisons())
                .containsExactly(ne);
        assertThat(fromEquality.getComparisonRange())
                .isEqualTo(equalityRange);

        // into INEQUALITY
        final ComparisonRange inequalityRange = ComparisonRange.from(gt(3));
        final ComparisonRange.MergeResult fromInequality = inequalityRange.merge(ne);
        assertThat(fromInequality.getResidualComparisons())
                .containsExactly(ne);
        assertThat(fromInequality.getComparisonRange())
                .isEqualTo(inequalityRange);
    }

    /**
     * Merging the same equality comparison into an EQUALITY range is idempotent. There should be
     * no residuals, and the equality comparison should be combined with the existing one.
     */
    @Test
    void mergingSameEqualityIntoEqualityRangeIsIdempotent() {
        final Comparisons.Comparison eq = eq(7);
        final ComparisonRange range = ComparisonRange.from(eq);
        final ComparisonRange.MergeResult merged = range.merge(eq);

        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange())
                .isEqualTo(range);
    }

    /**
     * Merging a different equality comparison into an EQUALITY range cannot be absorbed.
     * The incoming comparison becomes a residual. Theoretically, this could become a contradiction.
     */
    @Test
    void mergingDifferentEqualityIntoEqualityRangeProducesResidual() {
        final Comparisons.Comparison eq7 = eq(7);
        final Comparisons.Comparison eq9 = eq(9);

        // First, equals 9, then equals 7
        final ComparisonRange range7 = ComparisonRange.from(eq7);
        final ComparisonRange.MergeResult merge9Into7 = range7.merge(eq9);
        assertThat(merge9Into7.getResidualComparisons())
                .containsExactly(eq9);
        assertThat(merge9Into7.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merge9Into7.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq7);

        // Second, equals 7, then equals 9
        final ComparisonRange range9 = ComparisonRange.from(eq9);
        final ComparisonRange.MergeResult merge7Into9 = range9.merge(eq7);
        assertThat(merge7Into9.getResidualComparisons())
                .containsExactly(eq7);
        assertThat(merge7Into9.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merge7Into9.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq9);
    }

    /**
     * Merging an inequality comparison into an EQUALITY range. The inequality cannot be
     * absorbed by the equality; it becomes a residual.
     */
    @Test
    void mergingInequalityIntoEqualityRangeProducesResidual() {
        final Comparisons.Comparison eq = eq(7);
        final Comparisons.Comparison lt = lt(10);
        final ComparisonRange range = ComparisonRange.from(eq);
        final ComparisonRange.MergeResult merged = range.merge(lt);

        assertThat(merged.getResidualComparisons())
                .containsExactly(lt);
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
    }

    /**
     * Merging an equality comparison into an INEQUALITY range: the result becomes a new
     * EQUALITY range (the equality wins), and all existing inequality comparisons are
     * returned as residuals.
     */
    @Test
    void mergingEqualityIntoInequalityRangePromotesToEqualityWithResiduals() {
        final Comparisons.Comparison gt = gt(3);
        final Comparisons.Comparison lt = lt(10);
        final Comparisons.Comparison eq = eq(7);
        final ComparisonRange range = ComparisonRange.fromInequalities(List.of(gt, lt));
        final ComparisonRange.MergeResult merged = range.merge(eq);

        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(gt, lt);
    }

    /**
     * Merging a new (non-duplicate) inequality into an INEQUALITY range absorbs it into the
     * range without producing residuals.
     */
    @Test
    void mergingNewInequalityIntoInequalityRangeExpandsRangeWithNoResiduals() {
        final Comparisons.Comparison gt = gt(3);
        final Comparisons.Comparison lt = lt(10);
        final ComparisonRange range = ComparisonRange.from(gt);
        final ComparisonRange.MergeResult merged = range.merge(lt);

        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange().isInequality())
                .isTrue();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .hasSize(2)
                .containsExactlyInAnyOrder(gt, lt);
    }

    /**
     * Merging a duplicate inequality into an INEQUALITY range is idempotent.
     * There are no residuals, and the range size unchanged.
     */
    @Test
    void mergingDuplicateInequalityIntoInequalityRangeIsIdempotent() {
        final Comparisons.Comparison gt = gt(3);
        final ComparisonRange range = ComparisonRange.from(gt);
        final ComparisonRange.MergeResult merged = range.merge(gt);

        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .hasSize(1);
    }

    /**
     * Merging could result in a range being narrowed. Theoretically, this could
     * reduce the number of inequality comparisons, removing the now extraneous
     * ones.
     */
    @Test
    void mergingInequalityCouldNarrowRange() {
        final Comparisons.Comparison lt10 = lt(10);
        final Comparisons.Comparison gt = gt(3);
        final ComparisonRange merged = ComparisonRange.fromInequalities(List.of(gt, lt10));

        final Comparisons.Comparison lt8 = lt(8);
        final ComparisonRange.MergeResult result = merged.merge(lt8);

        assertThat(result.getResidualComparisons())
                .isEmpty();
        assertThat(result.getComparisonRange().getInequalityComparisons())
                // Could be modified to just gt and lt8 instead
                .containsExactlyInAnyOrder(gt, lt8, lt10);
    }

    //
    // ComparisonRange.MergeResult.merge(Comparison) -- residual accumulation
    //

    /**
     * When the existing MergeResult has no residuals and the new merge also produces no
     * residuals, the combined result has no residuals.
     */
    @Test
    void mergeResultWithNoResidualsStaysCleanWhenNewMergeHasNone() {
        ComparisonRange.MergeResult merged = ComparisonRange.MergeResult.empty();

        final Comparisons.Comparison gt = gt(1);
        merged = merged.merge(gt);

        final Comparisons.Comparison lt = lt(50);
        merged = merged.merge(lt);

        assertThat(merged.getResidualComparisons()).isEmpty();
        assertThat(merged.getComparisonRange().isInequality())
                .isTrue();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lt);
    }

    /**
     * When the existing MergeResult already has residuals and the new merge produces none,
     * the existing residuals are preserved unchanged.
     */
    @Test
    void mergeResultPreservesExistingResidualsWhenNewMergeHasNone() {
        final Comparisons.Comparison ne = ne(99);        // NONE type -> becomes residual
        final Comparisons.Comparison gt = gt(1);
        final Comparisons.Comparison lt = lt(50);

        // Start with EMPTY, merge a NONE comparison -> residuals = [ne]
        ComparisonRange.MergeResult merged = ComparisonRange.MergeResult.empty();
        merged = merged.merge(ne);
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne);

        // Now merge an inequality that CAN be absorbed -> still residuals = [ne]
        merged = merged.merge(gt);
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne);

        // And another absorbable inequality.
        merged = merged.merge(lt);
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne);
        assertThat(merged.getComparisonRange().isInequality())
                .isTrue();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lt);
    }

    /**
     * When the existing MergeResult has residuals AND the new merge also produces residuals,
     * both sets are accumulated together.
     */
    @Test
    void mergeResultAccumulatesResidualsFromBothExistingAndNewMerge() {
        final Comparisons.Comparison ne1 = ne(1);
        final Comparisons.Comparison ne2 = ne(2);
        final Comparisons.Comparison gt = gt(5);
        final Comparisons.Comparison eq = eq(10);

        // Build a MergeResult with an existing residual.
        ComparisonRange.MergeResult merged = ComparisonRange.MergeResult.empty();
        merged = merged.merge(ne1);   // ne1 -> residual; range stays EMPTY
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne1);

        // Now merge an equality so that the range becomes EQUALITY.
        merged = merged.merge(eq);
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne1);
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);

        // Now merge another NONE-type comparison -- it also becomes a residual.
        merged = merged.merge(ne2);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(ne1, ne2);
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);

        // And an inequality -- into an EQUALITY range it also becomes a residual.
        merged = merged.merge(gt);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(ne1, ne2, gt);
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
    }

    /**
     * Calling {@link ComparisonRange.MergeResult#empty()} produces a result with an empty
     * range and no residual comparisons.
     */
    @Test
    void emptyMergeResultHasEmptyRangeAndNoResiduals() {
        final ComparisonRange.MergeResult empty = ComparisonRange.MergeResult.empty();

        assertThat(empty.getComparisonRange().isEmpty())
                .isTrue();
        assertThat(empty.getResidualComparisons())
                .isEmpty();
    }

    //
    // ComparisonRange.merge(ComparisonRange) -- range-level merges
    //

    /**
     * Merging an EMPTY range into any range is a no-op: the original range is returned as-is.
     */
    @Test
    void mergingEmptyComparisonRangeIsNoOp() {
        final ComparisonRange range = ComparisonRange.from(gt(5));
        final ComparisonRange.MergeResult merged = range.merge(ComparisonRange.EMPTY);

        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange())
                .isEqualTo(range);
    }

    /**
     * Merging a non-empty range into an EMPTY range returns the non-empty range.
     */
    @Test
    void mergingIntoEmptyComparisonRangeReturnsOther() {
        final ComparisonRange other = ComparisonRange.from(gt(5));
        final ComparisonRange.MergeResult result = ComparisonRange.EMPTY.merge(other);

        assertThat(result.getResidualComparisons())
                .isEmpty();
        assertThat(result.getComparisonRange())
                .isEqualTo(other);
    }

    /**
     * Merging two INEQUALITY ranges whose comparisons are all compatible accumulates all
     * comparisons with no residuals.
     */
    @Test
    void mergingCompatibleInequalityRangesAccumulatesComparisons() {
        final Comparisons.Comparison gt = gt(3);
        final Comparisons.Comparison lt = lt(20);
        final Comparisons.Comparison gte = gte(5);

        final ComparisonRange range1 = ComparisonRange.fromInequalities(List.of(gt, lt));
        final ComparisonRange range2 = ComparisonRange.from(gte);
        final ComparisonRange.MergeResult result = range1.merge(range2);

        assertThat(result.getResidualComparisons())
                .isEmpty();
        assertThat(result.getComparisonRange().isInequality())
                .isTrue();
        assertThat(result.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lt, gte);
    }

    /**
     * Merging an INEQUALITY range with an EQUALITY range: the equality absorbs the
     * INEQUALITY range, and the prior inequalities appear as residuals.
     */
    @Test
    void mergingInequalityWithEqualityRangePromotesToEqualityWithResiduals() {
        final Comparisons.Comparison gt = gt(3);
        final Comparisons.Comparison lt = lt(20);
        final Comparisons.Comparison eq = eq(10);

        final ComparisonRange inequalityRange = ComparisonRange.fromInequalities(List.of(gt, lt));
        final ComparisonRange equalityRange = ComparisonRange.from(eq);
        final ComparisonRange.MergeResult merged = inequalityRange.merge(equalityRange);

        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(gt, lt);
    }

    /**
     * Full chain test using {@link ComparisonRange.MergeResult#empty()} as the starting
     * point, exercising multiple residuals produced from several incompatible comparisons.
     */
    @Test
    void fullChainWithMultipleResidualsFromIncompatibleComparisons() {
        final var gt = gt(0);
        final var lte = lte(100);
        final var ne1 = ne(10);
        final var ne2 = ne(20);
        final var eq = eq(50);

        ComparisonRange.MergeResult merged = ComparisonRange.MergeResult.empty();

        // Absorb gt -> inequality range [gt], no residuals.
        merged = merged.merge(gt);
        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange().isEquality())
                .isFalse();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactly(gt);

        // Absorb lte -> inequality range [gt, lte], no residuals.
        merged = merged.merge(lte);
        assertThat(merged.getResidualComparisons())
                .isEmpty();
        assertThat(merged.getComparisonRange().isEquality())
                .isFalse();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lte);

        // Merge ne1 (NONE type) -> becomes residual.
        merged = merged.merge(ne1);
        assertThat(merged.getResidualComparisons())
                .containsExactly(ne1);
        assertThat(merged.getComparisonRange().isEquality())
                .isFalse();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lte);

        // Merge ne2 (NONE type) -> also becomes residual; total = 2.
        merged = merged.merge(ne2);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(ne1, ne2);
        assertThat(merged.getComparisonRange().isEquality())
                .isFalse();
        assertThat(merged.getComparisonRange().getInequalityComparisons())
                .containsExactlyInAnyOrder(gt, lte);

        // Merge eq into an INEQUALITY range -> eq wins and the existing inequalities [gt, lte]
        // become residuals, adding 2 more; total = 4.
        merged = merged.merge(eq);
        assertThat(merged.getResidualComparisons())
                .containsExactlyInAnyOrder(ne1, ne2, gt, lte);
        assertThat(merged.getComparisonRange().isEquality())
                .isTrue();
        assertThat(merged.getComparisonRange().getEqualityComparison())
                .isEqualTo(eq);
    }

    //
    // Helpers
    //

    private static Comparisons.Comparison eq(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value);
    }

    private static Comparisons.Comparison ne(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, value);
    }

    private static Comparisons.Comparison gt(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, value);
    }

    private static Comparisons.Comparison gte(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, value);
    }

    private static Comparisons.Comparison lt(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, value);
    }

    private static Comparisons.Comparison lte(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, value);
    }
}
