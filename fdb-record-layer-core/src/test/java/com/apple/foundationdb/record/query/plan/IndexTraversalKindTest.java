/*
 * IndexTraversalKindTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowForFunction;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the {@link IndexTraversalKind} each kind of {@link IndexScanParameters} reports, and of the kind a
 * {@link RecordQueryIndexPlan} takes from them.
 */
class IndexTraversalKindTest {
    static Stream<Arguments> scanParameters() {
        return Stream.of(
                Arguments.of("by value", IndexScanComparisons.byValue(), IndexTraversalKind.BY_VALUE),
                Arguments.of("by value over-scan",
                        IndexScanComparisons.byValue(null, IndexScanType.BY_VALUE_OVER_SCAN),
                        IndexTraversalKind.BY_VALUE),
                Arguments.of("by group", scanBy(IndexScanType.BY_GROUP), IndexTraversalKind.BY_VALUE),
                Arguments.of("by rank", scanBy(IndexScanType.BY_RANK), IndexTraversalKind.RANKED_SET),
                Arguments.of("by time window", timeWindowScan(), IndexTraversalKind.RANKED_SET),
                Arguments.of("by text token", scanBy(IndexScanType.BY_TEXT_TOKEN), IndexTraversalKind.INVERTED),
                Arguments.of("multidimensional", multidimensionalScan(), IndexTraversalKind.R_TREE),
                //
                // A vector scan cannot say: which structure it walks depends on the engine backing the index, which only
                // VectorIndexPlan knows.
                //
                Arguments.of("by distance", vectorScan(), IndexTraversalKind.UNKNOWN),
                Arguments.of("scan type defined elsewhere", scanBy(new IndexScanType("BY_SOMETHING_ELSE")),
                        IndexTraversalKind.UNKNOWN));
    }

    @ParameterizedTest(name = "scanParametersReportTheirTraversalKind[{0}]")
    @MethodSource("scanParameters")
    @SuppressWarnings("unused") // the name only names the test case
    void scanParametersReportTheirTraversalKind(@Nonnull final String name,
                                                @Nonnull final IndexScanParameters scanParameters,
                                                @Nonnull final IndexTraversalKind expectedKind) {
        assertThat(scanParameters.getIndexTraversalKind()).isEqualTo(expectedKind);
    }

    @ParameterizedTest(name = "indexPlanTraversalKindFollowsScanParameters[{0}]")
    @MethodSource("scanParameters")
    @SuppressWarnings("unused") // the name only names the test case
    void indexPlanTraversalKindFollowsScanParameters(@Nonnull final String name,
                                                     @Nonnull final IndexScanParameters scanParameters,
                                                     @Nonnull final IndexTraversalKind expectedKind) {
        assertThat(indexPlan(scanParameters).getIndexTraversalKind()).isEqualTo(expectedKind);
    }

    @Test
    void kindsDefinedElsewhereAreDistinctFromTheOnesDefinedHere() {
        final IndexTraversalKind spaceFillingCurve = new IndexTraversalKind("SPACE_FILLING_CURVE");
        assertThat(spaceFillingCurve).isEqualTo(new IndexTraversalKind("SPACE_FILLING_CURVE"))
                .hasSameHashCodeAs(new IndexTraversalKind("SPACE_FILLING_CURVE"))
                .isNotEqualTo(IndexTraversalKind.BY_VALUE)
                .isNotEqualTo(IndexTraversalKind.UNKNOWN);
        assertThat(spaceFillingCurve.name()).isEqualTo("SPACE_FILLING_CURVE");
        assertThat(spaceFillingCurve).hasToString("SPACE_FILLING_CURVE");
    }

    @Nonnull
    private static IndexScanParameters scanBy(@Nonnull final IndexScanType scanType) {
        return new IndexScanComparisons(scanType, ScanComparisons.EMPTY);
    }

    @Nonnull
    private static IndexScanParameters timeWindowScan() {
        return new TimeWindowScanComparisons(new TimeWindowForFunction(1, 42L, null, null), ScanComparisons.EMPTY);
    }

    @Nonnull
    private static IndexScanParameters multidimensionalScan() {
        return MultidimensionalIndexScanComparisons.byValue(ScanComparisons.EMPTY,
                ImmutableList.of(ScanComparisons.EMPTY), ScanComparisons.EMPTY);
    }

    @Nonnull
    private static IndexScanParameters vectorScan() {
        final int numDimensions = 128;
        final Comparisons.DistanceRankValueComparison distanceRankValueComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 64, numDimensions),
                                new DoubleRealVector(new double[numDimensions])),
                        new LiteralValue<>(10), null, null);
        return VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY, distanceRankValueComparison);
    }

    @Nonnull
    private static RecordQueryIndexPlan indexPlan(@Nonnull final IndexScanParameters scanParameters) {
        return new RecordQueryIndexPlan("an_index",
                null,
                scanParameters,
                IndexFetchMethod.SCAN_AND_FETCH,
                FetchIndexRecords.PRIMARY_KEY,
                false,
                false,
                Optional.empty(),
                new Type.Any(),
                QueryPlanConstraint.noConstraint());
    }
}
