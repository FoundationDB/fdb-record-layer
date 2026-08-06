/*
 * RTreeDoubleCoordinateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.tuple.Tuple;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Tests that {@code double} coordinates are supported alongside {@code long} ones by
 * {@link RTreeHilbertCurveHelpers} and {@link RTree.Rectangle}.
 */
class RTreeDoubleCoordinateTest {
    @Test
    void hilbertValueOrdersDoublesLikeDoubleCompare() {
        final Double[] values = {-Double.MAX_VALUE, -100.5, -1.0, -0.0, 0.0, 0.5, 1.0, 100.25, Double.MAX_VALUE};
        final Double[] sorted = values.clone();
        Arrays.sort(sorted, Double::compare);

        final BigInteger[] hilbertValues = new BigInteger[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
            hilbertValues[i] = RTreeHilbertCurveHelpers.hilbertValue(new RTree.Point(Tuple.from(sorted[i])));
        }
        for (int i = 1; i < hilbertValues.length; i++) {
            Assertions.assertThat(hilbertValues[i]).isGreaterThan(hilbertValues[i - 1]);
        }
    }

    @Test
    void hilbertValueTreatsNullAsSmallestDouble() {
        final BigInteger nullValue =
                RTreeHilbertCurveHelpers.hilbertValue(new RTree.Point(Tuple.from((Object)null)));
        final BigInteger negativeValue =
                RTreeHilbertCurveHelpers.hilbertValue(new RTree.Point(Tuple.from(-Double.MAX_VALUE)));
        Assertions.assertThat(nullValue).isLessThan(negativeValue);
    }

    @Test
    void areaOfDoubleRectangleIsNotTruncated() {
        final RTree.Rectangle rectangle = new RTree.Rectangle(Tuple.from(0.0, 10.5));
        Assertions.assertThat(rectangle.area()).isEqualTo(BigInteger.TEN);
    }

    @Test
    void plotStringOfDoubleRectangleIsNotTruncated() {
        final RTree.Rectangle rectangle = new RTree.Rectangle(Tuple.from(1.5, 2.5));
        Assertions.assertThat(rectangle.toPlotString()).isEqualTo("1.5,2.5");
    }
}
