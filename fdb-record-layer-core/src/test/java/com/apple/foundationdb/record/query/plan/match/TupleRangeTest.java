/*
 * TupleRangeTest.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link com.apple.foundationdb.record.TupleRange}.
 */
public class TupleRangeTest {

    @Test
    public void testRangeImplication() {
        final var largerRange = CompileTimeRange.newBuilder();
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));


        final var smallerRange = CompileTimeRange.newBuilder();
        smallerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        smallerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));

        Assertions.assertTrue(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()));
    }

    @Test
    public void testRangeImplication2() {
        final var largerRange = CompileTimeRange.newBuilder();
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "aaa"));
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "ccc"));

        final var smallerRange = CompileTimeRange.newBuilder();
        smallerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));

        Assertions.assertTrue(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()));

        final var otherRange = CompileTimeRange.newBuilder();
        otherRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "z"));

        Assertions.assertFalse(largerRange.build().orElseThrow().implies(otherRange.build().orElseThrow()));
    }

    @Test
    public void testRangeImplication3() {
        final var largerRange = CompileTimeRange.newBuilder();
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "ccc"));
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));
        largerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "aaa"));

        final var smallerRange = CompileTimeRange.newBuilder();
        smallerRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));

        Assertions.assertTrue(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()));

        final var otherRange = CompileTimeRange.newBuilder();
        otherRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "z"));

        Assertions.assertFalse(largerRange.build().orElseThrow().implies(otherRange.build().orElseThrow()));
    }

    @Test
    public void testRangeImplication4() {
        final var emptyRangeBuilder = CompileTimeRange.newBuilder();
        emptyRangeBuilder.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0));
        emptyRangeBuilder.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0));
        final var emptyRange = CompileTimeRange.empty();

        Assertions.assertTrue(emptyRange.isEmpty());

        final var zeroValueRange = CompileTimeRange.newBuilder();
        zeroValueRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0));

        Assertions.assertFalse(emptyRange.implies(zeroValueRange.build().orElseThrow()));

        final var largeRange = CompileTimeRange.newBuilder();
        largeRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        largeRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

        Assertions.assertFalse(emptyRange.implies(largeRange.build().orElseThrow()));
    }

    @Test
    public void creatingInvalidRangesEndUpWithEmptyRange() {
        final var invalidRange = CompileTimeRange.newBuilder();
        invalidRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 30));
        invalidRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 20));
        Assertions.assertTrue(invalidRange.build().isEmpty());

        invalidRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));
        Assertions.assertTrue(invalidRange.build().isEmpty());

        invalidRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 10));
        Assertions.assertTrue(invalidRange.build().isEmpty());
    }
}
