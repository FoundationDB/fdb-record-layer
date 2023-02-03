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
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link com.apple.foundationdb.record.TupleRange}.
 */
public class TupleRangeTest {

    @Test
    public void testRangeImplication() {
        final var largerRange = CompileTimeEvaluableRange.newBuilder();
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));


        final var smallerRange = CompileTimeEvaluableRange.newBuilder();
        smallerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        smallerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));

        Assertions.assertEquals(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.TRUE);
    }

    @Test
    public void testRangeImplication2() {
        final var largerRange = CompileTimeEvaluableRange.newBuilder();
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "aaa"));
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "ccc"));

        final var smallerRange = CompileTimeEvaluableRange.newBuilder();
        smallerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));

        Assertions.assertEquals(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.TRUE);

        final var otherRange = CompileTimeEvaluableRange.newBuilder();
        otherRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "z"));

        Assertions.assertNotEquals(largerRange.build().orElseThrow().implies(otherRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.TRUE);
    }

    @Test
    public void testRangeImplication3() {
        final var largerRange = CompileTimeEvaluableRange.newBuilder();
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "ccc"));
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));
        largerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "aaa"));

        final var smallerRange = CompileTimeEvaluableRange.newBuilder();
        smallerRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bbb"));

        Assertions.assertEquals(largerRange.build().orElseThrow().implies(smallerRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.TRUE);

        final var otherRange = CompileTimeEvaluableRange.newBuilder();
        otherRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "z"));

        Assertions.assertNotEquals(largerRange.build().orElseThrow().implies(otherRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.TRUE);
    }

    @Test
    public void testRangeImplication4() {
        final var emptyRangeBuilder = CompileTimeEvaluableRange.newBuilder();
        emptyRangeBuilder.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 0));
        emptyRangeBuilder.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 0));
        final var emptyRange = CompileTimeEvaluableRange.empty();

        Assertions.assertEquals(emptyRange.isEmpty(), CompileTimeEvaluableRange.EvalResult.TRUE);

        final var zeroValueRange = CompileTimeEvaluableRange.newBuilder();
        zeroValueRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0));

        Assertions.assertEquals(emptyRange.implies(zeroValueRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.FALSE);

        final var largeRange = CompileTimeEvaluableRange.newBuilder();
        largeRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 0));
        largeRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

        Assertions.assertEquals(emptyRange.implies(largeRange.build().orElseThrow()), CompileTimeEvaluableRange.EvalResult.FALSE);
    }

    @Test
    public void creatingInvalidRangesEndUpWithEmptyRange() {
        final var invalidRange = CompileTimeEvaluableRange.newBuilder();
        invalidRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 30));
        invalidRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 20));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), CompileTimeEvaluableRange.EvalResult.TRUE);

        invalidRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), CompileTimeEvaluableRange.EvalResult.TRUE);

        invalidRange.addMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 10));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), CompileTimeEvaluableRange.EvalResult.TRUE);
    }
}
