/*
 * RangeConstraintsTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.Proposition;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.EQUALS;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.GREATER_THAN;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.GREATER_THAN_OR_EQUALS;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.IS_NULL;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.LESS_THAN;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.NOT_NULL;
import static com.apple.foundationdb.record.query.expressions.Comparisons.Type.STARTS_WITH;
import static org.apache.commons.lang3.tuple.Pair.of;

/**
 * Unit tests for {@link RangeConstraints}.
 */
@SuppressWarnings("unchecked")
public class RangeConstraintsTest {

    @Test
    public void testRangeImplications() {
        final var largerRange = range(of(GREATER_THAN, "aaa"), of(LESS_THAN, "ccc"));
        final var smallerRange = range(of(EQUALS, "bbb"));
        shouldImply(largerRange, smallerRange);
        shouldNotImply(smallerRange, largerRange);
        shouldImply(largerRange, largerRange);
        shouldImply(smallerRange, smallerRange);
    }

    @Test
    public void testRangeImplications2() {
        final var largerRange = range(of(GREATER_THAN_OR_EQUALS, "aaa"), of(LESS_THAN, "ccc"));
        final var smallerRange = range(of(EQUALS, "aaa"));
        shouldImply(largerRange, smallerRange);
        shouldNotImply(smallerRange, largerRange);
        shouldImply(largerRange, largerRange);
        shouldImply(smallerRange, smallerRange);
    }

    @Test
    public void testRangeImplications3() {
        final var largerRange = RangeConstraints.emptyRange();
        final var smallerRange = range(of(EQUALS, "aaa"));
        shouldNotImply(largerRange, smallerRange);
        shouldImply(largerRange, largerRange);
        shouldNotImply(smallerRange, largerRange);
    }

    @Test
    public void testRangeImplications4() {
        final var unknownRange = range(of(GREATER_THAN, "aaa"), of(LESS_THAN, "ccc"), of(STARTS_WITH, "bbb")); // not compile-time
        final var otherRange = range(of(EQUALS, "bbb"));
        implicationUnknown(unknownRange, otherRange);
        implicationUnknown(otherRange, unknownRange);
        implicationUnknown(unknownRange, unknownRange);
    }

    @Test
    public void testRangeEmptyUnknownIfNonCompileTime() {
        final var range = range(of(STARTS_WITH, "bbb")); // not compile-time
        Assertions.assertEquals(Proposition.UNKNOWN, range.isEmpty());
    }

    @Test
    public void testRangeNullIsSmallerThanOtherValues() {
        final var isNullRangeBuilder = RangeConstraints.newBuilder();
        isNullRangeBuilder.addComparisonMaybe(new Comparisons.NullComparison(IS_NULL));
        final var isNullRange = isNullRangeBuilder.build().orElseThrow();
        final var isNotNullRangeBuilder = RangeConstraints.newBuilder();
        isNotNullRangeBuilder.addComparisonMaybe(new Comparisons.NullComparison(NOT_NULL));
        final var isNotNullRange = isNotNullRangeBuilder.build().orElseThrow();
        Assertions.assertEquals(Proposition.FALSE, isNullRange.encloses(isNotNullRange));
        Assertions.assertEquals(Proposition.FALSE, isNotNullRange.encloses(isNullRange));
    }

    @Test
    public void creatingInvalidRangesEndUpWithEmptyRange() {
        final var invalidRange = RangeConstraints.newBuilder();
        invalidRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 30));
        invalidRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 20));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), Proposition.TRUE);

        invalidRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), Proposition.TRUE);

        invalidRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 10));
        Assertions.assertEquals(invalidRange.build().get().isEmpty(), Proposition.TRUE);
    }

    @Nonnull
    private static RangeConstraints range(@Nonnull Pair<Comparisons.Type, Object>... comparisons) {
        final var result = RangeConstraints.newBuilder();
        for (final var comparison : comparisons) {
            Assertions.assertTrue(result.addComparisonMaybe(new Comparisons.SimpleComparison(comparison.getKey(), comparison.getValue())));
        }
        return result.build().orElseThrow();
    }

    private static void shouldImply(@Nonnull RangeConstraints left, @Nonnull RangeConstraints right) {
        Assertions.assertEquals(Proposition.TRUE, left.encloses(right));
    }

    private static void shouldNotImply(@Nonnull RangeConstraints left, @Nonnull RangeConstraints right) {
        Assertions.assertEquals(Proposition.FALSE, left.encloses(right));
    }

    private static void implicationUnknown(@Nonnull RangeConstraints left, @Nonnull RangeConstraints right) {
        Assertions.assertEquals(Proposition.UNKNOWN, left.encloses(right));
    }
}
