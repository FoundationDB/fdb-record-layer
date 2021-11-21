/*
 * OrderingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.jupiter.api.Test;

class OrderingTest {
    @Test
    void testOrdering() {
        final KeyExpression a = Key.Expressions.field("a");
        final KeyExpression b = Key.Expressions.field("b");
        final KeyExpression c = Key.Expressions.field("c");

        final var requiredOrdering = ImmutableList.of(a, b, c);

        final var providedOrdering = new Ordering(
                ImmutableSetMultimap.of(b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(KeyPart.of(a, ComparisonRange.Type.INEQUALITY),
                        KeyPart.of(c, ComparisonRange.Type.INEQUALITY)));

        System.out.println(providedOrdering.satisfiesRequiredOrdering(requiredOrdering, ImmutableSet.of()));
    }

    @Test
    void testOrdering1() {
        final KeyExpression a = Key.Expressions.field("a");
        final KeyExpression b = Key.Expressions.field("b");
        final KeyExpression c = Key.Expressions.field("c");

        final var requiredOrdering = ImmutableList.of(a, b, c);

        final var providedOrdering = new Ordering(
                ImmutableSetMultimap.of(a, new Comparisons.NullComparison(Comparisons.Type.IS_NULL), b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(KeyPart.of(c, ComparisonRange.Type.INEQUALITY)));

        System.out.println(providedOrdering.satisfiesRequiredOrdering(requiredOrdering, ImmutableSet.of()));
    }
}
