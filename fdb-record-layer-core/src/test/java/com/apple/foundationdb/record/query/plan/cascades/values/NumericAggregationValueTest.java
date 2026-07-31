/*
 * NumericAggregationValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NumericAggregationValue}.
 */
class NumericAggregationValueTest {

    private static final LiteralValue<Integer> INT_CHILD =
            new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 42);

    @Test
    void differentAggregationFunctionsHaveDifferentHashCodes() {
        final NumericAggregationValue sum =
                new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, INT_CHILD);
        final NumericAggregationValue min =
                new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, INT_CHILD);
        final NumericAggregationValue max =
                new NumericAggregationValue.Max(NumericAggregationValue.PhysicalOperator.MAX_I, INT_CHILD);
        final NumericAggregationValue avg =
                new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, INT_CHILD);

        Assertions.assertNotEquals(sum.hashCode(), min.hashCode(),
                "SUM and MIN over the same child must not collide on hashCode");
        Assertions.assertNotEquals(sum.hashCode(), max.hashCode(),
                "SUM and MAX over the same child must not collide on hashCode");
        Assertions.assertNotEquals(sum.hashCode(), avg.hashCode(),
                "SUM and AVG over the same child must not collide on hashCode");
        Assertions.assertNotEquals(min.hashCode(), max.hashCode(),
                "MIN and MAX over the same child must not collide on hashCode");
        Assertions.assertNotEquals(min.hashCode(), avg.hashCode(),
                "MIN and AVG over the same child must not collide on hashCode");
        Assertions.assertNotEquals(max.hashCode(), avg.hashCode(),
                "MAX and AVG over the same child must not collide on hashCode");
    }
}
