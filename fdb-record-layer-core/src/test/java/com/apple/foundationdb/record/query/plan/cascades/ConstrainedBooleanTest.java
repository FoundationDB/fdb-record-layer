/*
 * ConstrainedBooleanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConstrainedBooleanTest {
    @Test
    void composeWithOtherThisTrueOtherFalse() {
        final ConstrainedBoolean result = ConstrainedBoolean.alwaysTrue().composeWithOther(ConstrainedBoolean.falseValue());
        assertTrue(result.isFalse());
    }

    @Test
    void composeWithOtherThisFalseOtherTrue() {
        final ConstrainedBoolean result = ConstrainedBoolean.falseValue().composeWithOther(ConstrainedBoolean.alwaysTrue());
        assertTrue(result.isFalse());
    }

    @Test
    void composeWithOtherCombinesConstraints() {
        final QueryPlanConstraint c1 = QueryPlanConstraint.ofPredicate(ConstantPredicate.FALSE);
        final QueryPlanConstraint c2 = QueryPlanConstraint.ofPredicate(ConstantPredicate.NULL);
        final ConstrainedBoolean a = ConstrainedBoolean.trueWithConstraint(c1);
        final ConstrainedBoolean b = ConstrainedBoolean.trueWithConstraint(c2);

        final ConstrainedBoolean result = a.composeWithOther(b);
        assertTrue(result.isTrue());
        assertTrue(result.getConstraint().isConstrained());
    }

    @Test
    void equalsAndHashCodeUseWrappedBooleanAndConstraint() {
        final var constraint1 = QueryPlanConstraint.ofPredicate(ConstantPredicate.TRUE);
        final var constraint2 = QueryPlanConstraint.ofPredicate(ConstantPredicate.FALSE);

        assertEquals(ConstrainedBoolean.alwaysTrue(), ConstrainedBoolean.alwaysTrue());
        assertEquals(ConstrainedBoolean.alwaysTrue().hashCode(), ConstrainedBoolean.alwaysTrue().hashCode());
        assertEquals(ConstrainedBoolean.falseValue(), ConstrainedBoolean.falseValue());
        assertEquals(ConstrainedBoolean.falseValue().hashCode(), ConstrainedBoolean.falseValue().hashCode());

        assertEquals(ConstrainedBoolean.trueWithConstraint(constraint1),
                ConstrainedBoolean.trueWithConstraint(constraint1));
        assertEquals(ConstrainedBoolean.trueWithConstraint(constraint1).hashCode(),
                ConstrainedBoolean.trueWithConstraint(constraint1).hashCode());

        assertNotEquals(ConstrainedBoolean.alwaysTrue(), ConstrainedBoolean.falseValue());
        assertNotEquals(ConstrainedBoolean.alwaysTrue().hashCode(), ConstrainedBoolean.falseValue().hashCode());

        assertNotEquals(ConstrainedBoolean.trueWithConstraint(constraint1),
                ConstrainedBoolean.trueWithConstraint(constraint2));
        assertNotEquals(ConstrainedBoolean.trueWithConstraint(constraint1),
                ConstrainedBoolean.trueWithConstraint(constraint2));
    }
}
