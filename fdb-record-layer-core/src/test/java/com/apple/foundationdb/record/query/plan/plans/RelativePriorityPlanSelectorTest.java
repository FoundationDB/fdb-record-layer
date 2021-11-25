/*
 * RelativePriorityTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.google.common.base.VerifyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class RelativePriorityPlanSelectorTest {
    @Test
    void testSelectRandom() {
        RelativePriorityPlanSelector classUnderTest = new RelativePriorityPlanSelector(Arrays.asList(0.2, 0.5, 0.3), new MockRandom(0.6));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(1, selectedIndex);
    }

    @Test
    void testSelectRandomZero() {
        RelativePriorityPlanSelector classUnderTest = new RelativePriorityPlanSelector(Arrays.asList(0.2, 0.5, 0.3), new MockRandom(0.0));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(0, selectedIndex);
    }

    @Test
    void testSelectRandomOne() {
        RelativePriorityPlanSelector classUnderTest = new RelativePriorityPlanSelector(Arrays.asList(0.2, 0.5, 0.3), new MockRandom(0.99999));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(2, selectedIndex);
    }

    @Test
    void testSelectRandomOnePriority() {
        RelativePriorityPlanSelector classUnderTest = new RelativePriorityPlanSelector(Collections.singletonList(1.0), new MockRandom(0.5));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(0, selectedIndex);
    }

    @Test
    void testEmpty() throws Exception {
        Assertions.assertThrows(VerifyException.class, () -> new RelativePriorityPlanSelector(Collections.emptyList(), new MockRandom(0.5)));
    }

    private static class MockRandom extends Random {
        static final long serialVersionUID = 3905348978240129618L;
        private final double mockValue;

        private MockRandom(final double mockValue) {
            this.mockValue = mockValue;
        }

        @Override
        public double nextDouble() {
            return mockValue;
        }
    }
}
