/*
 * RelativeProbabilityPlanSelectorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class RelativeProbabilityPlanSelectorTest {
    @Test
    void testSelectRandom() {
        RelativeProbabilityPlanSelector classUnderTest = new RelativeProbabilityPlanSelector(Arrays.asList(20, 50, 30), new MockRandom(60));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(1, selectedIndex);
    }

    @Test
    void testSelectRandomZero() {
        RelativeProbabilityPlanSelector classUnderTest = new RelativeProbabilityPlanSelector(Arrays.asList(20, 50, 30), new MockRandom(0));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(0, selectedIndex);
    }

    @Test
    void testSelectRandomOne() {
        RelativeProbabilityPlanSelector classUnderTest = new RelativeProbabilityPlanSelector(Arrays.asList(20, 50, 30), new MockRandom(99));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(2, selectedIndex);
    }

    @Test
    void testSelectRandomOnePriority() {
        RelativeProbabilityPlanSelector classUnderTest = new RelativeProbabilityPlanSelector(Collections.singletonList(100), new MockRandom(50));
        int selectedIndex = classUnderTest.selectPlan(Collections.emptyList());
        Assertions.assertEquals(0, selectedIndex);
    }

    @Test
    void testEmpty() throws Exception {
        Assertions.assertThrows(RecordCoreArgumentException.class, () -> new RelativeProbabilityPlanSelector(Collections.emptyList(), new MockRandom(50)));
    }

    private static class MockRandom extends Random {
        static final long serialVersionUID = 3905348978240129618L;
        private final int mockValue;

        private MockRandom(final int mockValue) {
            this.mockValue = mockValue;
        }

        @Override
        public int nextInt(int limit) {
            return mockValue;
        }
    }
}
