/*
 * PlanHashableTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for the PlanHashable class.
 */
public class PlanHashableTest {
    @Test
    public void oneInt() {
        Assertions.assertEquals(1, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1));
    }

    @Test
    public void oneIntVararg() {
        Assertions.assertEquals(992, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1, null));
    }

    @Test
    public void twoIntVararg() {
        Assertions.assertEquals(994, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1, 2));
    }

    @Test
    public void oneIntList() {
        Assertions.assertEquals(32, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, Collections.singletonList(1)));
    }

    @Test
    public void twoIntList() {
        Assertions.assertEquals(994, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, Arrays.asList(1, 2)));
    }

    @Test
    public void oneIntArrayOfArrays() {
        Assertions.assertEquals(63, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new Integer[][] {{1}}));
    }

    @Test
    public void twoIntArrayOfArrays() {
        Assertions.assertEquals(1025, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new Integer[][] {{1, 2}}));
    }

    @Test
    public void oneIntArrayOfTwoArrays() {
        Assertions.assertEquals(1986, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new Integer[][] {{1}, {2}}));
    }

    @Test
    public void oneIntListOfTwoLists() {
        Assertions.assertEquals(1986, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, Arrays.asList(Collections.singletonList(1), Collections.singletonList(2))));
    }

    @Test
    public void oneIntArrayMixedWithInt() {
        Assertions.assertEquals(1955, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new Integer[] {1}, 2));
    }

    @Test
    public void oneIntListMixedWithInt() {
        Assertions.assertEquals(1955, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, Collections.singletonList(1), 2));
    }

    @Test
    public void onePrimitive() {
        Assertions.assertEquals(1, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1));
    }

    @Test
    public void twoPrimitives() {
        Assertions.assertEquals(994, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1, 2));
    }

    @Test
    public void primitiveArray() {
        Assertions.assertEquals(994, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new int[] {1, 2}));
    }

    @Test
    public void primitiveArrayOfArrays() {
        Assertions.assertEquals(32833, PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, new int[][] {{1, 2}, {3, 4}}));
    }

    @Test
    public void primitiveMixedArrays() {
        Assertions.assertEquals(2018, PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, 1, new int[] {2, 3}));
    }
}
