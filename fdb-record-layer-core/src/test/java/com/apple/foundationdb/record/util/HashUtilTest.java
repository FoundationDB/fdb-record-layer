/*
 * HashUtilTest.java
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

package com.apple.foundationdb.record.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.apple.foundationdb.record.QueryHashable.QueryHashKind.STRUCTURAL_WITHOUT_LITERALS;

/**
 * Tests for the HashUtil class.
 */
public class HashUtilTest {
    @Test
    public void oneInt() throws Exception {
        Assertions.assertEquals(1, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Integer.valueOf(1)));
    }

    @Test
    public void oneIntVararg() throws Exception {
        Assertions.assertEquals(992, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Integer.valueOf(1), null));
    }

    @Test
    public void twoIntVararg() throws Exception {
        Assertions.assertEquals(994, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Integer.valueOf(1), Integer.valueOf(2)));
    }

    @Test
    public void oneIntList() throws Exception {
        Assertions.assertEquals(32, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Collections.singletonList(Integer.valueOf(1))));
    }

    @Test
    public void twoIntList() throws Exception {
        Assertions.assertEquals(994, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Arrays.asList(Integer.valueOf(1), Integer.valueOf(2))));
    }

    @Test
    public void oneIntArrayOfArrays() throws Exception {
        Assertions.assertEquals(63, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, (Object)(new Integer[][] {{Integer.valueOf(1)}})));
    }

    @Test
    public void twoIntArrayOfArrays() throws Exception {
        Assertions.assertEquals(1025, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, (Object)(new Integer[][] {{Integer.valueOf(1), Integer.valueOf(2)}})));
    }

    @Test
    public void oneIntArrayOfTwoArrays() throws Exception {
        Assertions.assertEquals(1986, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, (Object)(new Integer[][] {{Integer.valueOf(1)}, {Integer.valueOf(2)}})));
    }

    @Test
    public void oneIntListOfTwoLists() throws Exception {
        Assertions.assertEquals(1986, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Arrays.asList(Collections.singletonList(Integer.valueOf(1)), Collections.singletonList(Integer.valueOf(2)))));
    }

    @Test
    public void oneIntArrayMixedWithInt() throws Exception {
        Assertions.assertEquals(1955, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, new Integer[] {Integer.valueOf(1)}, Integer.valueOf(2)));
    }

    @Test
    public void oneIntListMixedWithInt() throws Exception {
        Assertions.assertEquals(1955, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, Collections.singletonList(Integer.valueOf(1)), Integer.valueOf(2)));
    }

    @Test
    public void onePrimitive() throws Exception {
        Assertions.assertEquals(1, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, 1));
    }

    @Test
    public void twoPrimitives() throws Exception {
        Assertions.assertEquals(994, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, 1, 2));
    }

    @Test
    public void primitiveArray() throws Exception {
        Assertions.assertEquals(994, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, new int[] {1, 2}));
    }

    @Test
    public void primitiveArrayOfArrays() throws Exception {
        Assertions.assertEquals(32833, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, (Object)(new int[][] {{1, 2}, {3, 4}})));
    }

    @Test
    public void primitiveMixedArrays() throws Exception {
        Assertions.assertEquals(2018, HashUtils.queryHash(STRUCTURAL_WITHOUT_LITERALS, 1, new int[] {2, 3}));
    }
}
