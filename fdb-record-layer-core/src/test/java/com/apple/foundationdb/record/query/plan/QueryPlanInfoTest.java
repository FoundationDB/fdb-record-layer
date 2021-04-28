/*
 * QueryPlanInfoTest.java
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

package com.apple.foundationdb.record.query.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the QueryPlanInfo class.
 */
public class QueryPlanInfoTest {
    private static final QueryPlanInfo.QueryPlanInfoKey<String> KEY_STR = new QueryPlanInfo.QueryPlanInfoKey<>("S");
    private static final QueryPlanInfo.QueryPlanInfoKey<Integer> KEY_INT = new QueryPlanInfo.QueryPlanInfoKey<>("I");

    private QueryPlanInfo classUnderTest;

    @BeforeEach
    void setup() throws Exception {
        classUnderTest = new QueryPlanInfo();
    }

    @Test
    void testAddKey() throws Exception {
        Assertions.assertFalse(classUnderTest.containsKey(KEY_STR));
        classUnderTest.put(KEY_STR, "Value");
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertEquals("Value", classUnderTest.get(KEY_STR));
    }

    @Test
    void testAddTwoKeys() throws Exception {
        Assertions.assertFalse(classUnderTest.containsKey(KEY_STR));
        Assertions.assertFalse(classUnderTest.containsKey(KEY_INT));
        classUnderTest.put(KEY_STR, "Value");
        classUnderTest.put(KEY_INT, 2);
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertTrue(classUnderTest.containsKey(KEY_INT));
        Assertions.assertEquals("Value", classUnderTest.get(KEY_STR));
        Assertions.assertEquals(2, classUnderTest.get(KEY_INT));
    }

    @Test
    void testNullValue() throws Exception {
        classUnderTest.put(KEY_STR, null);
        classUnderTest.put(KEY_INT, null);
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertTrue(classUnderTest.containsKey(KEY_INT));
        Assertions.assertNull(classUnderTest.get(KEY_STR));
        Assertions.assertNull(classUnderTest.get(KEY_INT));
    }

}
