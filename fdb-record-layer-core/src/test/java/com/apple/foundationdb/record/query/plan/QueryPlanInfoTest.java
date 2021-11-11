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

    @BeforeEach
    void setup() throws Exception {
    }

    @Test
    void testAddKey() throws Exception {
        QueryPlanInfo classUnderTest = QueryPlanInfo.newBuilder()
                .put(KEY_STR, "Value")
                .build();
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertEquals("Value", classUnderTest.get(KEY_STR));
    }

    @Test
    void testAddTwoKeys() throws Exception {
        QueryPlanInfo classUnderTest = QueryPlanInfo.newBuilder()
                .put(KEY_STR, "Value")
                .put(KEY_INT, 2)
                .build();
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertTrue(classUnderTest.containsKey(KEY_INT));
        Assertions.assertEquals("Value", classUnderTest.get(KEY_STR));
        Assertions.assertEquals(2, classUnderTest.get(KEY_INT));
    }

    @Test
    void testNullValue() throws Exception {
        QueryPlanInfo classUnderTest = QueryPlanInfo.newBuilder()
                .put(KEY_STR, null)
                .put(KEY_INT, null)
                .build();
        Assertions.assertTrue(classUnderTest.containsKey(KEY_STR));
        Assertions.assertTrue(classUnderTest.containsKey(KEY_INT));
        Assertions.assertNull(classUnderTest.get(KEY_STR));
        Assertions.assertNull(classUnderTest.get(KEY_INT));
    }

    @Test
    void testBuildFrom() throws Exception {
        QueryPlanInfo classUnderTest = QueryPlanInfo.newBuilder()
                .put(KEY_STR, "Value")
                .build();
        classUnderTest = classUnderTest.toBuilder()
                .put(KEY_INT, 2)
                .build();

        Assertions.assertEquals("Value", classUnderTest.get(KEY_STR));
        Assertions.assertEquals(2, classUnderTest.get(KEY_INT));
    }
}
