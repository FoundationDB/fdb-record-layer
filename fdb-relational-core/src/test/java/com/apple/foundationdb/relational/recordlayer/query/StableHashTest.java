/*
 * StableHashTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StableHashTest {
    @Test
    public void testEmpty() {
        PreparedStatementParameters classUnderTest = new PreparedStatementParameters();
        Assertions.assertThat(classUnderTest.stableHash()).isEqualTo(-1626848275);
    }

    @Test
    public void testNull() {
        PreparedStatementParameters classUnderTest = new PreparedStatementParameters(null, null);
        Assertions.assertThat(classUnderTest.stableHash()).isEqualTo(-1626848275);
    }

    @Test
    public void testValue1SameValue2() {
        Map<Integer, Object> m1 = Map.of(1, "Hello");
        Map<String, Object> m2 = Map.of("1", "Hello");
        PreparedStatementParameters p1 = new PreparedStatementParameters(m1, null);
        PreparedStatementParameters p2 = new PreparedStatementParameters(null, m2);
        Assertions.assertThat(p1.stableHash()).isNotEqualTo(p2.stableHash());
    }

    @Test
    public void testManyValuesDifferentInsertionOrder() {
        Map<Integer, Object> m1 = Map.of(1, "Hello", 2, "World", 3, "!");
        Map<Integer, Object> m2 = Map.of(2, "World", 3, "!", 1, "Hello");
        Map<String, Object> m3 = Map.of("1", "Hello", "2", "World", "3", "!");
        Map<String, Object> m4 = Map.of("2", "World", "3", "!", "1", "Hello");
        PreparedStatementParameters p1 = new PreparedStatementParameters(m1, m3);
        PreparedStatementParameters p2 = new PreparedStatementParameters(m2, m4);
        Assertions.assertThat(p1.stableHash()).isEqualTo(p2.stableHash());
    }

    @Test
    public void testManyValuesDifferentMapImplementation() {
        Map<Integer, Object> m1 = Map.of(1, "Hello", 2, "World", 3, "!");
        Map<Integer, Object> m2 = new HashMap(m1);
        Map<String, Object> m3 = Map.of("1", "Hello", "2", "World", "3", "!");
        Map<String, Object> m4 = new HashMap<>(m3);
        PreparedStatementParameters p1 = new PreparedStatementParameters(m1, m3);
        PreparedStatementParameters p2 = new PreparedStatementParameters(m2, m4);
        Assertions.assertThat(p1.stableHash()).isEqualTo(p2.stableHash());
    }
}
