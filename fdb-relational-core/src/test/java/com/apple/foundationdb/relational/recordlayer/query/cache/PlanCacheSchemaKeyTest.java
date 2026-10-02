/*
 * PlanCacheSchemaKeyTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PlanCacheSchemaKeyTest {

    @Test
    void ofCollectionProducesSortedKey() {
        final List<String> names = Arrays.asList("schema_b", "schema_a", "schema_c");
        final PlanCacheSchemaKey key = PlanCacheSchemaKey.of(names);
        assertEquals(List.of("schema_a", "schema_b", "schema_c"), List.copyOf(key.getSchemaNames()));
    }

    @Test
    void ofCollectionEqualsSingletonOfForSingleElement() {
        final PlanCacheSchemaKey fromCollection = PlanCacheSchemaKey.of(List.of("my_schema"));
        final PlanCacheSchemaKey fromSingle = PlanCacheSchemaKey.of("my_schema");
        assertEquals(fromSingle, fromCollection);
        assertEquals(fromSingle.hashCode(), fromCollection.hashCode());
    }

    @Test
    void ofCollectionOrderIndependent() {
        final PlanCacheSchemaKey k1 = PlanCacheSchemaKey.of(Arrays.asList("alpha", "beta"));
        final PlanCacheSchemaKey k2 = PlanCacheSchemaKey.of(Arrays.asList("beta", "alpha"));
        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
    }

    @Test
    void toStringSingleSchema() {
        assertEquals("my_schema", PlanCacheSchemaKey.of("my_schema").toString());
    }

    @Test
    void toStringMultiSchemaJoinedWithPipe() {
        final PlanCacheSchemaKey key = PlanCacheSchemaKey.of(Arrays.asList("schema_b", "schema_a"));
        assertEquals("schema_a|schema_b", key.toString());
    }

    @Test
    void differentSchemasDontMatch() {
        assertNotEquals(PlanCacheSchemaKey.of("schema_x"), PlanCacheSchemaKey.of("schema_y"));
    }
}
