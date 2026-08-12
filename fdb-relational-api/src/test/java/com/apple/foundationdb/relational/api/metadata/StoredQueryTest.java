/*
 * StoredQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class StoredQueryTest {

    @Test
    void twoArgConstructorDefaultsSignatureToEmpty() {
        final var storedQuery = new StoredQuery("select * from t1", List.of("f1", "f2"));
        Assertions.assertEquals("select * from t1", storedQuery.getQuery());
        Assertions.assertEquals(List.of("f1", "f2"), storedQuery.getTempFunctions());
        Assertions.assertEquals("", storedQuery.getSignature());
    }

    @Test
    void threeArgConstructorRetainsSignature() {
        final var storedQuery = new StoredQuery("SELECT id FROM f1(?param_b)", List.of("f1 body"), "param_a:LONG,param_b:NULL");
        Assertions.assertEquals("SELECT id FROM f1(?param_b)", storedQuery.getQuery());
        Assertions.assertEquals(List.of("f1 body"), storedQuery.getTempFunctions());
        Assertions.assertEquals("param_a:LONG,param_b:NULL", storedQuery.getSignature());
    }
}
