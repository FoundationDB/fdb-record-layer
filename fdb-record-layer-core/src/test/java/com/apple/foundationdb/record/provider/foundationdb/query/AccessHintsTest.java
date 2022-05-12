/*
 * AccessHintsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.PrimaryAccessHint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test the class AccessHints and interface AccessHint.
 */
class AccessHintsTest {

    @Test
    void testAccessHint() {
        AccessHint indexHint1 = new IndexAccessHint("index1");
        AccessHint indexHint2 = new IndexAccessHint("index1");
        AccessHint indexHint3 = new IndexAccessHint("index2");
        AccessHint primaryHint1 = new PrimaryAccessHint(Key.Expressions.field("name1"));
        AccessHint primaryHint2 = new PrimaryAccessHint(Key.Expressions.field("name1"));
        AccessHint primaryHint3 = new PrimaryAccessHint(Key.Expressions.field("name2"));

        // test equals method
        Assertions.assertEquals(indexHint1, indexHint2);
        Assertions.assertEquals(primaryHint1, primaryHint2);
        Assertions.assertNotEquals(indexHint1, indexHint3);
        Assertions.assertNotEquals(primaryHint1, primaryHint3);
        Assertions.assertNotEquals(indexHint1, primaryHint1);

        // test get type
        Assertions.assertEquals("IndexAccessHint", indexHint1.getAccessHintType());
        Assertions.assertEquals("PrimaryAccessHint", primaryHint1.getAccessHintType());
    }

    @Test
    void testContainsAll() {
        AccessHint indexHint1 = new IndexAccessHint("index1");
        AccessHint indexHint2 = new IndexAccessHint("index2");

        AccessHints hints1 = new AccessHints();
        AccessHints hints2 = new AccessHints(indexHint1, indexHint2);
        AccessHints hints3 = new AccessHints(indexHint1);

        Assertions.assertTrue(hints1.containsAll(hints2));
        Assertions.assertTrue(hints2.containsAll(hints3));
        Assertions.assertFalse(hints3.containsAll(hints2));
        Assertions.assertFalse(hints3.containsAll(hints1));
    }
}
