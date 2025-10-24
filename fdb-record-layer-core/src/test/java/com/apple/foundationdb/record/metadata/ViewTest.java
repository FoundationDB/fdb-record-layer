/*
 * ViewTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaDataProto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link View}.
 */
public class ViewTest {

    @Test
    public void testConstructorAndGetters() {
        final String viewName = "test_view";
        final String definition = "SELECT * FROM table1";

        final View view = new View(viewName, definition);

        assertEquals(viewName, view.getName());
        assertEquals(definition, view.getDefinition());
    }

    @Test
    public void testEqualsAndHashCode() {
        final View view1 = new View("view1", "SELECT * FROM t1");
        final View view2 = new View("view1", "SELECT * FROM t1");
        final View view3 = new View("view2", "SELECT * FROM t1");
        final View view4 = new View("view1", "SELECT * FROM t2");

        // Test equality
        assertEquals(view1, view2);
        assertEquals(view1.hashCode(), view2.hashCode());

        // Test inequality - different names
        assertNotEquals(view1, view3);

        // Test inequality - different definitions
        assertNotEquals(view1, view4);

        // Test self equality
        assertEquals(view1, view1);

        // Test null inequality
        assertNotEquals(view1, null);
    }

    @Test
    public void testToString() {
        final View view = new View("my_view", "SELECT id, name FROM users");
        final String result = view.toString();

        assertTrue(result.contains("my_view"));
        assertTrue(result.contains("SELECT id, name FROM users"));
    }

    @Test
    public void testProtoSerialization() {
        final String viewName = "serialization_test";
        final String definition = "SELECT a, b FROM table WHERE c > 10";
        final View originalView = new View(viewName, definition);

        final RecordMetaDataProto.PView proto = originalView.toProto();

        assertEquals(viewName, proto.getName());
        assertEquals(definition, proto.getDefinition());
    }

    @Test
    public void testProtoDeserialization() {
        final String viewName = "deserialization_test";
        final String definition = "SELECT x, y, z FROM test_table";

        final RecordMetaDataProto.PView protoView = RecordMetaDataProto.PView.newBuilder()
                .setName(viewName)
                .setDefinition(definition)
                .build();

        final View view = View.fromProto(protoView);

        assertEquals(viewName, view.getName());
        assertEquals(definition, view.getDefinition());
    }

    @Test
    public void testRoundTripSerialization() {
        final View originalView = new View("round_trip_view", "SELECT * FROM employees WHERE salary > 50000");

        // Serialize to proto
        final RecordMetaDataProto.PView proto = originalView.toProto();

        // Deserialize from proto
        final View deserializedView = View.fromProto(proto);

        // Verify equality
        assertEquals(originalView.getName(), deserializedView.getName());
        assertEquals(originalView.getDefinition(), deserializedView.getDefinition());
    }
}
