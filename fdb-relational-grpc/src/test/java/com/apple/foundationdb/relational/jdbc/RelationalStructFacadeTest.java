/*
 * RelationalStructFacadeTest.java
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

package com.apple.foundationdb.relational.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;

public class RelationalStructFacadeTest {
    @Test
    public void testSimpleString() throws SQLException {
        String key = "only-field";
        String value = "some-value";
        var relationalStruct = RelationalStructFacade.newBuilder().addString(key, value).build();
        Assertions.assertEquals(relationalStruct.getString(key), value);
    }

    @Test
    public void testSimpleBoolean() throws SQLException {
        String key = "only-field";
        boolean value = true;
        var relationalStruct = RelationalStructFacade.newBuilder().addBoolean(key, value).build();
        Assertions.assertEquals(relationalStruct.getBoolean(key), value);
    }

    @Test
    public void testSimpleDouble() throws SQLException {
        String key = "only-field";
        double value = 1.056;
        var relationalStruct = RelationalStructFacade.newBuilder().addDouble(key, value).build();
        Assertions.assertEquals(relationalStruct.getDouble(key), value);
    }

    @Test
    public void testSimpleBytes() throws SQLException {
        String key = "only-field";
        byte[] value = "something".getBytes();
        var relationalStruct = RelationalStructFacade.newBuilder().addBytes(key, value).build();
        Assertions.assertArrayEquals(relationalStruct.getBytes(key), value);
    }

    @Test
    public void testSimpleInt() throws SQLException {
        String key = "only-field";
        int value = 5;
        var relationalStruct = RelationalStructFacade.newBuilder().addInt(key, value).build();
        Assertions.assertEquals(relationalStruct.getInt(key), value);
    }

    @Test
    public void testSimpleLong() throws SQLException {
        String key = "only-field";
        long value = 55;
        var relationalStruct = RelationalStructFacade.newBuilder().addLong(key, value).build();
        Assertions.assertEquals(relationalStruct.getLong(key), value);
    }

    @Test
    public void testSimpleUuid() throws SQLException {
        String key = "only-field";
        final var uuid = UUID.randomUUID();
        var relationalStruct = RelationalStructFacade.newBuilder().addUuid(key, uuid).build();
        Assertions.assertEquals(relationalStruct.getUUID(key), uuid);
    }

    @Test
    public void testMultipleFields() throws SQLException {
        final var UuidValue = UUID.randomUUID();
        var relationalStruct = RelationalStructFacade.newBuilder()
                .addLong("field1", 1L)
                .addString("field2", "hello")
                .addUuid("field3", UuidValue)
                .build();
        Assertions.assertEquals(relationalStruct.getLong("field1"), 1L);
        Assertions.assertEquals(relationalStruct.getString("field2"), "hello");
        Assertions.assertEquals(relationalStruct.getUUID("field3"), UuidValue);
        Assertions.assertEquals(relationalStruct.getLong(1), 1L);
        Assertions.assertEquals(relationalStruct.getString(2), "hello");
        Assertions.assertEquals(relationalStruct.getUUID(3), UuidValue);
    }
}
