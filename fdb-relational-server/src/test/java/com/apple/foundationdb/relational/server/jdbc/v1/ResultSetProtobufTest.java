/*
 * ResultSetProtobufTest.java
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

package com.apple.foundationdb.relational.server.jdbc.v1;


import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Column;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

public class ResultSetProtobufTest {
    @Test
    public void testToColumnStringBiFunction() {
        BiFunction<String, Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearString() : b.setString((String) a);
        Column column = ResultSetProtobuf.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasString());
        String a = "abc";
        column = ResultSetProtobuf.toColumn(a, biFunction);
        Assertions.assertEquals(a, column.getString());
    }

    @Test
    public void testToColumnBinaryBiFunction() {
        BiFunction<byte[], Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearBinary() : b.setBinary(ByteString.copyFrom((byte[]) a));
        Column column = ResultSetProtobuf.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasBinary());
        byte[] a = new byte[]{'a', 'b', 'c'};
        column = ResultSetProtobuf.toColumn(a, biFunction);
        for (int i = 0; i < a.length; i++) {
            Assertions.assertEquals(a[i], column.getBinary().toByteArray()[i]);
        }
    }

    @Test
    public void testToColumnIntegerBiFunction() {
        BiFunction<Integer, Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearInteger() : b.setInteger((Integer) a);
        Column column = ResultSetProtobuf.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasInteger());
        Integer a = 123;
        column = ResultSetProtobuf.toColumn(a, biFunction);
        Assertions.assertEquals(a, column.getInteger());
    }
}
