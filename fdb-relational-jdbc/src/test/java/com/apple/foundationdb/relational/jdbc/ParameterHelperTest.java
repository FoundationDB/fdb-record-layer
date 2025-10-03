/*
 * JDBCSimpleStatementTest.java
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

import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.NullColumn;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Uuid;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public class ParameterHelperTest {

    @Test
    void testInt() throws SQLException {
        final var value = 32;
        checkValue(value, ParameterHelper.ofObject(value), Type.INTEGER, Column::hasInteger, Column::getInteger);
        checkValue(value, ParameterHelper.ofInt(value), Type.INTEGER, Column::hasInteger, Column::getInteger);
    }

    @Test
    void testDouble() throws SQLException {
        final var value = 32.0d;
        checkValue(value, ParameterHelper.ofObject(value), Type.DOUBLE, Column::hasDouble, Column::getDouble);
        checkValue(value, ParameterHelper.ofDouble(value), Type.DOUBLE, Column::hasDouble, Column::getDouble);
    }

    @Test
    void testFloat() throws SQLException {
        final var value = 32.0f;
        checkValue(value, ParameterHelper.ofObject(value), Type.FLOAT, Column::hasFloat, Column::getFloat);
        checkValue(value, ParameterHelper.ofFloat(value), Type.FLOAT, Column::hasFloat, Column::getFloat);
    }

    @Test
    void testBoolean() throws SQLException {
        final var value = true;
        checkValue(value, ParameterHelper.ofObject(value), Type.BOOLEAN, Column::hasBoolean, Column::getBoolean);
        checkValue(value, ParameterHelper.ofBoolean(value), Type.BOOLEAN, Column::hasBoolean, Column::getBoolean);
    }

    @Test
    void testString() throws SQLException {
        final var value = "test string";
        checkValue(value, ParameterHelper.ofObject(value), Type.STRING, Column::hasString, Column::getString);
        checkValue(value, ParameterHelper.ofString(value), Type.STRING, Column::hasString, Column::getString);
    }

    @Test
    void testUUID() throws SQLException {
        final var value = UUID.randomUUID();
        final Consumer<Object> customAssert = actual -> {
            Assertions.assertInstanceOf(Uuid.class, actual);
            final var uuid = (Uuid) actual;
            Assertions.assertEquals(value, new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        };
        checkValue(value, ParameterHelper.ofObject(value), Type.UUID, Column::hasUuid, Column::getUuid, customAssert);
        checkValue(value, ParameterHelper.ofUUID(value), Type.UUID, Column::hasUuid, Column::getUuid, customAssert);
    }

    @Test
    void testLong() throws SQLException {
        final var value = 1234567890L;
        checkValue(value, ParameterHelper.ofObject(value), Type.LONG, Column::hasLong, Column::getLong);
        checkValue(value, ParameterHelper.ofLong(value), Type.LONG, Column::hasLong, Column::getLong);
    }

    @Test
    void testBytes() throws SQLException {
        final var value = new byte[]{1, 2, 3, 4};
        final Consumer<Object> customAssert = actual -> {
            Assertions.assertInstanceOf(ByteString.class, actual);
            Assertions.assertArrayEquals(value, ((ByteString) actual).toByteArray());
        };
        checkValue(value, ParameterHelper.ofObject(value), Type.BYTES, Column::hasBinary, Column::getBinary, customAssert);
        checkValue(value, ParameterHelper.ofBytes(value), Type.BYTES, Column::hasBinary, Column::getBinary, customAssert);
    }

//    @Test
//    void testArray() throws SQLException {
//        final var value = new byte[]{1, 2, 3, 4};
//        final Function<Object, Object> transform = actual -> {
//            Assertions.assertInstanceOf(ByteString.class, actual);
//            return ((ByteString) actual).toByteArray();
//        };
//        checkValue(value, ParameterHelper.ofObject(value), Type.BYTES, Column::hasBinary, Column::getBinary, transform);
//        checkValue(value, ParameterHelper.ofBytes(value), Type.BYTES, Column::hasBinary, Column::getBinary, transform);
//    }

    @Test
    void testGenericNull() throws SQLException {
        checkValue(null, ParameterHelper.ofObject(null), Type.NULL, Column::hasNull, Column::getNull, actual -> {
            Assertions.assertInstanceOf(NullColumn.class, actual);
        });
    }

    private static void checkValue(Object value, Parameter actual, Type expectedType, Function<Column, Boolean> typeChecker, Function<Column, Object> typeGetter) {
        checkValue(value, actual, expectedType, typeChecker, typeGetter, null);
    }

    private static void checkValue(Object value, Parameter actual, Type expectedType, Function<Column, Boolean> typeChecker,
                                   Function<Column, Object> typeGetter, @Nullable Consumer<Object> customAssert) {
        Assertions.assertEquals(expectedType, actual.getType());
        Assertions.assertTrue(actual.hasParameter());
        if (value != null) {
            Assertions.assertTrue(typeChecker.apply(actual.getParameter()));
        }
        if (customAssert == null) {
            Assertions.assertEquals(value, typeGetter.apply(actual.getParameter()));
        } else {
            customAssert.accept(typeGetter.apply(actual.getParameter()));
        }
    }
}
