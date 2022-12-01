/*
 * AbstractRecordLayerResultSetTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;


class AbstractRecordLayerResultSetTest {

    private static final Set<Pair<java.lang.Class<?>, String>> supportedConversions = Set.of(
            Pair.of(null, "getBoolean"),
            Pair.of(Boolean.class, "getBoolean"),

            Pair.of(null, "getLong"),
            Pair.of(Long.class, "getLong"),
            Pair.of(Integer.class, "getLong"),
            Pair.of(Float.class, "getLong"),
            Pair.of(Double.class, "getLong"),

            Pair.of(null, "getFloat"),
            Pair.of(Long.class, "getFloat"),
            Pair.of(Integer.class, "getFloat"),
            Pair.of(Float.class, "getFloat"),
            Pair.of(Double.class, "getFloat"),

            Pair.of(null, "getDouble"),
            Pair.of(Long.class, "getDouble"),
            Pair.of(Integer.class, "getDouble"),
            Pair.of(Float.class, "getDouble"),
            Pair.of(Double.class, "getDouble"),

            Pair.of(null, "getString"),
            Pair.of(Boolean.class, "getString"),
            Pair.of(Integer.class, "getString"),
            Pair.of(Long.class, "getString"),
            Pair.of(Float.class, "getString"),
            Pair.of(Double.class, "getString"),
            Pair.of(String.class, "getString"),

            Pair.of(null, "getObject"),
            Pair.of(Boolean.class, "getObject"),
            Pair.of(Long.class, "getObject"),
            Pair.of(Integer.class, "getObject"),
            Pair.of(Float.class, "getObject"),
            Pair.of(Double.class, "getObject"),
            Pair.of(String.class, "getObject"),
            Pair.of(DynamicMessage.class, "getObject"),
            Pair.of(ArrayList.class, "getObject"));

    static Stream<Object> allTypes() {
        return Stream.of(
                null,
                Boolean.TRUE,
                0,
                0L,
                0.0,
                0.0f,
                "0",
                buildGenericMessage(),
                new ArrayList<>(List.of(0, 0L, 0.0, 0.0f)));
    }

    static Stream<String> allMethods() {
        return Stream.of(
                "getBoolean",
                "getLong",
                "getFloat",
                "getDouble",
                "getString",
                "getObject"
        );
    }

    static Stream<TestCaseWithResult> testCases() {
        return Stream.of(
                TestCaseWithResult.of("getBoolean", true, true),
                TestCaseWithResult.of("getBoolean", false, false),
                TestCaseWithResult.of("getBoolean", null, false),

                TestCaseWithResult.of("getLong", 1, 1L),
                TestCaseWithResult.of("getLong", 1L, 1L),
                TestCaseWithResult.of("getLong", 1.0, 1L),
                TestCaseWithResult.of("getLong", 1.0f, 1L),
                TestCaseWithResult.of("getLong", 0, 0L),
                TestCaseWithResult.of("getLong", 42, 42L),
                TestCaseWithResult.of("getLong", null, 0L),

                TestCaseWithResult.of("getFloat", 1, 1f),
                TestCaseWithResult.of("getFloat", 1L, 1f),
                TestCaseWithResult.of("getFloat", 1.1, 1.1f),
                TestCaseWithResult.of("getFloat", 1.1f, 1.1f),
                TestCaseWithResult.of("getFloat", 0, 0f),
                TestCaseWithResult.of("getFloat", 42, 42f),
                TestCaseWithResult.of("getFloat", null, 0f),

                TestCaseWithResult.of("getDouble", 1, 1d),
                TestCaseWithResult.of("getDouble", 1L, 1d),
                TestCaseWithResult.of("getDouble", 1.1, 1.1),
                TestCaseWithResult.of("getDouble", 1.1f, 1.1),
                TestCaseWithResult.of("getDouble", 0, 0d),
                TestCaseWithResult.of("getDouble", 42, 42d),
                TestCaseWithResult.of("getDouble", null, 0d),

                TestCaseWithResult.of("getString", "abc", "abc"),
                TestCaseWithResult.of("getString", null, null),

                TestCaseWithResult.of("getObject", null, null),
                TestCaseWithResult.of("getObject", true, true),
                TestCaseWithResult.of("getObject", 1, 1),
                TestCaseWithResult.of("getObject", 1f, 1f),
                TestCaseWithResult.of("getObject", 1d, 1d),
                TestCaseWithResult.of("getObject", 1L, 1L),
                TestCaseWithResult.of("getObject", "abc", "abc"),
                TestCaseWithResult.of("getObject", buildGenericMessage(), buildGenericMessage()),
                TestCaseWithResult.of("getObject", List.of(1, 2, 3, 4), List.of(1, 2, 3, 4)));
    }

    static Stream<TestCase> cannotConvertTestCases() {
        return allTypes().flatMap(type ->
                allMethods()
                        .filter(method -> !supportedConversions.contains(Pair.of(type == null ? null : type.getClass(), method)))
                        .map(method -> TestCase.of(method, type)));
    }

    private Row theRow;
    private AbstractRecordLayerResultSet resultSet;

    @BeforeEach
    void setUp() throws SQLException {
        StructMetaData smd = Mockito.mock(StructMetaData.class);
        Mockito.when(smd.getColumnCount()).thenReturn(1);
        Mockito.when(smd.getColumnName(1)).thenReturn("field1");

        resultSet = new AbstractRecordLayerResultSet(smd) {
            @Override
            protected Row advanceRow() {
                return theRow;
            }

            @Override
            @Nonnull
            public Continuation getContinuation() {
                return Continuation.BEGIN;
            }

            @Override
            public void close() {
                //no-op
            }
        };

    }

    @ParameterizedTest
    @MethodSource("testCases")
    void get(TestCaseWithResult testCase) throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InvalidColumnReferenceException {
        theRow = new ArrayRow(new Object[]{testCase.field});
        resultSet.next();

        Method positionMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, int.class);
        Method fieldNameMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);

        if (!"getObject".equals(testCase.method)) {
            positionMethod.invoke(resultSet, 1);
        }
        fieldNameMethod.invoke(resultSet, "field1");

        Object positionResult = positionMethod.invoke(resultSet, 1);
        Object fieldNameResult = fieldNameMethod.invoke(resultSet, "field1");

        if (testCase.expected instanceof Float) {
            Assertions.assertThat((Float) positionResult).isEqualTo((Float) testCase.expected, Assertions.withPrecision(0.001f));
            Assertions.assertThat((Float) fieldNameResult).isEqualTo((Float) testCase.expected, Assertions.withPrecision(0.001f));
        } else if (testCase.expected instanceof Double) {
            Assertions.assertThat((Double) positionResult).isEqualTo((Double) testCase.expected, Assertions.withPrecision(0.001));
            Assertions.assertThat((Double) fieldNameResult).isEqualTo((Double) testCase.expected, Assertions.withPrecision(0.001));
        } else {
            Assertions.assertThat(positionResult).isEqualTo(testCase.expected);
            Assertions.assertThat(fieldNameResult).isEqualTo(testCase.expected);
        }
    }

    @ParameterizedTest
    @MethodSource("cannotConvertTestCases")
    void getThrow(TestCase testCase) throws NoSuchMethodException, SQLException {
        theRow = new ArrayRow(new Object[]{testCase.field});
        resultSet.next();

        Method positionMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, int.class);
        Method fieldNameMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);

        Assertions.assertThatThrownBy(() -> {
            try {
                positionMethod.invoke(resultSet, 1);
                positionMethod.invoke(resultSet, 1);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        })
                .isInstanceOf(SQLException.class)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());

        Assertions.assertThatThrownBy(() -> {
            try {
                fieldNameMethod.invoke(resultSet, "field1");
                fieldNameMethod.invoke(resultSet, "field1");
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        })
                .isInstanceOf(SQLException.class)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void getThrowInvalidColumnReference(TestCase testCase) throws NoSuchMethodException, SQLException {
        Method method = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);
        //        Mockito.when(resultSet.getOneBasedPosition("field1")).thenThrow(new InvalidColumnReferenceException("field1"));

        theRow = new ArrayRow(new Object[]{testCase.field});

        resultSet.next();
        Assertions.assertThatThrownBy(() -> {
            try {
                method.invoke(resultSet, "field2");
                method.invoke(resultSet, "field2");
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        })
                .isInstanceOf(SQLException.class)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.INVALID_COLUMN_REFERENCE.getErrorCode());
    }

    @Test
    void testMethodCoherence() {
        Set<String> set1 = allMethods().collect(Collectors.toSet());
        Set<String> set2 = testCases().map(testCase -> testCase.method).collect(Collectors.toSet());
        Set<String> set3 = supportedConversions.stream().map(Pair::getRight).collect(Collectors.toSet());
        Assertions.assertThat(set1).isEqualTo(set2);
        Assertions.assertThat(set1).isEqualTo(set3);
    }

    private static class TestCase {
        String method;
        Object field;

        TestCase(String method, Object field) {
            this.method = method;
            this.field = field;
        }

        static TestCase of(String method, Object field) {
            return new TestCase(method, field);
        }

        public String toString() {
            StringBuilder sb = new StringBuilder("ResultSet(");
            if (field == null) {
                sb.append("NULL");
            } else {
                sb.append(field.getClass().getSimpleName()).append("(").append(field).append(")");
            }
            sb.append(").").append(method).append("()");
            return sb.toString();
        }
    }

    private static class TestCaseWithResult extends TestCase {
        Object expected;

        TestCaseWithResult(String method, Object field, Object expected) {
            super(method, field);
            this.expected = expected;
        }

        static TestCaseWithResult of(String method, Object field, Object expected) {
            return new TestCaseWithResult(method, field, expected);
        }

        public String toString() {
            return super.toString() + " == " + expected;
        }
    }

    private static Message buildGenericMessage() {
        return DynamicMessage.newBuilder(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(DescriptorProtos.DescriptorProto.newBuilder().build()).build()).build();
    }
}
