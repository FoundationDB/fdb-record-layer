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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withPrecision;

class AbstractRecordLayerResultSetTest {
    AbstractRecordLayerResultSet resultSet = Mockito.mock(AbstractRecordLayerResultSet.class);

    private static final Set<Pair<java.lang.Class, String>> supportedConversions = Set.of(
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
            Pair.of(String.class, "getString"),

            Pair.of(null, "getMessage"),
            Pair.of(Restaurant.RestaurantRecord.class, "getMessage"),

            Pair.of(null, "getRepeated"),
            Pair.of(ArrayList.class, "getRepeated"),

            Pair.of(null, "getObject"),
            Pair.of(Boolean.class, "getObject"),
            Pair.of(Long.class, "getObject"),
            Pair.of(Integer.class, "getObject"),
            Pair.of(Float.class, "getObject"),
            Pair.of(Double.class, "getObject"),
            Pair.of(String.class, "getObject"),
            Pair.of(Restaurant.RestaurantRecord.class, "getObject"),
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
                Restaurant.RestaurantRecord.newBuilder().setRestNo(39).build(),
                new ArrayList<>(List.of(0, 0L, 0.0, 0.0f)));
    }

    static Stream<String> allMethods() {
        return Stream.of(
                "getBoolean",
                "getLong",
                "getFloat",
                "getDouble",
                "getString",
                "getMessage",
                "getRepeated",
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

                TestCaseWithResult.of("getMessage", Restaurant.RestaurantRecord.newBuilder().setRestNo(42).build(), Restaurant.RestaurantRecord.newBuilder().setRestNo(42).build()),
                TestCaseWithResult.of("getMessage", null, null),

                TestCaseWithResult.of("getRepeated", List.of(1, 2, 3, 4), List.of(1, 2, 3, 4)),
                TestCaseWithResult.of("getRepeated", null, null),

                TestCaseWithResult.of("getObject", null, null),
                TestCaseWithResult.of("getObject", true, true),
                TestCaseWithResult.of("getObject", 1, 1),
                TestCaseWithResult.of("getObject", 1f, 1f),
                TestCaseWithResult.of("getObject", 1d, 1d),
                TestCaseWithResult.of("getObject", 1L, 1L),
                TestCaseWithResult.of("getObject", "abc", "abc"),
                TestCaseWithResult.of("getObject", Restaurant.RestaurantRecord.newBuilder().setRestNo(23).build(), Restaurant.RestaurantRecord.newBuilder().setRestNo(23).build()),
                TestCaseWithResult.of("getObject", List.of(1, 2, 3, 4), List.of(1, 2, 3, 4)));
    }

    static Stream<TestCase> cannotConvertTestCases() {
        return allTypes().flatMap(type ->
                allMethods()
                        .filter(method -> !supportedConversions.contains(Pair.of(type == null ? null : type.getClass(), method)))
                        .map(method -> TestCase.of(method, type)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void get(TestCaseWithResult testCase) throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InvalidColumnReferenceException {
        Method positionMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, int.class);
        Method fieldNameMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);

        if (!"getObject".equals(testCase.method)) {
            positionMethod.invoke(Mockito.doCallRealMethod().when(resultSet), 1);
        }
        fieldNameMethod.invoke(Mockito.doCallRealMethod().when(resultSet), "field1");

        Mockito.when(resultSet.getObject(1)).thenReturn(testCase.field);
        Mockito.when(resultSet.getPosition("field1")).thenReturn(1);

        Object positionResult = positionMethod.invoke(resultSet, 1);
        Object fieldNameResult = fieldNameMethod.invoke(resultSet, "field1");

        if (testCase.expected instanceof Float) {
            assertThat((Float) positionResult).isEqualTo((Float) testCase.expected, withPrecision(0.001f));
            assertThat((Float) fieldNameResult).isEqualTo((Float) testCase.expected, withPrecision(0.001f));
        } else if (testCase.expected instanceof Double) {
            assertThat((Double) positionResult).isEqualTo((Double) testCase.expected, withPrecision(0.001));
            assertThat((Double) fieldNameResult).isEqualTo((Double) testCase.expected, withPrecision(0.001));
        } else {
            assertThat(positionResult).isEqualTo(testCase.expected);
            assertThat(fieldNameResult).isEqualTo(testCase.expected);
        }
    }

    @ParameterizedTest
    @MethodSource("cannotConvertTestCases")
    void getThrow(TestCase testCase) throws NoSuchMethodException, SQLException, InvalidColumnReferenceException {
        Method positionMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, int.class);
        Method fieldNameMethod = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);

        Mockito.when(resultSet.getObject(1)).thenReturn(testCase.field);
        Mockito.when(resultSet.getPosition("field1")).thenReturn(1);

        assertThatThrownBy(() -> {
            try {
                positionMethod.invoke(Mockito.doCallRealMethod().when(resultSet), 1);
                positionMethod.invoke(resultSet, 1);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        })
                .isInstanceOf(SQLException.class)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());

        assertThatThrownBy(() -> {
            try {
                fieldNameMethod.invoke(Mockito.doCallRealMethod().when(resultSet), "field1");
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
    void getThrowInvalidColumnReference(TestCase testCase) throws SQLException, InvalidColumnReferenceException, NoSuchMethodException {
        Method method = AbstractRecordLayerResultSet.class.getMethod(testCase.method, String.class);
        Mockito.when(resultSet.getPosition("field1")).thenThrow(new InvalidColumnReferenceException("field1"));
        assertThatThrownBy(() -> {
            try {
                method.invoke(Mockito.doCallRealMethod().when(resultSet), "field1");
                method.invoke(resultSet, "field1");
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
        assertThat(set1).isEqualTo(set2);
        assertThat(set1).isEqualTo(set3);
    }

    @Test
    void supportsMessageParsing() {
        Mockito.doCallRealMethod().when(resultSet).supportsMessageParsing();
        assertThat(resultSet.supportsMessageParsing()).isFalse();
    }

    @Test
    void parseMessage() throws SQLException {
        Mockito.doCallRealMethod().when(resultSet).parseMessage();
        assertThatThrownBy(() -> resultSet.parseMessage())
                .isInstanceOf(SQLException.class)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Test
    void getMetaData() throws SQLException {
        Mockito.doCallRealMethod().when(resultSet).getMetaData();
        String[] mockedFieldNames = {"a", "b", "c"};
        Mockito.when(resultSet.getFieldNames()).thenReturn(mockedFieldNames);

        ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnCount()).isEqualTo(3);
        assertThat(metaData.getColumnName(0)).isEqualTo("a");
        assertThat(metaData.getColumnName(1)).isEqualTo("b");
        assertThat(metaData.getColumnName(2)).isEqualTo("c");
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
}
