/*
 * RelationalArrayTest.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalStructAssert;

import org.junit.Assert;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RelationalArrayTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, RelationalArrayTest.class, SCHEMA);

    private static final String SCHEMA = "CREATE TYPE AS STRUCT STRUCTURE(a integer, b string) " +
            "CREATE TYPE as enum enumeration('a', 'b') " +
            "CREATE TABLE T(" +
            "pk integer, " +
            "boolean_null boolean array, boolean_not_null boolean array not null, " +
            "integer_null integer array, integer_not_null integer array not null, " +
            "bigint_null bigint array, bigint_not_null bigint array not null, " +
            "float_null float array, float_not_null float array not null, " +
            "double_null double array, double_not_null double array not null, " +
            "string_null string array, string_not_null string array not null, " +
            "bytes_null bytes array, bytes_not_null bytes array not null, " +
            "structure_null structure array, structure_not_null structure array not null, " +
            // "enumeration_null enumeration array, enumeration_not_null enumeration array not null, " +
            "primary key(pk))";

    public void insertQuery(@Nonnull String q) throws RelationalException, SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate(q));
            }
        }
    }

    private void testInsertions() throws SQLException, RelationalException {
        insertQuery("INSERT INTO T VALUES (" +
                "1," +
                "[true, false], [true, false], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "['11', '22'], ['11', '22'], " +
                "[x'31', x'32'], [x'31', x'32'], " +
                "[(11, '11'), (22, '22')], [(11, '11'), (22, '22')] " +
                ")");
        insertQuery("INSERT INTO T VALUES (" +
                "2," +
                "null, [true, false], " +
                "null, [11, 22], " +
                "null, [11, 22], " +
                "null, [11, 22], " +
                "null, [11, 22], " +
                "null, ['11', '22'], " +
                "null, [x'31', x'32'], " +
                "null, [(11, '11'), (22, '22')] " +
                ")");
        Assert.assertThrows(SQLException.class,
                () -> insertQuery("INSERT INTO T VALUES (" +
                        "3," +
                        "[true, false], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "['11', '22'], null, " +
                        "[x'31', x'32'], null, " +
                        "[(11, '11'), (22, '22')], null " +
                        ")"));
        // This should not go through since non-null array is not initialized, However, that is being currently getting
        // initialized by an empty array (no elements in unwrapped REPEATED field in proto).
        // This phenomenon is also true with basic types, i.e., non-nullability is not enforced and is tracked by
        // TODO (Add support + tests for column nullable/not null)
        insertQuery("INSERT INTO T (pk) VALUES (4)");
    }

    private static Stream<Arguments> provideTypesForArrayTests() throws SQLException {
        return Stream.of(
                Arguments.of(2, 3, List.of(true, false), Types.BOOLEAN),
                Arguments.of(4, 5, List.of(11, 22), Types.INTEGER),
                Arguments.of(6, 7, List.of(11L, 22L), Types.BIGINT),
                Arguments.of(8, 9, List.of(11.0f, 22.0f), Types.FLOAT),
                Arguments.of(10, 11, List.of(11.0, 22.0), Types.DOUBLE),
                Arguments.of(12, 13, List.of("11", "22"), Types.VARCHAR),
                Arguments.of(14, 15, List.of(new byte[]{49}, new byte[]{50}), Types.BINARY),
                Arguments.of(16, 17, List.of(
                                EmbeddedRelationalStruct.newBuilder().addInt("A", 11).addString("B", "11").build(),
                                EmbeddedRelationalStruct.newBuilder().addInt("A", 22).addString("B", "22").build()),
                        Types.STRUCT)
        );
    }

    @ParameterizedTest
    @MethodSource("provideTypesForArrayTests")
    void testArrays(int nullArrayIdx, int nonNullArrayIdx, List<Object> filledArray, int sqlType)
            throws SQLException, RelationalException {
        testInsertions();
        testArrays(nullArrayIdx, nonNullArrayIdx, filledArray, filledArray, sqlType, 1);
        testArrays(nullArrayIdx, nonNullArrayIdx, null, filledArray, sqlType, 2);
        testArrays(nullArrayIdx, nonNullArrayIdx, null, List.of(), sqlType, 4);
    }

    private void testArrays(int nullArrayIdx, int nonNullArrayIdx, List<Object> nullArrayElements,
                            List<Object> nonNullArrayElements, int sqlType, int pk) throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            try (final var s = conn.createStatement()) {
                try (final var rs = s.executeQuery("SELECT * from T where pk = " + pk)) {
                    assertTrue(rs.next());
                    assertEquals(ResultSetMetaData.columnNullable, rs.getMetaData().isNullable(nullArrayIdx));
                    final var nullArray = rs.getArray(nullArrayIdx);
                    if (nullArrayElements == null) {
                        assertNull(nullArray);
                    } else {
                        try (final var arrayResultSet = nullArray.getResultSet()) {
                            checkArrayElementsUsingGetResultSet(arrayResultSet, sqlType, nullArrayElements);
                            checkArrayElementsUsingGetArray((Object[]) nullArray.getArray(), sqlType, nullArrayElements);
                        }
                    }
                    assertEquals(ResultSetMetaData.columnNoNulls, rs.getMetaData().isNullable(nonNullArrayIdx));
                    try (final var arrayResultSet = rs.getArray(nonNullArrayIdx).getResultSet()) {
                        checkArrayElementsUsingGetResultSet(arrayResultSet, sqlType, nonNullArrayElements);
                        checkArrayElementsUsingGetArray((Object[]) rs.getArray(nonNullArrayIdx).getArray(), sqlType, nonNullArrayElements);
                    }
                }
            }
        }
    }

    private void checkArrayElementsUsingGetResultSet(@Nonnull ResultSet arrayResultSet, int sqlType, List<Object> elements) throws SQLException {
        final var arrayMetadata = arrayResultSet.getMetaData();
        assertEquals(2, arrayMetadata.getColumnCount());
        assertEquals(ResultSetMetaData.columnNoNulls, arrayMetadata.isNullable(1));
        // TODO: TODO (Support array element nullability): The nullability of array element is not being propagated correctly!
        assertEquals(ResultSetMetaData.columnNoNulls, arrayMetadata.isNullable(2));
        assertEquals(Types.INTEGER, arrayMetadata.getColumnType(1));
        assertEquals(sqlType, arrayMetadata.getColumnType(2));
        for (int i = 0; i < elements.size(); i++) {
            assertTrue(arrayResultSet.next());
            assertEquals(arrayResultSet.getInt(1), i + 1);
            final var actual = arrayResultSet.getObject(2);
            if (sqlType == Types.BINARY) {
                assertArrayEquals((byte[]) actual, (byte[]) elements.get(i));
            } else if (sqlType == Types.STRUCT) {
                RelationalStructAssert.assertThat((RelationalStruct) actual).isEqualTo(elements.get(i));
            } else {
                assertEquals(arrayResultSet.getObject(2), elements.get(i));
            }
        }
        assertFalse(arrayResultSet.next());
    }

    private void checkArrayElementsUsingGetArray(@Nonnull Object[] array, int sqlType, List<Object> elements) throws SQLException {
        assertEquals(elements.size(), array.length);
        for (int i = 0; i < elements.size(); i++) {
            final var object = array[i];
            if (sqlType == Types.BINARY) {
                assertArrayEquals((byte[]) object, (byte[]) elements.get(i));
            } else if (sqlType == Types.STRUCT) {
                RelationalStructAssert.assertThat((RelationalStruct) object).isEqualTo(elements.get(i));
            } else {
                assertEquals(object, elements.get(i));
            }
        }
    }
}
