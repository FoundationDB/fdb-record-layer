/*
 * RelationalArrayTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.RelationalStructAssert;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
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
import java.util.UUID;
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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(RelationalArrayTest.class, SCHEMA);

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
            "struct_null structure array, struct_not_null structure array not null, " +
            "uuid_null uuid array, uuid_not_null uuid array not null, " +
            // "enumeration_null enumeration array, enumeration_not_null enumeration array not null, " +
            "primary key(pk)) " +
            "CREATE TABLE B(pk integer, uuid_null uuid array, primary key(pk))";

    public void insertQuery(@Nonnull String q) throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate(q));
            }
        }
    }

    @Test
    void testInsertArraysViaQuerySimpleStatement() throws SQLException {
        insertArraysViaQuerySimpleStatement();
    }

    @Test
    public void uuidArray() throws SQLException {
        insertQuery("INSERT INTO B VALUES (" +
                "1," +
                "['e5711bed-c606-49e2-a682-316348bf4091', '14b387cd-79ad-4860-9588-9c4e81588af0'])");

    }

    private void insertArraysViaQuerySimpleStatement() throws SQLException {
        insertQuery("INSERT INTO T VALUES (" +
                "1," +
                "[true, false], [true, false], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "[11, 22], [11, 22], " +
                "['11', '22'], ['11', '22'], " +
                "[x'31', x'32'], [x'31', x'32'], " +
                "[(11, '11'), (22, '22')], [(11, '11'), (22, '22')], " +
                "['e5711bed-c606-49e2-a682-316348bf4091', '14b387cd-79ad-4860-9588-9c4e81588af0'], ['e5711bed-c606-49e2-a682-316348bf4091', '14b387cd-79ad-4860-9588-9c4e81588af0'] " +
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
                "null, [(11, '11'), (22, '22')], " +
                "null, ['e5711bed-c606-49e2-a682-316348bf4091', '14b387cd-79ad-4860-9588-9c4e81588af0'] " +
                ")");
        RelationalAssertions.assertThrowsSqlException(() -> insertQuery("INSERT INTO T VALUES (" +
                        "3," +
                        "[true, false], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "[11, 22], null, " +
                        "['11', '22'], null, " +
                        "[x'31', x'32'], null, " +
                        "[(11, '11'), (22, '22')], null, " +
                        "['e5711bed-c606-49e2-a682-316348bf4091', '14b387cd-79ad-4860-9588-9c4e81588af0'], null " +
                        ")")).hasErrorCode(ErrorCode.INTERNAL_ERROR);
        RelationalAssertions.assertThrowsSqlException(() -> insertQuery("INSERT INTO T (pk) VALUES (4)"))
                .hasErrorCode(ErrorCode.NOT_NULL_VIOLATION);
    }

    @Test
    void testInsertArraysViaQueryPreparedStatement() throws SQLException {
        final var statement = "INSERT INTO T (pk, boolean_null, boolean_not_null, integer_null, integer_not_null, " +
                "bigint_null, bigint_not_null, float_null, float_not_null, double_null, double_not_null, string_null, " +
                "string_not_null, bytes_null, bytes_not_null, struct_null, struct_not_null, uuid_null, uuid_not_null) " +
                "VALUES (?pk, ?boolean_null, ?boolean_not_null, ?integer_null, ?integer_not_null, ?bigint_null, " +
                "?bigint_not_null, ?float_null, ?float_not_null, ?double_null, ?double_not_null, ?string_null, " +
                "?string_not_null, ?bytes_null, ?bytes_not_null, ?struct_null, ?struct_not_null, ?uuid_null, " +
                "?uuid_not_null)";

        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement(statement))) {
                ps.setInt("pk", 1);
                ps.setArray("boolean_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setArray("integer_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setArray("integer_not_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setArray("bigint_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setArray("bigint_not_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setArray("float_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setArray("float_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setArray("double_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setArray("double_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setArray("string_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                ps.setArray("string_not_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                ps.setArray("bytes_null", EmbeddedRelationalArray.newBuilder().addAll(new byte[]{49}, new byte[]{50}).build());
                ps.setArray("bytes_not_null", EmbeddedRelationalArray.newBuilder().addAll(new byte[]{49}, new byte[]{50}).build());
                ps.setArray("struct_null", EmbeddedRelationalArray.newBuilder().addAll(EmbeddedRelationalStruct.newBuilder().addInt("a", 11).addString("b", "11").build(), EmbeddedRelationalStruct.newBuilder().addInt("a", 22).addString("b", "22").build()).build());
                ps.setArray("struct_not_null", EmbeddedRelationalArray.newBuilder().addAll(EmbeddedRelationalStruct.newBuilder().addInt("a", 11).addString("b", "11").build(), EmbeddedRelationalStruct.newBuilder().addInt("a", 22).addString("b", "22").build()).build());
                ps.setArray("uuid_null", EmbeddedRelationalArray.newBuilder().addAll(UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"), UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")).build());
                ps.setArray("uuid_not_null", EmbeddedRelationalArray.newBuilder().addAll(UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"), UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")).build());
                Assertions.assertEquals(1, ps.executeUpdate());
            }
        }

        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement(statement))) {
                ps.setInt("pk", 2);
                ps.setNull("boolean_null", Types.ARRAY);
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setNull("integer_null", Types.ARRAY);
                ps.setArray("integer_not_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setNull("bigint_null", Types.ARRAY);
                ps.setArray("bigint_not_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setNull("float_null", Types.ARRAY);
                ps.setArray("float_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setNull("double_null", Types.ARRAY);
                ps.setArray("double_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setNull("string_null", Types.ARRAY);
                ps.setArray("string_not_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                ps.setNull("bytes_null", Types.ARRAY);
                ps.setArray("bytes_not_null", EmbeddedRelationalArray.newBuilder().addAll(new byte[]{49}, new byte[]{50}).build());
                ps.setNull("struct_null", Types.ARRAY);
                ps.setArray("struct_not_null", EmbeddedRelationalArray.newBuilder().addAll(EmbeddedRelationalStruct.newBuilder().addInt("a", 11).addString("b", "11").build(), EmbeddedRelationalStruct.newBuilder().addInt("a", 22).addString("b", "22").build()).build());
                ps.setNull("uuid_null", Types.ARRAY);
                ps.setArray("uuid_not_null", EmbeddedRelationalArray.newBuilder().addAll(UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"), UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")).build());
                Assertions.assertEquals(1, ps.executeUpdate());
            }
        }

        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement(statement))) {
                ps.setInt("pk", 2);
                ps.setArray("boolean_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setNull("boolean_not_null", Types.ARRAY);
                ps.setArray("integer_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setNull("integer_not_null", Types.ARRAY);
                ps.setArray("bigint_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setNull("bigint_not_null", Types.ARRAY);
                ps.setArray("float_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setNull("float_not_null", Types.ARRAY);
                ps.setArray("double_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setNull("double_not_null", Types.ARRAY);
                ps.setArray("string_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                ps.setNull("string_not_null", Types.ARRAY);
                ps.setArray("bytes_null", EmbeddedRelationalArray.newBuilder().addAll(new byte[]{49}, new byte[]{50}).build());
                ps.setNull("bytes_not_null", Types.ARRAY);
                ps.setArray("struct_null", EmbeddedRelationalArray.newBuilder().addAll(EmbeddedRelationalStruct.newBuilder().addInt("a", 11).addString("b", "11").build(), EmbeddedRelationalStruct.newBuilder().addInt("a", 22).addString("b", "22").build()).build());
                ps.setNull("struct_not_null", Types.ARRAY);
                ps.setArray("uuid_null", EmbeddedRelationalArray.newBuilder().addAll(UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"), UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")).build());
                ps.setNull("uuid_not_null", Types.ARRAY);
                RelationalAssertions.assertThrowsSqlException(ps::executeUpdate).hasErrorCode(ErrorCode.INTERNAL_ERROR);
            }
        }
    }

    @Test
    void testInsertEmptyArrayViaQueryPreparedStatement() throws SQLException {
        final var statement = "INSERT INTO T (pk, boolean_not_null, integer_not_null, bigint_not_null, float_not_null, " +
                "double_not_null, string_not_null, bytes_not_null, struct_not_null, uuid_not_null) " +
                "VALUES (?pk, ?boolean_not_null, ?integer_not_null, ?bigint_not_null, ?float_not_null, ?double_not_null, " +
                "?string_not_null, ?bytes_not_null, ?struct_not_null, ?uuid_not_null)";

        final var emptyStructArray = EmbeddedRelationalArray.newBuilder(DataType.StructType.from("STRUCTURE",
                List.of(
                        DataType.StructType.Field.from("A", DataType.IntegerType.nullable(), 1),
                        DataType.StructType.Field.from("B", DataType.StringType.nullable(), 2)),
                false)).build();

        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement(statement))) {
                ps.setInt("pk", 1);
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder(DataType.BooleanType.notNullable()).build());
                ps.setArray("integer_not_null", EmbeddedRelationalArray.newBuilder(DataType.IntegerType.notNullable()).build());
                ps.setArray("bigint_not_null", EmbeddedRelationalArray.newBuilder(DataType.LongType.notNullable()).build());
                ps.setArray("float_not_null", EmbeddedRelationalArray.newBuilder(DataType.FloatType.notNullable()).build());
                ps.setArray("double_not_null", EmbeddedRelationalArray.newBuilder(DataType.DoubleType.notNullable()).build());
                ps.setArray("string_not_null", EmbeddedRelationalArray.newBuilder(DataType.StringType.notNullable()).build());
                ps.setArray("bytes_not_null", EmbeddedRelationalArray.newBuilder(DataType.BytesType.notNullable()).build());
                ps.setArray("struct_not_null", emptyStructArray);
                ps.setArray("uuid_not_null", EmbeddedRelationalArray.newBuilder(DataType.UuidType.notNullable()).build());
                Assertions.assertEquals(1, ps.executeUpdate());
            }

            try (final var ps = (RelationalPreparedStatement) conn.prepareStatement("SELECT * from T where pk = 1")) {
                ResultSetAssert.assertThat(ps.executeQuery())
                        .hasNextRow()
                        .hasColumn("boolean_not_null", List.of())
                        .hasColumn("integer_not_null", List.of())
                        .hasColumn("bigint_not_null", List.of())
                        .hasColumn("float_not_null", List.of())
                        .hasColumn("double_not_null", List.of())
                        .hasColumn("string_not_null", List.of())
                        .hasColumn("bytes_not_null", List.of())
                        .hasColumn("struct_not_null", List.of())
                        .hasColumn("uuid_not_null", List.of());
            }
        }
    }

    @Test
    void testMoreParametersThanColumns() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement("INSERT INTO T (pk) VALUES (?pk, ?boolean_null, ?boolean_not_null)"))) {
                ps.setInt("pk", 1);
                ps.setArray("boolean_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                RelationalAssertions.assertThrowsSqlException(ps::executeUpdate).containsInMessage("Too many parameters")
                        .hasErrorCode(ErrorCode.SYNTAX_ERROR);
            }
        }
    }

    @Test
    void testNonNullViolation() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement("INSERT INTO T (pk) VALUES (?pk)"))) {
                ps.setInt("pk", 1);
                RelationalAssertions.assertThrowsSqlException(ps::executeUpdate).containsInMessage("violates not-null constraint")
                        .hasErrorCode(ErrorCode.NOT_NULL_VIOLATION);
            }
        }
    }

    @Test
    void testNullFieldsGetAutomaticallyFilled() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement("INSERT INTO T (pk, boolean_not_null, " +
                    "integer_not_null, bigint_not_null, float_not_null, double_not_null, string_not_null, bytes_not_null, " +
                    "struct_not_null, uuid_not_null) VALUES (?pk, ?boolean_not_null, ?integer_not_null, ?bigint_not_null, " +
                    "?float_not_null, ?double_not_null, ?string_not_null, ?bytes_not_null, ?struct_not_null, ?uuid_not_null)"))) {
                ps.setInt("pk", 2);
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setArray("integer_not_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setArray("bigint_not_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setArray("float_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setArray("double_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setArray("string_not_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                ps.setArray("bytes_not_null", EmbeddedRelationalArray.newBuilder().addAll(new byte[]{49}, new byte[]{50}).build());
                ps.setArray("struct_not_null", EmbeddedRelationalArray.newBuilder().addAll(EmbeddedRelationalStruct.newBuilder().addInt("a", 11).addString("b", "11").build(), EmbeddedRelationalStruct.newBuilder().addInt("a", 22).addString("b", "22").build()).build());
                ps.setArray("uuid_not_null", EmbeddedRelationalArray.newBuilder().addAll(UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"), UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")).build());
                assertEquals(1, ps.executeUpdate());
            }

            try (final var ps = (RelationalPreparedStatement) conn.prepareStatement("SELECT * from T where pk = 2")) {
                ResultSetAssert.assertThat(ps.executeQuery())
                        .hasNextRow()
                        .hasColumn("boolean_null", null)
                        .hasColumn("integer_null", null)
                        .hasColumn("bigint_null", null)
                        .hasColumn("float_null", null)
                        .hasColumn("double_null", null)
                        .hasColumn("string_null", null)
                        .hasColumn("bytes_null", null)
                        .hasColumn("struct_null", null)
                        .hasColumn("uuid_null", null);
            }
        }
    }

    @Test
    void testColumnRequiresValue() throws SQLException {
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString())) {
            conn.setSchema(database.getSchemaName());
            conn.setAutoCommit(true);
            try (final var ps = ((RelationalPreparedStatement) conn.prepareStatement("INSERT INTO T (pk, boolean_not_null, " +
                    "integer_not_null, bigint_not_null, float_not_null, double_not_null, string_not_null, bytes_not_null, " +
                    "struct_not_null) VALUES (?pk, ?boolean_not_null, ?integer_not_null, ?bigint_not_null, ?float_not_null, " +
                    "?double_not_null, ?string_not_null)"))) {
                ps.setInt("pk", 2);
                ps.setArray("boolean_not_null", EmbeddedRelationalArray.newBuilder().addAll(true, false).build());
                ps.setArray("integer_not_null", EmbeddedRelationalArray.newBuilder().addAll(11, 22).build());
                ps.setArray("bigint_not_null", EmbeddedRelationalArray.newBuilder().addAll(11L, 22L).build());
                ps.setArray("float_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0f, 22.0f).build());
                ps.setArray("double_not_null", EmbeddedRelationalArray.newBuilder().addAll(11.0, 22.0).build());
                ps.setArray("string_not_null", EmbeddedRelationalArray.newBuilder().addAll("11", "22").build());
                RelationalAssertions.assertThrowsSqlException(ps::executeUpdate).containsInMessage("not provided")
                        .hasErrorCode(ErrorCode.SYNTAX_ERROR);
            }
        }
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
                        Types.STRUCT),
                Arguments.of(18, 19, List.of(
                                UUID.fromString("e5711bed-c606-49e2-a682-316348bf4091"),
                                UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0")),
                        Types.OTHER)

        );
    }

    @ParameterizedTest
    @MethodSource("provideTypesForArrayTests")
    void testArrays(int nullArrayIdx, int nonNullArrayIdx, List<Object> filledArray, int sqlType) throws SQLException {
        insertArraysViaQuerySimpleStatement();
        testArrays(nullArrayIdx, nonNullArrayIdx, filledArray, filledArray, sqlType, 1);
        testArrays(nullArrayIdx, nonNullArrayIdx, null, filledArray, sqlType, 2);
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

    private void checkArrayElementsUsingGetArray(@Nonnull Object[] array, int sqlType, List<Object> elements) {
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
