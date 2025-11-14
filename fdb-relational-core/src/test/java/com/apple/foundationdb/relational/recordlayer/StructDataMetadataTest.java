/*
 * StructDataMetadataTest.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.ThrowingConsumer;

import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThrowsSqlException;

/**
 * Tests around using Struct data types in Returned ResultSets.
 */
public class StructDataMetadataTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String TABLE_STRUCTURE =
            "CREATE TYPE AS STRUCT struct_1 (a string) " +
                    " CREATE TABLE t (name string, st1 struct_1, PRIMARY KEY(name))" +
                    " CREATE TYPE AS STRUCT struct_2 (c bigint, d struct_1) " +
                    " CREATE TABLE nt (t_name string, st1 struct_2, PRIMARY KEY(t_name))" +
                    " CREATE TYPE AS STRUCT struct_3 (c bytes, d boolean) " +
                    " CREATE TABLE at (a_name string, st2 struct_3 ARRAY, PRIMARY KEY(a_name))";

    /*
    message at {
      string a_name = 1;
      repeated struct_3 st2 = 2;
    }
     */
    @RegisterExtension
    @Order(0)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(StructDataMetadataTest.class, TABLE_STRUCTURE);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @BeforeEach
    void setUp() throws SQLException {
        var m = EmbeddedRelationalStruct.newBuilder()
                .addString("NAME", "test_record_1")
                .addStruct("ST1", EmbeddedRelationalStruct.newBuilder().addString("A", "Hello").build())
                .build();
        statement.executeInsert("T", m);
        m = EmbeddedRelationalStruct.newBuilder()
                .addString("NAME", "test_record_2")
                .addStruct("ST1", EmbeddedRelationalStruct.newBuilder().addString("A", "World").build())
                .build();
        statement.executeInsert("T", m);

        m = EmbeddedRelationalStruct.newBuilder()
                .addString("T_NAME", "nt_record")
                .addStruct("ST1", EmbeddedRelationalStruct.newBuilder()
                        .addLong("C", 1234L)
                        .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                .addString("A", "Goodbye")
                                .build())
                        .build())
                .build();
        statement.executeInsert("NT", m);

        m = EmbeddedRelationalStruct.newBuilder()
                .addString("T_NAME", "nt_record2")
                .addStruct("ST1", EmbeddedRelationalStruct.newBuilder()
                        .addLong("C", 5678L)
                        .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                .addString("A", "Ciao")
                                .build())
                        .build())
                .build();
        statement.executeInsert("NT", m);

        var atBuilder = EmbeddedRelationalStruct.newBuilder();
        m = atBuilder.addString("A_NAME", "a_test_rec")
                .addArray("ST2", EmbeddedRelationalArray.newBuilder()
                        .addStruct(EmbeddedRelationalStruct.newBuilder()
                                .addBytes("C", "Hello".getBytes(StandardCharsets.UTF_8))
                                .addBoolean("D", true)
                                .build())
                        .addStruct(EmbeddedRelationalStruct.newBuilder()
                                .addBytes("C", "Bonjour".getBytes(StandardCharsets.UTF_8))
                                .addBoolean("D", false)
                                .build())
                        .build())
                .build();
        statement.executeInsert("AT", m);

        atBuilder = EmbeddedRelationalStruct.newBuilder();
        m = atBuilder.addString("A_NAME", "another_test_rec")
                .addArray("ST2", EmbeddedRelationalArray.newBuilder()
                        .addStruct(EmbeddedRelationalStruct.newBuilder()
                                .addBytes("C", "今日は".getBytes(StandardCharsets.UTF_8))
                                .addBoolean("D", true)
                                .build())
                        .addStruct(EmbeddedRelationalStruct.newBuilder()
                                .addBytes("C", "مرحبًا".getBytes(StandardCharsets.UTF_8))
                                .addBoolean("D", false)
                                .build())
                        .build())
                .build();
        statement.executeInsert("AT", m);
    }

    @Test
    void canReadSingleStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("NAME", "test_record_1");
        try (final RelationalResultSet resultSet = statement.executeGet("T", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            RelationalStruct struct = resultSet.getStruct("ST1");
            Assertions.assertNotNull(struct, "No struct found for column!");
            Assertions.assertEquals("Hello", struct.getString(1), "Incorrect value for nested struct!");
            Assertions.assertEquals("Hello", struct.getString("A"), "Incorrect value for nested struct!");

            //check that the JDBC attributes methods work properly
            Assertions.assertArrayEquals(new Object[]{"Hello"}, struct.getAttributes(), "Incorrect attributes!");
        }
    }

    private void canReadStructTypeName(String query, ThrowingConsumer<RelationalResultSet> assertOnMetaData) throws Throwable {
        Continuation continuation;
        statement.setMaxRows(1);
        try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            assertOnMetaData.accept(resultSet);
            continuation = resultSet.getContinuation();
        }
        try (final PreparedStatement ps = connection.prepareStatement("EXECUTE CONTINUATION ?")) {
            ps.setBytes(1, continuation.serialize());
            try (final ResultSet resultSet = ps.executeQuery()) {
                Assertions.assertTrue(resultSet.next(), "Did not find a record!");
                assertOnMetaData.accept(resultSet.unwrap(RelationalResultSet.class));
            }
        }
    }

    @Test
    void canReadProjectedStructTypeNameInNestedStar() throws Throwable {
        canReadStructTypeName("SELECT (*) FROM T", resultSet -> {
            RelationalStruct struct = resultSet.getStruct(1).getStruct("ST1");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedNestedStructTypeNameInNestedStar() throws Throwable {
        canReadStructTypeName("SELECT (*) FROM NT", resultSet -> {
            RelationalStruct struct = resultSet.getStruct(1).getStruct("ST1").getStruct("D");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedStructInArrayTypeNameInNestedStar() throws Throwable {
        canReadStructTypeName("SELECT (*) FROM AT", resultSet -> {
            RelationalArray array = resultSet.getStruct(1).getArray("ST2");
            Assertions.assertEquals("STRUCT", array.getMetaData().getElementTypeName());
            Assertions.assertEquals("STRUCT_3", array.getMetaData().getElementStructMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedStructTypeNameInUnnestedStar() throws Throwable {
        canReadStructTypeName("SELECT * FROM T", resultSet -> {
            RelationalStruct struct = resultSet.getStruct("ST1");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedNestedStructTypeNameInUnnestedStar() throws Throwable {
        canReadStructTypeName("SELECT * FROM NT", resultSet -> {
            RelationalStruct struct = resultSet.getStruct("ST1").getStruct("D");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedStructInArrayTypeNameInUnnestedStar() throws Throwable {
        canReadStructTypeName("SELECT * FROM AT", resultSet -> {
            RelationalArray array = resultSet.getArray("ST2");
            Assertions.assertEquals("STRUCT", array.getMetaData().getElementTypeName());
            Assertions.assertEquals("STRUCT_3", array.getMetaData().getElementStructMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedStructTypeNameDirectlyProjected() throws Throwable {
        canReadStructTypeName("SELECT ST1 FROM T", resultSet -> {
            RelationalStruct struct = resultSet.getStruct("ST1");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedNestedStructTypeNameDirectlyProjected() throws Throwable {
        canReadStructTypeName("SELECT ST1 FROM NT", resultSet -> {
            RelationalStruct struct = resultSet.getStruct("ST1").getStruct("D");
            Assertions.assertEquals("STRUCT_1", struct.getMetaData().getTypeName());
        });
    }

    @Test
    void canReadProjectedStructInArrayTypeNameDirectlyProjected() throws Throwable {
        canReadStructTypeName("SELECT * FROM AT", resultSet -> {
            RelationalArray array = resultSet.getArray("ST2");
            Assertions.assertEquals("STRUCT", array.getMetaData().getElementTypeName());
            Assertions.assertEquals("STRUCT_3", array.getMetaData().getElementStructMetaData().getTypeName());
        });
    }

    @Test
    void errorAccessingNonExistentColumn() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeGet("T", new KeySet().setKeyColumn("NAME", "test_record_1"), Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            final var actualStruct = resultSet.getStruct("ST1");
            Assertions.assertNotNull(actualStruct, "No struct found for column!");
            // Directly accessing the value throws SQLException
            assertThrowsSqlException(() -> actualStruct.getString(100)).containsInMessage("Invalid column position");
            // Accessing info in metadata throws SQLException as well
            assertThrowsSqlException(() -> actualStruct.getMetaData().getColumnType(100)).containsInMessage("Position <100> is not valid.");
        }
    }

    @Test
    void canReadNestedStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("T_NAME", "nt_record");
        try (final RelationalResultSet resultSet = statement.executeGet("NT", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            RelationalStruct struct = resultSet.getStruct("ST1");
            Assertions.assertNotNull(struct, "No struct found for column!");
            Assertions.assertEquals(1234L, struct.getLong(1), "Incorrect value for nested struct!");
            Assertions.assertEquals(1234L, struct.getLong("C"), "Incorrect value for nested struct!");
            RelationalStruct nestedStruct = struct.getStruct("D");
            Assertions.assertNotNull(nestedStruct);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("A"), "Incorrect doubly-nested struct");

            nestedStruct = struct.getStruct(2);
            Assertions.assertNotNull(nestedStruct);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("A"), "Incorrect doubly-nested struct");
            //use get object to make sure it returns the correct type
            nestedStruct = (RelationalStruct) struct.getObject(2);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("A"), "Incorrect doubly-nested struct");
        }
    }

    @Test
    void canReadRepeatedStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("A_NAME", "a_test_rec");
        try (final RelationalResultSet resultSet = statement.executeGet("AT", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            Assertions.assertEquals("a_test_rec", resultSet.getString("A_NAME"), "Incorrect name!");
            Assertions.assertEquals("a_test_rec", resultSet.getString(1), "Incorrect name!");

            final var st2 = resultSet.getArray("ST2");
            Assertions.assertNotNull(st2, "Array is missing!");

            try (var arrayRs = st2.getResultSet()) {
                Assertions.assertTrue(arrayRs.next(), "No array records returned!");
                var struct = arrayRs.getStruct(2);
                Assertions.assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), struct.getBytes(1), "Incorrect bytes column!");
                Assertions.assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), struct.getBytes("C"), "Incorrect bytes column!");

                Assertions.assertTrue(struct.getBoolean(2), "Incorrect boolean column!");
                Assertions.assertTrue(struct.getBoolean("D"), "Incorrect boolean column!");

                Assertions.assertTrue(arrayRs.next(), "too few array records returned!");
                struct = arrayRs.getStruct(2);
                Assertions.assertArrayEquals("Bonjour".getBytes(StandardCharsets.UTF_8), struct.getBytes(1), "Incorrect bytes column!");
                Assertions.assertArrayEquals("Bonjour".getBytes(StandardCharsets.UTF_8), struct.getBytes("C"), "Incorrect bytes column!");

                Assertions.assertFalse(struct.getBoolean(2), "Incorrect boolean column!");
                Assertions.assertFalse(struct.getBoolean("D"), "Incorrect boolean column!");

                Assertions.assertFalse(arrayRs.next(), "too many array records returned!");
            }

        }
    }

    @Test
    void canReadRepeatedStructWithArray() throws SQLException {
        final KeySet key = new KeySet().setKeyColumn("A_NAME", "a_test_rec");
        try (final RelationalResultSet resultSet = statement.executeGet("AT", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            Assertions.assertEquals("a_test_rec", resultSet.getString("A_NAME"), "Incorrect name!");
            Assertions.assertEquals("a_test_rec", resultSet.getString(1), "Incorrect name!");

            final Array st2 = resultSet.getArray("ST2");
            Assertions.assertNotNull(st2, "Array is missing!");

            //now check that the Object[] functionality also works
            Object obj = st2.getArray();
            Assertions.assertInstanceOf(Object[].class, obj, "Did not return an array of data!");
            Object[] data = (Object[]) obj;
            Set<String> expectedFirstColumn = Set.of("Hello", "Bonjour");
            Set<Boolean> expectedSecondColumn = Set.of(true, false);

            for (Object r : data) {
                Assertions.assertInstanceOf(RelationalStruct.class, r, "Elements of array are expected to be a struct!");
                final var struct = (RelationalStruct) r;
                Assertions.assertEquals(struct.getMetaData().getColumnCount(), 2, "Incorrect row length");
                Assertions.assertTrue(expectedFirstColumn.contains(new String(struct.getBytes(1), StandardCharsets.UTF_8)), "Did not contain the correct value for column c");
                Assertions.assertTrue(expectedSecondColumn.contains(struct.getBoolean(2)), "Did not contain the correct value for column d");
            }
        }
    }
}
