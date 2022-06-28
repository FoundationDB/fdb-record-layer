/*
 * StructDataTypeTest.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Tests around using Struct data types in Returned ResultSets.
 */
public class StructDataTypeTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String TABLE_STRUCTURE =
            "CREATE STRUCT struct_1 (a string); " +
                    "CREATE TABLE t (name string, st1 struct_1, PRIMARY KEY(name));" +
                    "CREATE STRUCT struct_2 (c int64, d struct_1); " +
                    "CREATE TABLE nt (t_name string, st1 struct_2, PRIMARY KEY(t_name));" +
                    "CREATE STRUCT struct_3 (c bytes, d boolean); " +
                    "CREATE TABLE at (a_name string, st2 struct_3 ARRAY, PRIMARY KEY(a_name));";

    @RegisterExtension
    @Order(0)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, StructDataTypeTest.class, TABLE_STRUCTURE);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @BeforeEach
    void setUp() throws RelationalException {
        final DynamicMessageBuilder t1 = statement.getDataBuilder("t");
        Message m = t1.setField("name", "test_record_1")
                .setField("st1", t1.getNestedMessageBuilder("st1").setField("a", "Hello").build())
                .build();

        statement.executeInsert("t", m);

        final DynamicMessageBuilder ntBuilder = statement.getDataBuilder("nt");
        final DynamicMessageBuilder stBuilder = ntBuilder.getNestedMessageBuilder("st1");
        m = ntBuilder.setField("t_name", "nt_record")
                .setField("st1", stBuilder
                        .setField("c", 1234L)
                        .setField("d", stBuilder.getNestedMessageBuilder("d")
                                .setField("a", "Goodbye").build())
                        .build())
                .build();

        statement.executeInsert("nt", m);

        final DynamicMessageBuilder atBuilder = statement.getDataBuilder("at");
        final DynamicMessageBuilder st2Builder = atBuilder.getNestedMessageBuilder("st2");
        m = atBuilder.setField("a_name", "a_test_rec")
                .addRepeatedField("st2", st2Builder.setField("c", "Hello".getBytes(StandardCharsets.UTF_8))
                        .setField("d", true)
                        .build())
                .addRepeatedField("st2", st2Builder.setField("c", "Bonjour".getBytes(StandardCharsets.UTF_8))
                        .setField("d", false)
                        .build())
                .build();

        statement.executeInsert("at", m);

    }

    @Test
    void canReadSingleStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("name", "test_record_1");
        try (final RelationalResultSet resultSet = statement.executeGet("t", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            RelationalStruct struct = resultSet.getStruct("st1");
            Assertions.assertNotNull(struct, "No struct found for column!");
            Assertions.assertEquals("Hello", struct.getString(1), "Incorrect value for nested struct!");
            Assertions.assertEquals("Hello", struct.getString("a"), "Incorrect value for nested struct!");

            //check that the JDBC attributes methods work properly
            Assertions.assertArrayEquals(struct.getAttributes(), new Object[]{"Hello"}, "Incorrect attributes!");
        }
    }

    @Test
    void canReadNestedStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("t_name", "nt_record");
        try (final RelationalResultSet resultSet = statement.executeGet("nt", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            RelationalStruct struct = resultSet.getStruct("st1");
            Assertions.assertNotNull(struct, "No struct found for column!");
            Assertions.assertEquals(1234L, struct.getLong(1), "Incorrect value for nested struct!");
            Assertions.assertEquals(1234L, struct.getLong("c"), "Incorrect value for nested struct!");
            RelationalStruct nestedStruct = struct.getStruct("d");
            Assertions.assertNotNull(nestedStruct);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("a"), "Incorrect doubly-nested struct");

            nestedStruct = struct.getStruct(2);
            Assertions.assertNotNull(nestedStruct);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("a"), "Incorrect doubly-nested struct");
            //use get object to make sure it returns the correct type
            nestedStruct = (RelationalStruct) struct.getObject(2);
            Assertions.assertEquals("Goodbye", nestedStruct.getString(1), "Incorrect doubly-nested struct");
            Assertions.assertEquals("Goodbye", nestedStruct.getString("a"), "Incorrect doubly-nested struct");
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    void canReadRepeatedStruct() throws Exception {
        final KeySet key = new KeySet().setKeyColumn("a_name", "a_test_rec");
        try (final RelationalResultSet resultSet = statement.executeGet("at", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            Assertions.assertEquals("a_test_rec", resultSet.getString("a_name"), "Incorrect name!");
            Assertions.assertEquals("a_test_rec", resultSet.getString(1), "Incorrect name!");

            final Array st2 = resultSet.getArray("st2");
            Assertions.assertNotNull(st2, "Array is missing!");

            try (ResultSet arrayRs = st2.getResultSet()) {
                Assertions.assertTrue(arrayRs.next(), "No array records returned!");
                Assertions.assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), arrayRs.getBytes(1), "Incorrect bytes column!");
                Assertions.assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), arrayRs.getBytes("c"), "Incorrect bytes column!");

                Assertions.assertTrue(arrayRs.getBoolean(2), "Incorrect boolean column!");
                Assertions.assertTrue(arrayRs.getBoolean("d"), "Incorrect boolean column!");

                Assertions.assertTrue(arrayRs.next(), "too few array records returned!");
                Assertions.assertArrayEquals("Bonjour".getBytes(StandardCharsets.UTF_8), arrayRs.getBytes(1), "Incorrect bytes column!");
                Assertions.assertArrayEquals("Bonjour".getBytes(StandardCharsets.UTF_8), arrayRs.getBytes("c"), "Incorrect bytes column!");

                Assertions.assertFalse(arrayRs.getBoolean(2), "Incorrect boolean column!");
                Assertions.assertFalse(arrayRs.getBoolean("d"), "Incorrect boolean column!");

                Assertions.assertFalse(arrayRs.next(), "too many array records returned!");
            }

        }
    }

    @Test
    void canReadRepeatedStructWithArray() throws RelationalException, SQLException {
        final KeySet key = new KeySet().setKeyColumn("a_name", "a_test_rec");
        try (final RelationalResultSet resultSet = statement.executeGet("at", key, Options.NONE)) {
            Assertions.assertTrue(resultSet.next(), "Did not find a record!");
            Assertions.assertEquals("a_test_rec", resultSet.getString("a_name"), "Incorrect name!");
            Assertions.assertEquals("a_test_rec", resultSet.getString(1), "Incorrect name!");

            final Array st2 = resultSet.getArray("st2");
            Assertions.assertNotNull(st2, "Array is missing!");

            //now check that the Object[] functionality also works
            Object obj = st2.getArray();
            Assertions.assertTrue(obj instanceof Object[], "Did not return an array of data!");
            Object[] data = (Object[]) obj;
            Set<String> expectedFirstColumn = Set.of("Hello", "Bonjour");
            Set<Boolean> expectedSecondColumn = Set.of(true, false);

            for (Object r : data) {
                Assertions.assertTrue(r instanceof Object[], "Did not return an array for a row!");
                Object[] row = (Object[]) r;
                Assertions.assertEquals(row.length, 2, "Incorrect row length");
                Assertions.assertTrue(expectedFirstColumn.contains(new String((byte[]) row[0], StandardCharsets.UTF_8)), "Did not contain the correct value for column c");
                Assertions.assertTrue(expectedSecondColumn.contains(row[1]), "Did not contain the correct value for column d");
            }
        }
    }
}
