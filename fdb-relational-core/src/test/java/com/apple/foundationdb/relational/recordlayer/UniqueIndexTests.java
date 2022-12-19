/*
 * UniqueIndexTests.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UniqueIndexTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String getTemplate_definition =
            "CREATE TABLE T1(t1_p int64, t1_a int64, primary key(t1_p))\n" +
                    "CREATE UNIQUE INDEX mv1 AS SELECT t1_a FROM t1\n" +
                    "CREATE TABLE T2(t2_p int64, t2_a int64, t2_b int64, primary key(t2_p))\n" +
                    "CREATE UNIQUE INDEX mv2 AS SELECT t2_a, t2_b FROM t2 order by t2_a, t2_b\n" +
                    "CREATE TABLE T3(t3_p int64, t3_a int64, t3_b int64, primary key(t3_p))\n" +
                    "CREATE UNIQUE INDEX mv3 AS SELECT t3_a FROM t3\n" +
                    "CREATE UNIQUE INDEX mv4 AS SELECT t3_b FROM t3\n" +
                    "CREATE TYPE AS STRUCT ST1(st1_a int64)\n" +
                    "CREATE TABLE T4(t4_p int64, t4_st1 st1 array, primary key(t4_p))\n" +
                    "CREATE UNIQUE INDEX mv5 AS SELECT v.st1_a from t4 t, (SELECT u.st1_a from t.t4_st1 u) v\n" +
                    "CREATE TABLE T5(t5_p int64, t5_a int64, t5_b int64, t5_c int64, t5_d int64, primary key(t5_p))\n" +
                    "CREATE UNIQUE INDEX mv6 AS SELECT t5_a, t5_b, t5_c, t5_d from t5 order by t5_d, t5_c\n";

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(relationalExtension, URI.create("/DbWithUniqueTests"), getTemplate_definition);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private void insertUniqueRecordsToTable(@Nonnull List<Message> toInsert, @Nonnull String tableName) {
        try {
            final var count = statement.executeInsert(tableName, toInsert);
            Assertions.assertEquals(count, toInsert.size());
        } catch (Exception e) {
            Assertions.fail(String.format("Unexpected exception while inserting records to table %s: %s", tableName, e.getMessage()));
        }
    }

    private void checkErrorOnNonUniqueInsertionsToTable(@Nonnull List<Message> toInsert, @Nonnull String tableName) {
        boolean foundError = false;
        try {
            final var count = statement.executeInsert(tableName, toInsert);
            Assertions.assertEquals(count, toInsert.size());
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains("Duplicate entry for unique index"));
            foundError = true;
        } catch (Exception e) {
            Assertions.fail(String.format("Unexpected exception while inserting records to table %s: %s", tableName, e.getMessage()));
            foundError = true;
        }
        if (!foundError) {
            Assertions.fail("Non unique record inserted without an error.");
        }
    }

    @Test
    public void insertToColMarkedUnique() throws Exception {
        final var uniqueOnARecords = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(statement.getDataBuilder("T1")
                    .setField("T1_P", i)
                    .setField("T1_A", i)
                    .build());
        }
        insertUniqueRecordsToTable(uniqueOnARecords, "T1");
        final var nonUniqueOnARecord = List.of(statement.getDataBuilder("T1")
                .setField("T1_P", 5)
                .setField("T1_A", 0)
                .build());
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T1");
    }

    @Test
    public void insertToTupleMarkedUnique() throws Exception {
        final var uniqueOnARecords = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(statement.getDataBuilder("T2")
                    .setField("T2_P", i)
                    .setField("T2_A", i * 2)
                    .setField("T2_B", 10 - i)
                    .build());
        }
        // This will insert [{0, 0, 10}, {1, 2, 9}, {2, 4, 8}, {3, 6, 7}, {4, 8, 6}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T2");
        final var nonUniqueOnARecord = List.of(statement.getDataBuilder("T2")
                .setField("T2_P", 5)
                .setField("T2_A", 2)
                .setField("T2_B", 9)
                .build());
        // Trying to insert [{5, 2, 9}]
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T2");
    }

    @Test
    public void insertWith2UniqueIndexes() throws Exception {
        final var uniqueOnARecords = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(statement.getDataBuilder("T3")
                    .setField("T3_P", i)
                    .setField("T3_A", i * 2)
                    .setField("T3_B", 10 - i)
                    .build());
        }
        // This will insert [{0, 0, 10}, {1, 2, 9}, {2, 4, 8}, {3, 6, 7}, {4, 8, 6}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T3");
        final var nonUniqueOnARecord1 = List.of(statement.getDataBuilder("T3")
                .setField("T3_P", 5)
                .setField("T3_A", 2)
                .setField("T3_B", 5)
                .build());
        // Trying to insert [{5, 2, 5}]. Other 5 is duplicated
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord1, "T3");
        final var nonUniqueOnARecord2 = List.of(statement.getDataBuilder("T3")
                .setField("T3_P", 5)
                .setField("T3_A", 10)
                .setField("T3_B", 9)
                .build());
        // Trying to insert [{5, 10, 9}]. 9 is duplicated.
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord2, "T3");
    }

    // This tests our current behavior with reference to treating null(s) as a non-unique value of a column and hence,
    // the unique index allows for having multiple nulls in the table. This test can be regarded more of a precautionary
    // measure to adhere to our current semantics and will require a change (or more tests needed to be added) as and
    // when those semantics change underneath.
    @Test
    public void insertToUniqueColWithNull() throws Exception {
        final var recordsWithNullA = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            recordsWithNullA.add(statement.getDataBuilder("T1")
                    .setField("T1_P", i)
                    .build());
        }
        insertUniqueRecordsToTable(recordsWithNullA, "T1");
    }

    @Test
    public void insertToArrayNestedFieldMarkedUnique() throws Exception {
        final var records = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            var messageBuilder = statement.getDataBuilder("T4");
            messageBuilder.setField("T4_P", i);
            var ST1ArrayBuilder = messageBuilder.getNestedMessageBuilder("T4_ST1");
            for (var j = 0; j < 5; j++) {
                ST1ArrayBuilder.addRepeatedField(1, ST1ArrayBuilder.getNestedMessageBuilder(1).setField(1, i * 5 + j).build());
            }
            messageBuilder.setField("T4_ST1", ST1ArrayBuilder.build());
            records.add(messageBuilder.build());
        }
        insertUniqueRecordsToTable(records, "T4");
        final var nonUniqueRecord = List.of(statement.getDataBuilder("T4")
                .setField("T4_P", 5)
                .setField("T4_ST1", statement.getDataBuilder("T4").getNestedMessageBuilder("T4_ST1")
                        .addRepeatedField(1, statement.getDataBuilder("T4").getNestedMessageBuilder("T4_ST1").getNestedMessageBuilder(1)
                                .setField(1, 1)
                                .build()
                        ).build()
                ).build()
        );
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueRecord, "T4");
    }

    @Test
    public void insertToTableWithUniqueCoveringIndexWithValueExp() throws Exception {
        final var uniqueOnARecords = new ArrayList<Message>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(statement.getDataBuilder("T5")
                    .setField("T5_P", i)
                    .setField("T5_A", i)
                    .setField("T5_B", 5 - i)
                    .setField("T5_C", i * 2)
                    .setField("T5_D", Math.abs(2 - i))
                    .build());
        }
        // This will insert [{0, 0, 5, 0, 2}, {1, 1, 4, 2, 1}, {2, 2, 3, 4, 0}, {3, 3, 2, 6, 1}, {4, 4, 1, 8, 2}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T5");
        final var nonUniqueOnARecord = List.of(statement.getDataBuilder("T5")
                .setField("T5_P", 10)
                .setField("T5_A", 10)
                .setField("T5_B", 10)
                .setField("T5_C", 8)
                .setField("T5_D", 2)
                .build());
        // Trying to insert [{10, 10, 10, 8, 2}]. This should fail since (T5_C=8, T5_D=2) is repeated, because
        // only the Key keyExpression is tested for the uniqueness.
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T5");
    }
}
