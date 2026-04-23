/*
 * UniqueIndexTests.java
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
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class UniqueIndexTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String getTemplate_definition =
            """
            CREATE TABLE T1(t1_p bigint, t1_a bigint, primary key(t1_p))
            CREATE UNIQUE INDEX mv1 AS SELECT t1_a FROM t1
            CREATE TABLE T2(t2_p bigint, t2_a bigint, t2_b bigint, primary key(t2_p))
            CREATE UNIQUE INDEX mv2 AS SELECT t2_a, t2_b FROM t2 order by t2_a, t2_b
            CREATE TABLE T3(t3_p bigint, t3_a bigint, t3_b bigint, primary key(t3_p))
            CREATE UNIQUE INDEX mv3 AS SELECT t3_a FROM t3
            CREATE UNIQUE INDEX mv4 AS SELECT t3_b FROM t3
            CREATE TYPE AS STRUCT ST1(st1_a bigint)
            CREATE TABLE T4(t4_p bigint, t4_st1 st1 array, primary key(t4_p))
            CREATE UNIQUE INDEX mv5 AS SELECT v.st1_a from t4 t, (SELECT u.st1_a from t.t4_st1 u) v
            CREATE TABLE T5(t5_p bigint, t5_a bigint, t5_b bigint, t5_c bigint, t5_d bigint, primary key(t5_p))
            CREATE UNIQUE INDEX mv6 AS SELECT t5_a, t5_b, t5_c, t5_d from t5 order by t5_d, t5_c
            """;

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(UniqueIndexTests.class, getTemplate_definition);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private void insertUniqueRecordsToTable(@Nonnull List<RelationalStruct> toInsert, @Nonnull String tableName) {
        try {
            final var count = statement.executeInsert(tableName, toInsert);
            Assertions.assertEquals(count, toInsert.size());
        } catch (Exception e) {
            Assertions.fail(String.format(Locale.ROOT, "Unexpected exception while inserting records to table %s: %s", tableName, e.getMessage()));
        }
    }

    private void checkErrorOnNonUniqueInsertionsToTable(@Nonnull List<RelationalStruct> toInsert, @Nonnull String tableName) {
        boolean foundError = false;
        try {
            final var count = statement.executeInsert(tableName, toInsert);
            Assertions.assertEquals(count, toInsert.size());
        } catch (SQLException e) {
            Assertions.assertEquals(e.getSQLState(), ErrorCode.UNIQUE_CONSTRAINT_VIOLATION.getErrorCode());
            Assertions.assertTrue(e.getMessage().contains("Duplicate entry for unique index"));
            foundError = true;
        } catch (Exception e) {
            Assertions.fail(String.format(Locale.ROOT, "Unexpected exception while inserting records to table %s: %s", tableName, e.getMessage()));
        }
        if (!foundError) {
            Assertions.fail("Non unique record inserted without an error.");
        }
    }

    @Test
    public void insertToColMarkedUnique() throws Exception {
        final var uniqueOnARecords = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("T1_P", i)
                    .addLong("T1_A", i)
                    .build());
        }
        insertUniqueRecordsToTable(uniqueOnARecords, "T1");
        final var nonUniqueOnARecord = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T1_P", 5)
                .addLong("T1_A", 0)
                .build());
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T1");
    }

    @Test
    public void insertToTupleMarkedUnique() throws Exception {
        final var uniqueOnARecords = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("T2_P", i)
                    .addLong("T2_A", i * 2)
                    .addLong("T2_B", 10 - i)
                    .build());
        }
        // This will insert [{0, 0, 10}, {1, 2, 9}, {2, 4, 8}, {3, 6, 7}, {4, 8, 6}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T2");
        final var nonUniqueOnARecord = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T2_P", 5)
                .addLong("T2_A", 2)
                .addLong("T2_B", 9)
                .build());

        // Trying to insert [{5, 2, 9}]
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T2");
    }

    @Test
    public void insertWith2UniqueIndexes() throws Exception {
        final var uniqueOnARecords = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("T3_P", i)
                    .addLong("T3_A", i * 2)
                    .addLong("T3_B", 10 - i)
                    .build());
        }
        // This will insert [{0, 0, 10}, {1, 2, 9}, {2, 4, 8}, {3, 6, 7}, {4, 8, 6}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T3");
        final var nonUniqueOnARecord1 = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T3_P", 5)
                .addLong("T3_A", 2)
                .addLong("T3_B", 5)
                .build());

        // Trying to insert [{5, 2, 5}]. Other 5 is duplicated
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord1, "T3");
        final var nonUniqueOnARecord2 = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T3_P", 5)
                .addLong("T3_A", 10)
                .addLong("T3_B", 9)
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
        final var recordsWithNullA = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            recordsWithNullA.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("T1_P", i)
                    .build());
        }
        insertUniqueRecordsToTable(recordsWithNullA, "T1");
    }

    @Test
    public void insertToArrayNestedFieldMarkedUnique() throws Exception {
        final var records = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            var builder = EmbeddedRelationalStruct.newBuilder();
            builder.addLong("T4_P", i);
            var ST1ArrayBuilder = EmbeddedRelationalArray.newBuilder();
            for (var j = 0; j < 5; j++) {
                ST1ArrayBuilder.addStruct(EmbeddedRelationalStruct.newBuilder().addLong("ST1_A", i * 5 + j).build());
            }
            builder.addArray("T4_ST1", ST1ArrayBuilder.build());
            records.add(builder.build());
        }
        insertUniqueRecordsToTable(records, "T4");
        final var nonUniqueRecord = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T4_P", 5)
                .addArray("T4_ST1", EmbeddedRelationalArray.newBuilder()
                        .addStruct(EmbeddedRelationalStruct.newBuilder()
                                .addLong("ST1_A", 1)
                                .build()
                        ).build()
                ).build()
        );
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueRecord, "T4");
    }

    @Test
    public void insertToTableWithUniqueCoveringIndexWithValueExp() throws Exception {
        final var uniqueOnARecords = new ArrayList<RelationalStruct>();
        for (var i = 0; i < 5; i++) {
            uniqueOnARecords.add(EmbeddedRelationalStruct.newBuilder()
                    .addLong("T5_P", i)
                    .addLong("T5_A", i)
                    .addLong("T5_B", 5 - i)
                    .addLong("T5_C", i * 2)
                    .addLong("T5_D", Math.abs(2 - i))
                    .build());
        }
        // This will insert [{0, 0, 5, 0, 2}, {1, 1, 4, 2, 1}, {2, 2, 3, 4, 0}, {3, 3, 2, 6, 1}, {4, 4, 1, 8, 2}]
        insertUniqueRecordsToTable(uniqueOnARecords, "T5");
        final var nonUniqueOnARecord = List.of(EmbeddedRelationalStruct.newBuilder()
                .addLong("T5_P", 10)
                .addLong("T5_A", 10)
                .addLong("T5_B", 10)
                .addLong("T5_C", 8)
                .addLong("T5_D", 2)
                .build());
        // Trying to insert [{10, 10, 10, 8, 2}]. This should fail since (T5_C=8, T5_D=2) is repeated, because
        // only the Key keyExpression is tested for the uniqueness.
        checkErrorOnNonUniqueInsertionsToTable(nonUniqueOnARecord, "T5");
    }
}
