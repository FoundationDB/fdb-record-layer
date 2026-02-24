/*
 * RecordTypeKeyTest.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;

public class RecordTypeKeyTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            RecordTypeKeyTest.class,
                    """
                    CREATE TABLE restaurant_review (reviewer bigint, rating bigint, SINGLE ROW ONLY)
                    CREATE TABLE restaurant_tag (tag string, weight bigint, PRIMARY KEY(tag))
                    CREATE INDEX record_rt_covering_idx as select reviewer from restaurant_review
                    """
    );

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void testPrimaryKeyWithOnlyRecordTypeKey() throws SQLException {
        var review = EmbeddedRelationalStruct.newBuilder()
                .addLong("REVIEWER", 12345)
                .addLong("RATING", 4)
                .build();
        var tag = EmbeddedRelationalStruct.newBuilder()
                .addString("TAG", "Awesome Burgers")
                .addLong("WEIGHT", 23)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW", review);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        count = statement.executeInsert("RESTAURANT_TAG", tag);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        // Only scan the "RESTAURANT" table
        try (final RelationalResultSet resultSet = statement.executeScan("RESTAURANT_REVIEW", new KeySet(), Options.NONE)) {
            // Only 1 RestaurantRecord is expected to be returned
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .isRowExactly(12345L, 4L)
                    .hasNoNextRow();
        }
    }

    @Test
    void testScanningWithUnknownKeys() throws Exception {
        var review = EmbeddedRelationalStruct.newBuilder()
                .addLong("REVIEWER", 678910)
                .addLong("RATING", 2)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW", review);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        KeySet keySet = new KeySet().setKeyColumn("REVIEWER", 678910);
        // Scan is expected to rejected because it uses fields which are not included in primary key
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeScan("RESTAURANT_REVIEW", keySet, Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("Unknown keys for primary key of <RESTAURANT_REVIEW>, unknown keys: <REVIEWER>");
    }

    @Test
    void canGetWithRecordTypeInPrimaryKey() throws  SQLException {
        var tag = EmbeddedRelationalStruct.newBuilder()
                .addString("TAG", "culvers")
                .addLong("WEIGHT", 23)
                .build();
        int count = statement.executeInsert("RESTAURANT_TAG", tag);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RESTAURANT_TAG",
                new KeySet().setKeyColumn("TAG", "culvers"),
                Options.NONE)) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .isRowExactly("culvers", 23L)
                    .hasNoNextRow();
        }
    }

    @Test
    void canGetWithRecordTypeKeyIndex() throws SQLException {
        var review = EmbeddedRelationalStruct.newBuilder()
                .addLong("REVIEWER", 678910)
                .addLong("RATING", 2)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW", review);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RESTAURANT_REVIEW",
                new KeySet().setKeyColumn("REVIEWER", 678910),
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_RT_COVERING_IDX").build())) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .isRowExactly(678910L, 2L)
                    .hasNoNextRow();
        }
    }
}
