/*
 * RecordTypeKeyTest.java
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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import com.google.protobuf.Message;
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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            RecordTypeKeyTest.class,
            "CREATE TABLE restaurant_review (reviewer int64, rating int64, PRIMARY KEY(RECORD TYPE))" +
                    " CREATE TABLE restaurant_tag (tag string, weight int64, PRIMARY KEY(RECORD TYPE,tag))" +
                    " CREATE VALUE INDEX record_rt_covering_idx on restaurant_review(reviewer)");

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void testPrimaryKeyWithOnlyRecordTypeKey() throws RelationalException, SQLException {
        Message review = statement.getDataBuilder("RESTAURANT_REVIEW")
                .setField("REVIEWER", 12345)
                .setField("RATING", 4)
                .build();
        Message tag = statement.getDataBuilder("RESTAURANT_TAG")
                .setField("TAG", "Awesome Burgers")
                .setField("WEIGHT", 23)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW", review);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        count = statement.executeInsert("RESTAURANT_TAG", tag);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        // Only scan the "RESTAURANT" table
        TableScan scan = TableScan.newBuilder()
                .withTableName("RESTAURANT_REVIEW")
                .build();
        try (final RelationalResultSet resultSet = statement.executeScan(scan, Options.NONE)) {
            // Only 1 RestaurantRecord is expected to be returned
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasRowExactly(12345L, 4L)
                    .hasNoNextRow();
        }
    }

    @Test
    void testScanningWithUnknownKeys() throws RelationalException {
        Restaurant.RestaurantReview review = Restaurant.RestaurantReview.newBuilder()
                .setReviewer(678910)
                .setRating(2)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW",
                statement.getDataBuilder("RESTAURANT_REVIEW").convertMessage(review)
        );
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        TableScan scan = TableScan.newBuilder()
                .withTableName("RESTAURANT_REVIEW")
                .setStartKey("REVIEWER", review.getReviewer())
                .setEndKey("REVIEWER", review.getReviewer() + 1)
                .build();
        // Scan is expected to rejected because it uses fields which are not included in primary key
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> statement.executeScan(scan, Options.NONE))
                .hasMessageContaining("Unknown keys for primary key of <RESTAURANT_REVIEW>, unknown keys: <REVIEWER>")
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void canGetWithRecordTypeInPrimaryKey() throws RelationalException, SQLException {
        Message tag = statement.getDataBuilder("RESTAURANT_TAG")
                .setField("TAG", "culvers")
                .setField("WEIGHT", 23)
                .build();
        int count = statement.executeInsert("RESTAURANT_TAG", tag);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RESTAURANT_TAG",
                new KeySet().setKeyColumn("TAG", "culvers"),
                Options.NONE)) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .hasRowExactly("culvers", 23L)
                    .hasNoNextRow();
        }
    }

    @Test
    void canGetWithRecordTypeKeyIndex() throws RelationalException, SQLException {
        Message review = statement.getDataBuilder("RESTAURANT_REVIEW")
                .setField("REVIEWER", 678910)
                .setField("RATING", 2)
                .build();
        int count = statement.executeInsert("RESTAURANT_REVIEW", review);
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RESTAURANT_REVIEW",
                new KeySet().setKeyColumn("REVIEWER", 678910),
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_RT_COVERING_IDX").build())) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .hasRowExactly(678910L, 2L)
                    .hasNoNextRow();
        }
    }
}
