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
            "CREATE TABLE RestaurantReview (reviewer int64, rating int64, PRIMARY KEY(RECORD TYPE))" +
                    " CREATE TABLE RestaurantTag (tag string, weight int64, PRIMARY KEY(RECORD TYPE,tag))" +
                    " CREATE VALUE INDEX record_rt_covering_idx on RestaurantReview(reviewer)");

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void testPrimaryKeyWithOnlyRecordTypeKey() throws RelationalException, SQLException {
        Restaurant.RestaurantReview review = Restaurant.RestaurantReview.newBuilder()
                .setReviewer(12345)
                .setRating(4)
                .build();
        Restaurant.RestaurantTag tag = Restaurant.RestaurantTag.newBuilder().setTag("Awesome Burgers").setWeight(23).build();
        int count = statement.executeInsert("RestaurantReview",
                statement.getDataBuilder("RestaurantReview").convertMessage(review)
        );
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        count = statement.executeInsert("RestaurantTag",
                statement.getDataBuilder("RestaurantTag").convertMessage(tag)
        );
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        // Only scan the "RestaurantRecord" table
        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantReview")
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
        int count = statement.executeInsert("RestaurantReview",
                statement.getDataBuilder("RestaurantReview").convertMessage(review)
        );
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantReview")
                .setStartKey("reviewer", review.getReviewer())
                .setEndKey("reviewer", review.getReviewer() + 1)
                .build();
        // Scan is expected to rejected because it uses fields which are not included in primary key
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> statement.executeScan(scan, Options.NONE))
                .hasMessageContaining("Unknown keys for primary key of <RestaurantReview>, unknown keys: <REVIEWER>")
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void canGetWithRecordTypeInPrimaryKey() throws RelationalException, SQLException {
        Restaurant.RestaurantTag tag = Restaurant.RestaurantTag.newBuilder().setTag("culvers").setWeight(23).build();
        int count = statement.executeInsert("RestaurantTag",
                statement.getDataBuilder("RestaurantTag").convertMessage(tag));
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RestaurantTag",
                new KeySet().setKeyColumn("tag", tag.getTag()),
                Options.NONE)) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .hasRowExactly(tag.getTag(), (long) tag.getWeight())
                    .hasNoNextRow();
        }
    }

    @Test
    void canGetWithRecordTypeKeyIndex() throws RelationalException, SQLException {
        Restaurant.RestaurantReview review = Restaurant.RestaurantReview.newBuilder()
                .setReviewer(678910)
                .setRating(2)
                .build();
        int count = statement.executeInsert("RestaurantReview",
                statement.getDataBuilder("RestaurantReview").convertMessage(review)
        );
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RestaurantReview",
                new KeySet().setKeyColumn("reviewer", review.getReviewer()),
                Options.builder().withOption(Options.Name.INDEX_HINT, "record_rt_covering_idx").build())) {
            ResultSetAssert.assertThat(rrs).hasNextRow()
                    .hasRowExactly(review.getReviewer(), (long) review.getRating())
                    .hasNoNextRow();
        }
    }
}
