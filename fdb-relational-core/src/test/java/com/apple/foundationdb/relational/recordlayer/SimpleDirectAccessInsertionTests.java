/*
 * SimpleDirectAccessInsertionTests.java
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
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;

/**
 * Simple unit tests around direct-access insertion tests.
 */
public class SimpleDirectAccessInsertionTests {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(relationalExtension,
            URI.create("/" + SimpleDirectAccessInsertionTests.class.getSimpleName()), TestSchemas.restaurant());

    @Test
    void insertNestedFields() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.create())) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final DynamicMessageBuilder builder = s.getDataBuilder("RestaurantReviewer");
                builder.setField("id", 1L);
                builder.setField("name", "Anthony Bourdain");
                builder.setField("email", "abourdain@apple.com");
                builder.setField("stats",
                        s.getDataBuilder("ReviewerStats")
                                .setField("school_name", "Truman High School")
                                .setField("hometown", "Boise, Indiana")
                                .setField("start_date", 0L)
                                .build());
                Message toWrite = builder.build();

                int inserted = s.executeInsert("RestaurantReviewer", toWrite, Options.create());
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!").isEqualTo(1);
                KeySet key = new KeySet()
                        .setKeyColumn("id", 1L);
                try (RelationalResultSet rrs = s.executeGet("RestaurantReviewer", key, Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasExactly(new MessageTuple(toWrite));
                }
            }
        }
    }

    @Test
    void insertMultipleTablesDontMix() throws SQLException, RelationalException {
        /*
         * Because RecordLayer allows multiple types within the same keyspeace, we need to validate that
         * tables are logically separated.
         */

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.create())) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                Message review = s.getDataBuilder("RestaurantReviewer")
                        .setField("id", 1L)
                        .setField("name", "Jane Doe")
                        .setField("email", "isabel.hawthowrne@apples.com")
                        .setField("stats", s.getDataBuilder("ReviewerStats")
                                .setField("school_name", "l'ecole populaire")
                                .setField("hometown", "Athens, GA")
                                .setField("start_date", 12L)
                                .build())
                        .build();

                Message restaurant = s.getDataBuilder("RestaurantRecord")
                        .setField("rest_no", 2L)
                        .setField("name", "Burgers Burgers")
                        .setField("location", s.getDataBuilder("Location")
                                .setField("address", "12345 Easy Street")
                                .build())
                        .addRepeatedField("tags", s.getDataBuilder("RestaurantTag")
                                .setField("tag", "title-123")
                                .setField("weight", 1L)
                                .build())
                        .addRepeatedField("reviews", s.getDataBuilder("RestaurantReview")
                                .setField("reviewer", 1L)
                                .setField("rating", 1L)
                                .build())
                        .build();

                //insert the review
                Assertions.assertThat(s.executeInsert("RestaurantReviewer", review, Options.create())).isEqualTo(1);
                //insert the restaurant
                Assertions.assertThat(s.executeInsert("RestaurantRecord", restaurant, Options.create())).isEqualTo(1);

                //now make sure that you don't get back the other one
                try (RelationalResultSet rrs = s.executeGet("RestaurantRecord", new KeySet().setKeyColumn("rest_no", 1L), Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasNoNextRow();
                }

                try (RelationalResultSet rrs = s.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("id", 2L), Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasNoNextRow();
                }

                //make sure you get back the correct rows from the correct tables
                try (RelationalResultSet rrs = s.executeGet("RestaurantRecord", new KeySet().setKeyColumn("rest_no", 2L), Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasExactly(new MessageTuple(restaurant));
                }
                try (RelationalResultSet rrs = s.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("id", 1L), Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasExactly(new MessageTuple(review));
                }

                //now scan the data and see if too much comes back
                TableScan restaurantScan = new TableScan("RestaurantRecord", KeySet.EMPTY, KeySet.EMPTY, QueryProperties.DEFAULT);
                try (RelationalResultSet rrs = s.executeScan(restaurantScan, Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasExactly(new MessageTuple(restaurant));
                }

                TableScan reviewScan = new TableScan("RestaurantReviewer", KeySet.EMPTY, KeySet.EMPTY, QueryProperties.DEFAULT);
                try (RelationalResultSet rrs = s.executeScan(reviewScan, Options.create())) {
                    RelationalAssertions.assertThat(rrs).hasExactly(new MessageTuple(review));
                }
            }
        }
    }
}
