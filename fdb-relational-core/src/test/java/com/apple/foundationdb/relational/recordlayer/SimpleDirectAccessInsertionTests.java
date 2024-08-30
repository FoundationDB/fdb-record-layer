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

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

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
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(relationalExtension, SimpleDirectAccessInsertionTests.class, TestSchemas.restaurant());

    @Test
    void insertNestedFields() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final var struct = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addString("NAME", "Anthony Bourdain")
                        .addString("EMAIL", "abourdain@apple.com")
                        .addStruct("STATS", EmbeddedRelationalStruct.newBuilder()
                                .addLong("START_DATE", 0L)
                                .addString("SCHOOL_NAME", "Truman High School")
                                .addString("HOMETOWN", "Boise, Indiana")
                                .build())
                        .build();
                int inserted = s.executeInsert("RESTAURANT_REVIEWER", struct);
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!").isEqualTo(1);
                KeySet key = new KeySet()
                        .setKeyColumn("ID", 1L);
                try (RelationalResultSet rrs = s.executeGet("RESTAURANT_REVIEWER", key, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .isRowExactly(struct)
                            .hasNoNextRow();
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

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final var reviewerStruct = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addString("NAME", "Jane Doe")
                        .addString("EMAIL", "isabel.hawthowrne@apples.com")
                        .addStruct("STATS", EmbeddedRelationalStruct.newBuilder()
                                .addLong("START_DATE", 12L)
                                .addString("SCHOOL_NAME", "l'ecole populaire")
                                .addString("HOMETOWN", "Athens, GA")
                                .build())
                        .build();
                final var restaurantStruct = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", 2L)
                        .addString("NAME", "Burgers Burgers")
                        .addStruct("LOCATION", EmbeddedRelationalStruct.newBuilder()
                                .addString("ADDRESS", "12345 Easy Street")
                                .build())
                        .addArray("TAGS", EmbeddedRelationalArray.newBuilder()
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("TAG", "title-123")
                                        .addLong("WEIGHT", 1L)
                                        .build())
                                .build())
                        .addArray("REVIEWS", EmbeddedRelationalArray.newBuilder()
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addLong("REVIEWER", 1L)
                                        .addLong("RATING", 1L)
                                        .build())
                                .build())
                        .build();

                //insert the review
                Assertions.assertThat(s.executeInsert("RESTAURANT_REVIEWER", reviewerStruct)).isEqualTo(1);
                //insert the restaurant
                Assertions.assertThat(s.executeInsert("RESTAURANT", restaurantStruct)).isEqualTo(1);

                //now make sure that you don't get back the other one
                try (RelationalResultSet rrs = s.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                try (RelationalResultSet rrs = s.executeGet("RESTAURANT_REVIEWER", new KeySet().setKeyColumn("ID", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                //make sure you get back the correct rows from the correct tables
                try (RelationalResultSet rrs = s.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumn("REST_NO", 2L)
                            .hasColumn("NAME", "Burgers Burgers")
                            .hasNoNextRow();
                }
                try (RelationalResultSet rrs = s.executeGet("RESTAURANT_REVIEWER", new KeySet().setKeyColumn("ID", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .isRowExactly(reviewerStruct)
                            .hasNoNextRow();
                }

                //now scan the data and see if too much comes back
                try (RelationalResultSet rrs = s.executeScan("RESTAURANT", KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumn("REST_NO", 2L)
                            .hasColumn("NAME", "Burgers Burgers")
                            .hasNoNextRow();
                }

                try (RelationalResultSet rrs = s.executeScan("RESTAURANT_REVIEWER", KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .isRowExactly(reviewerStruct)
                            .hasNoNextRow();
                }
            }
        }
    }
}
