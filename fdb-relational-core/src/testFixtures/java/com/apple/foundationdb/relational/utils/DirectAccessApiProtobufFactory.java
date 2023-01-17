/*
 * DirectAccessApiProtobufFactory.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.Message;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;

/**
 * Test fixture for making protobufs for use in Direct Access API tests.
 * This fixture is for test use by modules other than fdb-relational-core where use of the  embedded jdbc driver or
 * access to core types is awkward (e.g. in fdb-relational-jdbc module with its own jdbc driver). This fixture will
 * likely have a short life as we undo inserting raw protobuf from direct access api.
 * The protobuf messages made below come from the recordlayer/SimpleDirectAccessInsertionTests unit test.
 */
public class DirectAccessApiProtobufFactory {
    public static final String REVIEWER = "RESTAURANT_REVIEWER";
    public static final String RESTAURANT = "RESTAURANT";

    /**
     * Make the anthony bourdain reviewer protobuf Message that recordlayer/SimpleDirectAccessInsertionTests does in
     * its insertNestedFields test. Presumes an already registered embedded jdbc driver.
     */
    public static Message createAnthonyBourdainReviewer(URI databasePath, String schemaName) throws Exception {
        Message message = null;
        try (var conn =
                 Relational.connect(URI.create("jdbc:embed://" + databasePath.getPath()), Options.NONE)) {
            conn.setSchema(schemaName);
            try (RelationalStatement s = conn.createStatement()) {
                final DynamicMessageBuilder builder = s.getDataBuilder(REVIEWER);
                builder.setField("ID", 1L);
                builder.setField("NAME", "Anthony Bourdain");
                builder.setField("EMAIL", "abourdain@apple.com");
                builder.setField("STATS",
                        s.getDataBuilder(REVIEWER, List.of("STATS"))
                            .setField("SCHOOL_NAME", "Truman High School")
                            .setField("HOMETOWN", "Boise, Indiana")
                            .setField("START_DATE", 0L)
                            .build());
                message = builder.build();
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
        return message;
    }

    /**
     * Make the Jane Doe reviewer protobuf Message that recordlayer/SimpleDirectAccessInsertionTests does in
     * its insertMultipleTablesDontMix test. Presumes an already registered embedded jdbc driver.
     */
    public static Message createIsabelHawthornReviewer(URI databasePath, String schemaName) throws SQLException {
        Message message = null;
        try (var conn =
                     Relational.connect(URI.create("jdbc:embed://" + databasePath.getPath()), Options.NONE)) {
            conn.setSchema(schemaName);
            try (RelationalStatement s = conn.createStatement()) {
                message = s.getDataBuilder(REVIEWER)
                        .setField("ID", 1L)
                        .setField("NAME", "Jane Doe")
                        .setField("EMAIL", "isabel.hawthowrne@apples.com")
                        .setField("STATS",
                                s.getDataBuilder(REVIEWER, List.of("STATS"))
                                        .setField("SCHOOL_NAME", "l'ecole populaire")
                                        .setField("HOMETOWN", "Athens, GA")
                                        .setField("START_DATE", 12L)
                                        .build())
                        .build();
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
        return message;
    }

    /**
     * Make the Burgers Burgers Restaurant protobuf Message that recordlayer/SimpleDirectAccessInsertionTests does in
     * its insertMultipleTablesDontMix test. Presumes an already registered embedded jdbc driver.
     */
    public static Message createBurgersBurgersRestaurant(URI databasePath, String schemaName) throws SQLException {
        Message message = null;
        try (var conn =
                     Relational.connect(URI.create("jdbc:embed://" + databasePath.getPath()), Options.NONE)) {
            conn.setSchema(schemaName);
            try (RelationalStatement s = conn.createStatement()) {
                message = s.getDataBuilder(RESTAURANT)
                        .setField("REST_NO", 2L)
                        .setField("NAME", "Burgers Burgers")
                        .setField("LOCATION",
                                s.getDataBuilder(RESTAURANT, List.of("LOCATION"))
                                        .setField("ADDRESS", "12345 Easy Street")
                                        .build())
                        .addRepeatedFields("TAGS",
                                List.of(s.getDataBuilder(RESTAURANT, List.of("TAGS"))
                                        .setField("TAG", "title-123")
                                        .setField("WEIGHT", 1L)
                                        .build()))
                        .addRepeatedFields("REVIEWS",
                                List.of(s.getDataBuilder(RESTAURANT, List.of("REVIEWS"))
                                        .setField("REVIEWER", 1L)
                                        .setField("RATING", 1L)
                                        .build()))
                        .build();
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
        return message;
    }
}
