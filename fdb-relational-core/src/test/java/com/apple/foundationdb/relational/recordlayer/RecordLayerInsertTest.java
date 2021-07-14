/*
 * RecordLayerInsertTest.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

public class RecordLayerInsertTest {
    @RegisterExtension
    public final KeySpaceRule keySpace = new KeySpaceRule("record_layer_insert_test", "testInsert",
            metaDataBuilder -> metaDataBuilder.setRecords(Restaurant.getDescriptor())
                    .getRecordType("RestaurantRecord")
                    .setPrimaryKey(Key.Expressions.field("name")));



    @Test
    void canQueryWithRange() throws Exception {
        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + System.currentTimeMillis()).setRestNo(1L).build();
        System.out.println(r.getName());
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            try(RelationalResultSet resultSet = statement.executeQuery("select * from RestaurantRecord where name >= '" + r.getName() + "' and name < 'testRest99999999999'", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        String n = resultSet.getString("name");
                        System.out.println("name: " + n);
                    }
                }
            }
        }
    }

    @Test
    void canExtractNonNestedField() throws Exception {
        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + System.currentTimeMillis()).setRestNo(1L).build();
        System.out.println(r.getName());
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {
            recStoreConn.setSchema("test");

            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            // TODO(bfines) representing that I want to query the whole message here is awkward when saying "*"
            try(RelationalResultSet resultSet = statement.executeQuery("select name from RestaurantRecord where name >= '" + r.getName() + "'", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        String n = resultSet.getString("name");
                        System.out.println("name: " + n);
                    }
                }
            }
        }
    }

    @Test
    void canExtractNestedRepeatedField() throws Exception {

        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder()
                .setName("testRest" + System.currentTimeMillis()).setRestNo(1L)
                .addReviews(Restaurant.RestaurantReview.newBuilder()
                        .setReviewer(21L)
                        .setRating(4)
                        .build())
                .addReviews(Restaurant.RestaurantReview.newBuilder()
                        .setReviewer(23L)
                        .setRating(2)
                        .build())
                .build();
        System.out.println(r.getName());
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {
            recStoreConn.setSchema("test");

            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            // TODO(bfines) representing that I want to query the whole message here is awkward when saying "*"
            try(RelationalResultSet resultSet = statement.executeQuery("select RestaurantRecord.\"reviews\".\"reviewer\" from RestaurantRecord", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        Iterable<?> reviews = resultSet.getRepeated("reviewer");
                        for (Object review : reviews) {
                            System.out.println("review: " + review);
                        }
                        try {
                            String name = resultSet.getString("name");
                            Assertions.fail("Should not have found name!");
                        } catch (RelationalException ve) {
                            Assertions.assertEquals(RelationalException.ErrorCode.NO_SUCH_FIELD, ve.getErrorCode(), "Incorrect error code");
                        }
                    }
                }
            }
        }
    }

    @Test
    void canExtractRepeatedField() throws Exception {

        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder()
                .setName("testRest" + System.currentTimeMillis()).setRestNo(1L)
                .addReviews(Restaurant.RestaurantReview.newBuilder()
                        .setReviewer(21L)
                        .setRating(4)
                        .build())
                .addReviews(Restaurant.RestaurantReview.newBuilder()
                        .setReviewer(23L)
                        .setRating(2)
                        .build())
                .build();
        System.out.println(r.getName());

        try (RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
             Statement statement = recStoreConn.createStatement()) {
            recStoreConn.setSchema("test");
            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            // TODO(bfines) representing that I want to query the whole message here is awkward when saying "*"
            try (RelationalResultSet resultSet = statement.executeQuery("select reviews from RestaurantRecord", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        Iterable<?> reviews = resultSet.getRepeated("reviews");
                        for (Object review : reviews) {
                            System.out.println("review: " + review);
                        }
                        try {
                            String name = resultSet.getString("name");
                            Assertions.fail("Should not have found name!");
                        } catch (RelationalException ve) {
                            Assertions.assertEquals(RelationalException.ErrorCode.NO_SUCH_FIELD, ve.getErrorCode(), "Incorrect error code");
                        }
                    }
                }
            }
        }
    }

    @Test
    void canQueryWithGreaterThanOrEqual() throws Exception {

        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + System.currentTimeMillis()).setRestNo(1L).build();
        System.out.println(r.getName());
        try (RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
             Statement statement = recStoreConn.createStatement()) {
            recStoreConn.setSchema("test");

            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            // TODO(bfines) representing that I want to query the whole message here is awkward when saying "*"
            try (RelationalResultSet resultSet = statement.executeQuery("select * from RestaurantRecord where name >= '" + r.getName() + "'", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        String n = resultSet.getString("name");
                        System.out.println("name: " + n);
                    }
                }
            }
        }
    }

    @Test
    void canRunUnboundedQuery() throws Exception {

        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + System.currentTimeMillis()).setRestNo(1L).build();
        System.out.println(r.getName());

        try (RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
             Statement statement = recStoreConn.createStatement()) {
            recStoreConn.setSchema("test");
            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            // TODO(bfines) representing that I want to query the whole message here is awkward when saying "*"
            try(RelationalResultSet resultSet = statement.executeQuery("select * from RestaurantRecord", Options.create())) {
                while (resultSet.next()) {
                    if (resultSet.supportsMessageParsing()) {
                        final Message row = resultSet.parseMessage();
                        System.out.println(row);
                    } else {
                        String n = resultSet.getString("name");
                        System.out.println("name: " + n);
                    }
                }
            }
        }
    }
}
