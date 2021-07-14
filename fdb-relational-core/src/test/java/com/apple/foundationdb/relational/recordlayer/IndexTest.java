/*
 * IndexTest.java
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

import com.apple.foundationdb.record.Leaderboard;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public class IndexTest {
    @RegisterExtension
    public final KeySpaceRule keySpace = new KeySpaceRule("record_layer_index_test", "test",
            metaDataBuilder -> {
                metaDataBuilder.setRecords(Restaurant.getDescriptor())
                        .getRecordType("RestaurantRecord")
                        .setPrimaryKey(field("rest_no"));

                metaDataBuilder.addIndex("RestaurantRecord","addr_idx", Key.Expressions.concat(
                        field("rest_no"),
                        field("location").nest(field("address"))
                ));

//                metaDataBuilder.addIndex("RestaurantRecord","review_idx", Key.Expressions.concat(
//                        field("rest_no"),
//                        field("reviews", KeyExpression.FanType.FanOut).nest(field("rating"))
//                ));

                metaDataBuilder.addIndex("RestaurantRecord","tags_idx", Key.Expressions.concat(
                        field("rest_no"),
                        field("tags", KeyExpression.FanType.FanOut).nest(field("tag"))
                ));

                metaDataBuilder.addIndex("RestaurantRecord","rest_no_name_idx",Key.Expressions.concat(
                        field("rest_no"),
                        field("name")
                ));

            }
    );

    @Test
    void canQueryWithNonPrimaryKey() {
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()){

            Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testIndexedRestaurant").setRestNo(System.currentTimeMillis()).build();
            System.out.println(r.getName());


            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            try(RelationalResultSet resultSet = statement.executeQuery("select name from RestaurantRecord where name = '" + r.getName() + "'", Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                if (resultSet.supportsMessageParsing()) {
                    final Message row = resultSet.parseMessage();
                    System.out.println(row);
                } else {
                    String name = resultSet.getString("name");
                    Assertions.assertEquals(name, r.getName(), "Did not match names");
                }
            }
        }
    }

    @Test
    void canQueryUnionOfDifferentTypes() {
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            String baseQuery = "select record.rest_no,record.location.latitude from RestaurantRecord record union select reviewer.id,reviewer.email from RestaurantReviewer reviewer";
            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for " + baseQuery, Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                System.out.println(resultSet.getString(0));
            }
        }
    }

    @Test
    void canQueryAndOperations() {
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            String baseQuery = "select rest_no from RestaurantRecord where rest_no < 12 intersect select rest_no from RestaurantRecord where rest_no > 5";
            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for " + baseQuery, Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                System.out.println(resultSet.getString(0));
            }
        }
    }

    @Test
    void canDoCorrelatedSubquery() {
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            String baseQuery = "SELECT r.\"rest_no\",t.tag  from RestaurantRecord r, UNNEST(r.\"tags\") as t where t.tag = 'foo'";
            System.out.println(baseQuery);
            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for " + baseQuery, Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                System.out.println(resultSet.getString(0));
            }
        }
    }

    @Test
    void canDoMultipleMaterializations(){
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection()) {
            String baseQuery = "SELECT R1.name,R2.location.address from RestaurantRecord R1, RestaurantRecord R2 where R1.name = 'foo' and R2.rest_no = 123 and R1.rest_no = R2.rest_no";
//                ", RestaurantRecord R2" +
//                "where " +
//                "R1.name = 'foo' ";
//                "and R2.location.address =  12345" +
//                "and R1.rest_no = R2.rest_no";

            explain(recStoreConn, baseQuery);
        }
    }

    private void explain(RecordStoreConnection recStoreConn, String baseQuery) {
        System.out.println(baseQuery);
        try(Statement s = recStoreConn.createStatement()) {
            System.out.println(s.explainQuery(baseQuery, Options.create()));
        }
//        RelationalResultSet resultSet = recStoreConn.query("explain plan for "+baseQuery, Options.create());
//        Assertions.assertTrue(resultSet.next(), "Did not return rows");
//        System.out.println(resultSet.getString(0));
    }

    @Test
    void canQueryNestedIndexedField(){
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            String baseQuery = "select rest_no, RestaurantRecord.location.address from RestaurantRecord where rest_no=12";
            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for " + baseQuery, Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                System.out.println(resultSet.getString(0));
            }
        }
    }

    @Test
    void canQueryNestedRepeatedIndexedField(){
        try(RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
            Statement statement = recStoreConn.createStatement()) {

            String baseQuery = "select rest_no, RestaurantRecord.reviews.rating from RestaurantRecord where rest_no=12";
            try (RelationalResultSet resultSet = statement.executeQuery("explain plan for " + baseQuery, Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                System.out.println(resultSet.getString(0));
            }
        }
    }

    @Test
    void canQueryWithIndexHint() {
        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testIndexedRestaurant").setRestNo(System.currentTimeMillis()).build();
        System.out.println(r.getName());

        try (RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
             Statement statement = recStoreConn.createStatement()) {

            statement.executeInsert("RestaurantRecord", Collections.singleton(r), Options.create());

            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for select name from RestaurantRecord/*+ INDEX(RestaurantRecord$name) */ where name = '" + r.getName() + "'", Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                if (resultSet.supportsMessageParsing()) {
                    final Message row = resultSet.parseMessage();
                    System.out.println(row);
                } else {
                    String name = resultSet.getString(0);
                    System.out.println(name);
//            Assertions.assertEquals(name,r.getName(),"Did not match names");
                }
            }
        }
    }
}
