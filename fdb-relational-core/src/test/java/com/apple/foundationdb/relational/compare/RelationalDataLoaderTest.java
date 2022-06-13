/*
 * RelationalDataLoaderTest.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Tests about the functionality of converting a nested structure to a relational SQL type.
 */
@Disabled("disabled until comparison testing API is further resolved")
public class RelationalDataLoaderTest {

    @Test
    void canDoNonNestedProtobufs() {
        Table tableStructure = RelationalStructure.createFullStructure(Restaurant.ReviewerStats.getDescriptor()).iterator().next();

        Assertions.assertEquals("CREATE TABLE ReviewerStats(RECORD_ID INTEGER,start_date BIGINT,school_name VARCHAR(65535),hometown VARCHAR(65535))",
                tableStructure.getCreateStatement());
    }

    @Test
    void handlesNestedProtobuf() {
        Set<Table> structures = RelationalStructure.createFullStructure(Restaurant.RestaurantRecord.getDescriptor());
        System.out.println("");
        System.out.println("");
        for (Table tableStructure : structures) {
            System.out.println(tableStructure.getCreateStatement());
        }
        System.out.println("");
        System.out.println("");
    }

    @Test
    void testFlattenToValues() {
        Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder()
                .setRestNo(2L)
                .setName("Burgers Burgers")
                .setLocation(Restaurant.Location.newBuilder().setAddress("123 Main Street").build())
                .addTags(Restaurant.RestaurantTag.newBuilder().setTag("this is a taq").setWeight(24).build())
                .build();

        RelationalStructure structure = new RelationalStructure(record.getDescriptorForType());
        final Map<String, ValuesClause> map = structure.flattenToValues(record);
        for (Map.Entry<String, ValuesClause> entry : map.entrySet()) {
            String tableName = entry.getKey();
            ValuesClause clause = entry.getValue();
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName).append("(")
                    .append(clause.columnList()).append(")")
                    .append(" VALUES (").append(clause.valuesString()).append(")");

            System.out.println(sb);
        }
    }

    @Test
    void h2Load() throws Exception {
        Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder()
                .setRestNo(2L)
                .setName("Burgers Burgers")
                .setLocation(Restaurant.Location.newBuilder().setAddress("123 Main Street").build())
                .addTags(Restaurant.RestaurantTag.newBuilder().setTag("this is a tag").setWeight(24).build())
                .addTags(Restaurant.RestaurantTag.newBuilder().setTag("yet another tag").setWeight(13).build())
                .build();

        Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder()
                .setEmail("blobbityblib@bloo.foo")
                //                .setId(23L)
                .setName("Scott Fines")
                .setStats(Restaurant.ReviewerStats.newBuilder()
                        .setSchoolName("Truman High")
                        .setHometown("Indep Mo")
                        .setStartDate(123456L)
                        .build())
                .build();
        //register the driver
        Class.forName("org.h2.Driver");

        //<./test> = current working directory/test -- should ultimately be src/test/resources/h2Data or something
        try (final Connection h2Conn = DriverManager.getConnection("jdbc:h2:./src/test/resources/test")) {
            RelationalCatalog catalog = new RelationalCatalog();
            catalog.loadStructure("RestaurantRecord", Restaurant.RestaurantRecord.getDescriptor());
            catalog.loadStructure("RestaurantReviewer", Restaurant.RestaurantReviewer.getDescriptor());

            JDBCDatabaseConnection jdbcConn = new JDBCDatabaseConnection(catalog, h2Conn);
            jdbcConn.beginTransaction();

            try (RelationalStatement s = jdbcConn.createStatement()) {
                catalog.createTables(s);
                try {

                    int count = s.executeInsert("RestaurantRecord", Collections.singleton(record).iterator());
                    System.out.println(count);
                    count = s.executeInsert("RestaurantReviewer", Collections.singleton(reviewer).iterator());
                    System.out.println(count);

                    KeySet ks = new KeySet();
                    //                    ks.setKeyColumn("name", "'Scott Fines'");
                    ks.setKeyColumn("rest_no", 2L);
                    try (final RelationalResultSet rrs = s.executeGet("RestaurantRecord", ks, Options.NONE)) {
                        if (!rrs.next()) {
                            System.out.println("NOT FOUND!");
                        }
                    }
                } finally {
                    catalog.dropTables(s);
                }
            }
        }
    }

    @Test
    void utf8Stuff() {
        RandomDataGenerator generator = new RandomDataGenerator(new Random(0), Restaurant.RestaurantRecord.getDescriptor(), 10, 10, 10, 10);
        Message message;
        while ((message = generator.generateNext()) != null) {
            System.out.println(message);
        }
    }
}
