/*
 * InsertTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;

import java.net.URI;
import java.util.Collections;

public class InsertTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate(URI.create("/Restaurant"), builder.build()));

        catalog.createDatabase(URI.create("/dbId"),
                DatabaseTemplate.newBuilder()
                        .withSchema("main", "Restaurant")
                        .build());
    }

    @Test
    void canInsertWithMultipleRecordTypes() {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        RelationalDriver driver = new RecordLayerDriver(catalog);
        try (DatabaseConnection conn = driver.connect(URI.create("/dbId"), Options.create())){
            conn.setSchema("/dbId/main");
            conn.beginTransaction();
            try(Statement s = conn.createStatement()){
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord"+id).build();
                int inserted = s.executeInsert("RestaurantRecord", Collections.singleton(record),Options.create());
                Assertions.assertEquals(1,inserted, "Did not insert properly!");

                Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder().setName("reviewerName"+id).setId(id).build();
                inserted = s.executeInsert("RestaurantReviwer",Collections.singleton(reviewer),Options.create());
                Assertions.assertEquals(1,inserted, "Did not insert reviewers properly!");
            }
        }
    }

    @Test
    void cannotInsertWithIncorrectTypeForRecord() {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        RelationalDriver driver = new RecordLayerDriver(catalog);
        try (DatabaseConnection conn = driver.connect(URI.create("/dbId/databaseId"), Options.create())){
            conn.setSchema("main");
            conn.beginTransaction();
            try(Statement s = conn.createStatement()){
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord"+id).build();
                RelationalException ve = Assertions.assertThrows(RelationalException.class, () -> s.executeInsert("RestaurantReviewer", Collections.singleton(record),Options.create()));
                Assertions.assertEquals(RelationalException.ErrorCode.INVALID_PARAMETER,ve.getErrorCode(),"Incorrect error code");
            }
        }
    }

    @Test
    void cannotInsertWithMissingSchema() {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        RelationalDriver driver = new RecordLayerDriver(catalog);
        try (DatabaseConnection conn = driver.connect(URI.create("/dbId/databaseId"), Options.create())){
            conn.setSchema("doesNotExist");
            conn.beginTransaction();
            try(Statement s = conn.createStatement()){
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord"+id).build();
                RelationalException thrown = Assertions.assertThrows(RelationalException.class, () -> {
                    int inserted = s.executeInsert("RestaurantReviewer", Collections.singleton(record), Options.create());
                });
                Assertions.assertEquals(RelationalException.ErrorCode.UNKNOWN_SCHEMA,thrown.getErrorCode(),"Incorrect error code returned!");
            }
        }
    }
}
