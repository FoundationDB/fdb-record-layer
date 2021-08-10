/*
 * CreateDatabaseTest.java
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
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

/**
 * Tests around database creation (error handling, correctness, etc.)
 */
public class CreateDatabaseTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @Test
    void canCreateDatabase() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate(URI.create("/Restaurant"), builder.build()));

        catalog.createDatabase(URI.create("/test_table"),
                DatabaseTemplate.newBuilder()
                        .withSchema("test", "Restaurant")
                        .build());

        final RelationalDatabase database = catalog.getDatabase(URI.create("/test_table"));
        Assertions.assertNotNull(database,"No database returned!");
    }

    @Test
    void cannotCreateDatabaseWithBadSchemaTemplate() {
        /*
         * Tests that we fail to create a database if we specify a SchemaTemplate which is not present in the
         * catalog
         */
        final DatabaseTemplate template = DatabaseTemplate.newBuilder()
                .withSchema("test", "NoSuchSchemaTemplate")
                .build();
        RelationalException ve = Assertions.assertThrows(RelationalException.class, ()->catalog.createDatabase(URI.create("/test_table"), template));
        Assertions.assertEquals(RelationalException.ErrorCode.UNKNOWN_SCHEMA_TEMPLATE,ve.getErrorCode(),"Incorect error code!");
    }

    @Test
    void cannotLoadNonExistentDatabase() {
        RelationalException ve = Assertions.assertThrows(RelationalException.class,()->catalog.getDatabase(URI.create("/no_such_database")));
        Assertions.assertEquals(RelationalException.ErrorCode.UNDEFINED_DATABASE,ve.getErrorCode(),"Incorrect error code!");
    }
}
