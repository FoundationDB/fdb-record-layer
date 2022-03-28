/*
 * RecordMetaDataStoreImplTest.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RecordMetaDataStoreImplTest {
    @Test
    void testRecordMetaDataBuild() throws InvalidProtocolBufferException, RelationalException {
        CatalogData.Schema schema = getGoodSchema();
        RecordMetaData recordMetaData = RecordMetaDataStoreImpl.buildRecordMetaData(schema).getRecordMetaData();

        Assertions.assertEquals("restaurant.proto", recordMetaData.getRecordsDescriptor().getFullName());
        Assertions.assertEquals("com.apple.foundationdb.record.test4", recordMetaData.getRecordsDescriptor().getPackage());
        Assertions.assertEquals(2, recordMetaData.getRecordTypes().size());
        Assertions.assertNotNull(recordMetaData.getRecordType("RestaurantRecord"));
        Assertions.assertNotNull(recordMetaData.getRecordType("RestaurantReviewer"));
        Assertions.assertEquals(Key.Expressions.field("rest_no"), recordMetaData.getRecordType("RestaurantRecord").getPrimaryKey());
        Assertions.assertEquals(Key.Expressions.concatenateFields("id", "name"), recordMetaData.getRecordType("RestaurantReviewer").getPrimaryKey());
    }

    // test with wrong table name
    @Test
    void testWrongTableName() {
        CatalogData.Schema goodSchema = getGoodSchema();
        // add 1 table with a wrong name
        CatalogData.Table badTable = CatalogData.Table.newBuilder()
                .setName("wrong_table_name")
                .setPrimaryKey(RecordMetaDataProto.KeyExpression.newBuilder().setField(RecordMetaDataProto.Field.newBuilder().setFieldName("rest_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR).build()).build())
                .build();
        CatalogData.Schema badSchema = goodSchema.toBuilder().addTables(badTable).build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            RecordMetaDataStoreImpl.buildRecordMetaData(badSchema).getRecordMetaData();
        });
        Assertions.assertEquals(ErrorCode.UNDEFINED_TABLE, exception.getErrorCode());
        Assertions.assertEquals("com.apple.foundationdb.record.metadata.MetaDataException: Unknown record type wrong_table_name", exception.getMessage());
    }

    @Test
    void testMissingPrimaryKey() {
        CatalogData.Schema goodSchema = getGoodSchema();
        CatalogData.Table badTable = goodSchema.getTables(0).toBuilder().clearPrimaryKey().build();
        CatalogData.Schema badSchema = goodSchema.toBuilder().setTables(0, badTable).build();
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
            RecordMetaDataStoreImpl.buildRecordMetaData(badSchema).getRecordMetaData();
        });
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
    }

    private CatalogData.Schema getGoodSchema() {
        // primary key = 1 scalar field
        CatalogData.Table table1 = CatalogData.Table.newBuilder()
                .setName("RestaurantRecord")
                .setPrimaryKey(RecordMetaDataProto.KeyExpression.newBuilder().setField(RecordMetaDataProto.Field.newBuilder().setFieldName("rest_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR).build()).build())
                .build();
        // primary key = concat of 2 scalar fields
        RecordMetaDataProto.KeyExpression key1 = RecordMetaDataProto.KeyExpression.newBuilder().setField(RecordMetaDataProto.Field.newBuilder().setFieldName("id").setFanType(RecordMetaDataProto.Field.FanType.SCALAR)).build();
        RecordMetaDataProto.KeyExpression key2 = RecordMetaDataProto.KeyExpression.newBuilder().setField(RecordMetaDataProto.Field.newBuilder().setFieldName("name").setFanType(RecordMetaDataProto.Field.FanType.SCALAR)).build();
        CatalogData.Table table2 = CatalogData.Table.newBuilder()
                .setName("RestaurantReviewer")
                .setPrimaryKey(RecordMetaDataProto.KeyExpression.newBuilder().setThen(RecordMetaDataProto.Then.newBuilder().addChild(key1).addChild(key2).build()).build())
                .build();
        return CatalogData.Schema.newBuilder()
                .setSchemaName("restaurant")
                .setDatabaseId("test_database_id")
                .setSchemaTemplateName("test_schema_template_name")
                .setSchemaVersion(1)
                .setRecord(Restaurant.getDescriptor().toProto().toByteString())
                .addTables(table1)
                .addTables(table2)
                .build();
    }

}
