/*
 * MessageTupleTest.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MessageTupleTest {
    Message record = restaurantMessageBuilder(true).build();
    MessageTuple tuple = new MessageTuple(record);
    Message recordWithNonNullableArray = restaurantMessageBuilder(false).build();
    MessageTuple tupleWithNonNullableArray = new MessageTuple(recordWithNonNullableArray);

    MessageTupleTest() throws Exception {
    }

    @Test
    void getNumFields() {
        assertThat(tuple.getNumFields()).isEqualTo(7);
        assertThat(tupleWithNonNullableArray.getNumFields()).isEqualTo(7);
    }

    @Test
    void getObjectNullableArray() throws InvalidColumnReferenceException {
        // position 0: int64 rest_no, is null if unset
        assertThat(tuple.getObject(0)).isEqualTo(null);
        // position 1: string name, is null if unset
        assertThat(tuple.getObject(1)).isEqualTo(null);
        // position 2: Location location, is null if unset
        assertThat(tuple.getObject(2)).isEqualTo(null);
        // position 3-5 are nullable arrays, is null if unset
        assertThat(tuple.getObject(3)).isEqualTo(null);
        assertThat(tuple.getObject(4)).isEqualTo(null);
        assertThat(tuple.getObject(5)).isEqualTo(null);

        RelationalAssertions.assertThrows(
                () -> tuple.getObject(-1))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
        RelationalAssertions.assertThrows(
                () -> tuple.getObject(10))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Test
    void getObjectNonNullableArray() throws InvalidColumnReferenceException {
        // position 0: int64 rest_no, is null if unset
        assertThat(tupleWithNonNullableArray.getObject(0)).isEqualTo(null);
        // position 1: string name, is null if unset
        assertThat(tupleWithNonNullableArray.getObject(1)).isEqualTo(null);
        // position 2: Location location, is null if unset
        assertThat(tupleWithNonNullableArray.getObject(2)).isEqualTo(null);
        // position 3-4 are non-nullable arrays, is empty list if unset
        assertThat(tupleWithNonNullableArray.getObject(3)).isEqualTo(Collections.emptyList());
        assertThat(tupleWithNonNullableArray.getObject(4)).isEqualTo(Collections.emptyList());
        // position 5 is nullable array, is null if unset
        assertThat(tupleWithNonNullableArray.getObject(5)).isEqualTo(null);

        RelationalAssertions.assertThrows(
                () -> tupleWithNonNullableArray.getObject(-1))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
        RelationalAssertions.assertThrows(
                () -> tupleWithNonNullableArray.getObject(10))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Test
    void parseMessage() {
        Assertions.assertEquals(record, tuple.parseMessage());
        Assertions.assertEquals(recordWithNonNullableArray, tupleWithNonNullableArray.parseMessage());
    }

    @Nonnull
    private DynamicMessage.Builder restaurantMessageBuilder(boolean nullableArray) throws Exception {
        final var location = RecordLayerTable.newBuilder()
                .setName("LOCATION")
                .addColumns(List.of(
                        RecordLayerColumn.newBuilder().setName("ADDRESS").setDataType(DataType.Primitives.STRING.type()).build(),
                        RecordLayerColumn.newBuilder().setName("LATITUDE").setDataType(DataType.Primitives.STRING.type()).build(),
                        RecordLayerColumn.newBuilder().setName("LONGITUDE").setDataType(DataType.Primitives.STRING.type()).build()))
                .build();

        final var restaurantReview = RecordLayerTable.newBuilder()
                .setName("RESTAURANT_REVIEW")
                .addColumns(List.of(
                        RecordLayerColumn.newBuilder().setName("REVIEWER").setDataType(DataType.Primitives.LONG.type()).build(),
                        RecordLayerColumn.newBuilder().setName("RATING").setDataType(DataType.Primitives.LONG.type()).build()))
                .build();

        final var restaurantTag = RecordLayerTable.newBuilder()
                .setName("RESTAURANT_TAG")
                .addColumns(List.of(
                        RecordLayerColumn.newBuilder().setName("TAG").setDataType(DataType.Primitives.STRING.type()).build(),
                        RecordLayerColumn.newBuilder().setName("WEIGHT").setDataType(DataType.Primitives.LONG.type()).build()))
                .build();

        final var restaurant = RecordLayerTable.newBuilder()
                .setName("RESTAURANT")
                .addColumns(List.of(
                        RecordLayerColumn.newBuilder().setName("REST_NO").setDataType(DataType.Primitives.STRING.type()).build(),
                        RecordLayerColumn.newBuilder().setName("NAME").setDataType(DataType.Primitives.LONG.type()).build(),
                        RecordLayerColumn.newBuilder().setName("LOCATION").setDataType(location.getDatatype().withNullable(false)).build(),
                        RecordLayerColumn.newBuilder().setName("REVIEWS").setDataType(DataType.ArrayType.from(restaurantReview.getDatatype(), nullableArray)).build(),
                        RecordLayerColumn.newBuilder().setName("TAGS").setDataType(DataType.ArrayType.from(restaurantTag.getDatatype(), nullableArray)).build(),
                        RecordLayerColumn.newBuilder().setName("CUSTOMER").setDataType(DataType.Primitives.STRING.type()).build(),
                        RecordLayerColumn.newBuilder().setName("ENCODED_BYTES").setDataType(DataType.Primitives.BYTES.type()).build()))
                .build();

        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("testTemplate")
                .setVersion(1)
                .addTable(restaurant)
                .build();

        Descriptors.Descriptor descriptor = Descriptors.FileDescriptor.buildFrom(schemaTemplate.toRecordMetadata().toProto().getRecords(), new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()})
                .getMessageTypes()
                .stream()
                .filter(m -> "RESTAURANT".equals(m.getName()))
                .findFirst()
                .get();

        return DynamicMessage.newBuilder(descriptor);
    }
}
