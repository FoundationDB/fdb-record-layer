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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    private DynamicMessage.Builder restaurantMessageBuilder(boolean nullableArray) throws Exception {
        List<TypingContext.FieldDefinition> locationColumns = List.of(
                new TypingContext.FieldDefinition("ADDRESS", Type.TypeCode.STRING, null, false),
                new TypingContext.FieldDefinition("LATITUDE", Type.TypeCode.STRING, null, false),
                new TypingContext.FieldDefinition("LONGITUDE", Type.TypeCode.STRING, null, false)
        );
        TypingContext.TypeDefinition location = new TypingContext.TypeDefinition("LOCATION", locationColumns, false, List.of());

        List<TypingContext.FieldDefinition> restaurantReviewColumns = List.of(
                new TypingContext.FieldDefinition("REVIEWER", Type.TypeCode.LONG, null, false),
                new TypingContext.FieldDefinition("RATING", Type.TypeCode.LONG, null, false)
        );
        TypingContext.TypeDefinition restaurantReview = new TypingContext.TypeDefinition("RESTAURANT_REVIEW", restaurantReviewColumns, false, List.of());

        List<TypingContext.FieldDefinition> restaurantTagColumns = List.of(
                new TypingContext.FieldDefinition("TAG", Type.TypeCode.STRING, null, false),
                new TypingContext.FieldDefinition("WEIGHT", Type.TypeCode.LONG, null, false)
        );
        TypingContext.TypeDefinition restaurantTag = new TypingContext.TypeDefinition("RESTAURANT_TAG", restaurantTagColumns, false, List.of());

        List<TypingContext.FieldDefinition> restaurantColumns = List.of(
                new TypingContext.FieldDefinition("REST_NO", Type.TypeCode.LONG, null, false),
                new TypingContext.FieldDefinition("NAME", Type.TypeCode.STRING, null, false),
                new TypingContext.FieldDefinition("LOCATION", Type.TypeCode.RECORD, "LOCATION", false),
                new TypingContext.FieldDefinition("REVIEWS", Type.TypeCode.RECORD, "RESTAURANT_REVIEW", true, nullableArray),
                new TypingContext.FieldDefinition("TAGS", Type.TypeCode.RECORD, "RESTAURANT_TAG", true, nullableArray),
                new TypingContext.FieldDefinition("CUSTOMER", Type.TypeCode.STRING, null, true),
                new TypingContext.FieldDefinition("ENCODED_BYTES", Type.TypeCode.BYTES, null, false)
        );
        TypingContext.TypeDefinition restaurant = new TypingContext.TypeDefinition("RESTAURANT", restaurantColumns, true, List.of(List.of("REST_NO")));

        TypingContext typingContext = TypingContext.create();
        typingContext.addType(location);
        typingContext.addType(restaurantReview);
        typingContext.addType(restaurantTag);
        typingContext.addType(restaurant);
        typingContext.addAllToTypeRepository();

        SchemaTemplate template = typingContext.generateSchemaTemplate("testTemplate", 1L);

        Descriptors.Descriptor descriptor = Descriptors.FileDescriptor.buildFrom(template.toProtobufDescriptor(), new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()})
                .getMessageTypes()
                .stream()
                .filter(m -> "RESTAURANT".equals(m.getName()))
                .findFirst()
                .get();

        return DynamicMessage.newBuilder(descriptor);
    }
}
