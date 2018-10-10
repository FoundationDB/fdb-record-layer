/*
 * RecordMetaDataBuilderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned1Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned2Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned3Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned4Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned5Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link RecordMetaDataBuilder}.
 */
public class RecordMetaDataBuilderTest {

    @Test
    public void caching() throws Exception {
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData1 = builder.getRecordMetaData();
        assertTrue(metaData1 == builder.getRecordMetaData());
        builder.addIndex(builder.getRecordType("MySimpleRecord"),
                new Index("MySimpleRecord$PRIMARY", "rec_no"));
        RecordMetaData metaData2 = builder.getRecordMetaData();
        assertFalse(metaData1 == metaData2);

    }

    @Test
    public void normalIndexDoesNotOverlapPrimaryKey() throws Exception {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        Index index = metaData.getIndex("MySimpleRecord$str_value_indexed");
        assertNotNull(index);
        assertNull(index.getPrimaryKeyComponentPositions());
    }

    @Test
    public void primaryIndexDoesOverlapPrimaryKey() throws Exception {
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        builder.addIndex(builder.getRecordType("MySimpleRecord"),
                         new Index("MySimpleRecord$PRIMARY", "rec_no"));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MySimpleRecord$PRIMARY");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] { 0 }, index.getPrimaryKeyComponentPositions());
    }

    @Test
    public void indexOnNestedPrimaryKey() throws Exception {
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord")
            .setPrimaryKey(field("header").nest("rec_no"));
        builder.addIndex(builder.getRecordType("MyRecord"),
                         new Index("MyRecord$PRIMARY",
                                   field("header").nest("rec_no"),
                                   IndexTypes.VALUE));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MyRecord$PRIMARY");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] { 0 }, index.getPrimaryKeyComponentPositions());
    }

    @Test
    public void indexOnPartialNestedPrimaryKey() throws Exception {
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord")
            .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
        builder.addIndex(builder.getRecordType("MyRecord"),
                         new Index("MyRecord$path_str",
                                   concat(field("header").nest("path"),
                                          field("str_value")),
                                   IndexTypes.VALUE));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MyRecord$path_str");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] { 0, -1 }, index.getPrimaryKeyComponentPositions());
    }

    @Test
    public void unsignedFields() {
        try {
            new RecordMetaDataBuilder(TestRecordsUnsigned1Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned1Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
        }
        try {
            new RecordMetaDataBuilder(TestRecordsUnsigned2Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned2Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.NestedWithUnsigned has illegal unsigned type UINT32", e.getMessage());
        }
        try {
            new RecordMetaDataBuilder(TestRecordsUnsigned3Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned3Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed32UnsignedRecord has illegal unsigned type FIXED32", e.getMessage());
        }
        try {
            new RecordMetaDataBuilder(TestRecordsUnsigned4Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned4Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed64UnsignedRecord has illegal unsigned type FIXED64", e.getMessage());
        }
        try {
            new RecordMetaDataBuilder(TestRecordsUnsigned5Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned5Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
        }
    }

    @Test
    public void versionInPrimaryKey() {
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        RecordTypeBuilder simpleRecordTypeBuilder = builder.getRecordType("MySimpleRecord");
        assertThrows(MetaDataException.class, () ->
                simpleRecordTypeBuilder.setPrimaryKey(Key.Expressions.concat(Key.Expressions.field("rec_no"), VersionKeyExpression.VERSION)));
    }
}
