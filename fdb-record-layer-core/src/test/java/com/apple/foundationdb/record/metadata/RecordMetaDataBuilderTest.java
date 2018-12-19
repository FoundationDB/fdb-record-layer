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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsChained1Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned1Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned2Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned3Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned4Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned5Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link RecordMetaDataBuilder}.
 */
public class RecordMetaDataBuilderTest {
    @SuppressWarnings("deprecation")
    private RecordMetaDataBuilder createBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                boolean deprecatedWay) {
        RecordMetaDataBuilder builder;
        if (deprecatedWay) {
            builder = new RecordMetaDataBuilder(fileDescriptor);
        } else {
            builder = RecordMetaData.newBuilder().setRecords(fileDescriptor);
        }
        return builder;
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "caching [deprecatedWay = {0}]")
    public void caching(final TestHelpers.BooleanEnum deprecatedWay) throws Exception {
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), deprecatedWay.toBoolean());
        RecordMetaData metaData1 = builder.getRecordMetaData();
        assertTrue(metaData1 == builder.getRecordMetaData());
        builder.addIndex("MySimpleRecord", "MySimpleRecord$PRIMARY", "rec_no");
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

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "caching [deprecatedWay = {0}]")
    public void primaryIndexDoesOverlapPrimaryKey(final TestHelpers.BooleanEnum deprecatedWay) throws Exception {
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), deprecatedWay.toBoolean());
        builder.addIndex("MySimpleRecord", "MySimpleRecord$PRIMARY", "rec_no");
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MySimpleRecord$PRIMARY");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] {0}, index.getPrimaryKeyComponentPositions());
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "caching [deprecatedWay = {0}]")
    public void indexOnNestedPrimaryKey(final TestHelpers.BooleanEnum deprecatedWay) throws Exception {
        RecordMetaDataBuilder builder = createBuilder(TestRecordsWithHeaderProto.getDescriptor(), deprecatedWay.toBoolean());
        builder.getRecordType("MyRecord")
                .setPrimaryKey(field("header").nest("rec_no"));
        builder.addIndex("MyRecord", new Index("MyRecord$PRIMARY",
                field("header").nest("rec_no"),
                IndexTypes.VALUE));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MyRecord$PRIMARY");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] {0}, index.getPrimaryKeyComponentPositions());
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "caching [deprecatedWay = {0}]")
    public void indexOnPartialNestedPrimaryKey(final TestHelpers.BooleanEnum deprecatedWay) throws Exception {
        RecordMetaDataBuilder builder = createBuilder(TestRecordsWithHeaderProto.getDescriptor(), deprecatedWay.toBoolean());
        builder.getRecordType("MyRecord")
                .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
        builder.addIndex("MyRecord", new Index("MyRecord$path_str",
                concat(field("header").nest("path"),
                        field("str_value")),
                IndexTypes.VALUE));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MyRecord$path_str");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] {0, -1}, index.getPrimaryKeyComponentPositions());
    }

    @Test
    public void unsignedFields() {
        try {
            RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned1Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned1Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
        }
        try {
            RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned2Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned2Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.NestedWithUnsigned has illegal unsigned type UINT32", e.getMessage());
        }
        try {
            RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned3Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned3Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed32UnsignedRecord has illegal unsigned type FIXED32", e.getMessage());
        }
        try {
            RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned4Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned4Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed64UnsignedRecord has illegal unsigned type FIXED64", e.getMessage());
        }
        try {
            RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned5Proto.getDescriptor());
            fail("Did not throw exception for TestRecordsUnsigned5Proto");
        } catch (MetaDataException e) {
            assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
        }
    }

    @Test
    public void versionInPrimaryKey() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        RecordTypeBuilder simpleRecordTypeBuilder = builder.getRecordType("MySimpleRecord");
        assertThrows(MetaDataException.class, () ->
                simpleRecordTypeBuilder.setPrimaryKey(Key.Expressions.concat(Key.Expressions.field("rec_no"), VersionKeyExpression.VERSION)));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void selfContainedMetaData() {
        // Using the deprecated constructor to ensure that the output of the new builder equals the old one.
        RecordMetaDataBuilder builder = new RecordMetaDataBuilder(TestRecordsChained1Proto.getDescriptor());
        RecordMetaDataProto.MetaData metaData = builder.getRecordMetaData().toProto();

        // Rebuild from proto and file descriptor
        RecordMetaData recordMetaData = RecordMetaData.build(metaData);
        MetaDataProtoTest.verifyEquals(builder.getRecordMetaData(), recordMetaData);
        MetaDataProtoTest.verifyEquals(recordMetaData, RecordMetaData.build(TestRecordsChained1Proto.getDescriptor()));

        // Basic setRecords
        RecordMetaDataBuilder builder2 = RecordMetaData.newBuilder().setRecords(metaData);
        MetaDataProtoTest.verifyEquals(recordMetaData, builder2.getRecordMetaData());

        // Override a dependency
        RecordMetaDataBuilder builder3 = RecordMetaData.newBuilder()
                .addDependency(TestRecords1Proto.getDescriptor())
                .setRecords(builder2.getRecordMetaData().toProto());
        MetaDataProtoTest.verifyEquals(recordMetaData, builder3.getRecordMetaData());

        // Exclude dependencies
        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[] {
                TestRecords1Proto.getDescriptor()
        };

        Throwable t = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(recordMetaData.toProto(dependencies)));
        assertEquals("Dependency test_records_1.proto not found", t.getMessage());
        RecordMetaDataBuilder builder4 = RecordMetaData.newBuilder()
                .addDependencies(dependencies)
                .setRecords(recordMetaData.toProto(dependencies));
        MetaDataProtoTest.verifyEquals(recordMetaData, builder4.getRecordMetaData());
        Descriptors.FileDescriptor dep4 = builder4.getRecordMetaData().getRecordsDescriptor().getDependencies().get(1);
        assertEquals(dep4.getName(), TestRecords1Proto.getDescriptor().getName());
        assertSame(dep4, dependencies[0]);
        Descriptors.FileDescriptor dep2 = builder2.getRecordMetaData().getRecordsDescriptor().getDependencies().get(1);
        assertEquals(dep2.getName(), dep4.getName());
        assertNotSame(dep2, dep4);

        // Add and remove index
        int version = builder.getVersion();
        builder.addIndex("MySimpleRecord", "MySimpleRecord$testIndex", "rec_no");
        assertEquals(builder.getVersion(), version + 1);
        assertNotNull(builder.getRecordMetaData().getIndex("MySimpleRecord$testIndex"));
        builder.removeIndex("MySimpleRecord$testIndex");
        assertEquals(builder.getVersion(), version + 2);
        t = assertThrows(MetaDataException.class, () ->
                builder.getRecordMetaData().getIndex("MySimpleRecord$testIndex"));
        assertEquals("Index MySimpleRecord$testIndex not defined", t.getMessage());
    }
}
