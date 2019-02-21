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
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecordsBadUnion1Proto;
import com.apple.foundationdb.record.TestRecordsBadUnion2Proto;
import com.apple.foundationdb.record.TestRecordsChained1Proto;
import com.apple.foundationdb.record.TestRecordsImportFlatProto;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsMarkedUnmarkedProto;
import com.apple.foundationdb.record.TestRecordsNoPrimaryKeyProto;
import com.apple.foundationdb.record.TestRecordsUnionMissingRecordProto;
import com.apple.foundationdb.record.TestRecordsUnionWithImportedNestedProto;
import com.apple.foundationdb.record.TestRecordsUnionWithNestedProto;
import com.apple.foundationdb.record.TestRecordsUnsigned1Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned2Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned3Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned4Proto;
import com.apple.foundationdb.record.TestRecordsUnsigned5Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TestTwoUnionsProto;
import com.apple.foundationdb.record.TestUnionDefaultNameProto;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertSame(metaData1, builder.getRecordMetaData());
        builder.addIndex("MySimpleRecord", "MySimpleRecord$PRIMARY", "rec_no");
        RecordMetaData metaData2 = builder.getRecordMetaData();
        assertNotSame(metaData1, metaData2);
    }

    @Test
    public void normalIndexDoesNotOverlapPrimaryKey() throws Exception {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        Index index = metaData.getIndex("MySimpleRecord$str_value_indexed");
        assertNotNull(index);
        assertNull(index.getPrimaryKeyComponentPositions());
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "primaryIndexDoesNotOverlapPrimaryKey [deprecatedWay = {0}]")
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
    @ParameterizedTest(name = "indexOnNestedPrimaryKey [deprecatedWay = {0}]")
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
    @ParameterizedTest(name = "indexOnPartialNestedPrimaryKey [deprecatedWay = {0}]")
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

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "setEvolutionValidatorAfterRecords [deprecatedWay = {0}]")
    public void setEvolutionValidatorAfterRecords(final TestHelpers.BooleanEnum deprecatedWay) {
        final MetaDataEvolutionValidator defaultValidator = MetaDataEvolutionValidator.getDefaultInstance();
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), deprecatedWay.toBoolean());
        assertSame(defaultValidator, builder.getEvolutionValidator());
        MetaDataEvolutionValidator secondValidator = MetaDataEvolutionValidator.newBuilder().setAllowIndexRebuilds(true).build();
        MetaDataException e = assertThrows(MetaDataException.class, () -> builder.setEvolutionValidator(secondValidator));
        assertEquals("Records already set.", e.getMessage());
        assertSame(defaultValidator, builder.getEvolutionValidator());
    }

    @Test
    public void setEvolutionValidator() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
        assertSame(MetaDataEvolutionValidator.getDefaultInstance(), builder.getEvolutionValidator());
        MetaDataEvolutionValidator secondValidator = MetaDataEvolutionValidator.newBuilder().setAllowIndexRebuilds(true).build();
        builder.setEvolutionValidator(secondValidator);
        assertSame(secondValidator, builder.getEvolutionValidator());
    }

    @Test
    public void unsignedFields() {
        MetaDataException e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned1Proto.getDescriptor()));
        assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
        e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned2Proto.getDescriptor()));
        assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.NestedWithUnsigned has illegal unsigned type UINT32", e.getMessage());
        e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned3Proto.getDescriptor()));
        assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed32UnsignedRecord has illegal unsigned type FIXED32", e.getMessage());
        e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned4Proto.getDescriptor()));
        assertEquals("Field unsigned_field in message com.apple.foundationdb.record.unsigned.Fixed64UnsignedRecord has illegal unsigned type FIXED64", e.getMessage());
        e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsUnsigned5Proto.getDescriptor()));
        assertEquals("Field unsigned_rec_no in message com.apple.foundationdb.record.unsigned.SimpleUnsignedRecord has illegal unsigned type UINT64", e.getMessage());
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

        MetaDataException e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(recordMetaData.toProto(dependencies)));
        assertEquals("Dependency test_records_1.proto not found", e.getMessage());
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
        e = assertThrows(MetaDataException.class, () ->
                builder.getRecordMetaData().getIndex("MySimpleRecord$testIndex"));
        assertEquals("Index MySimpleRecord$testIndex not defined", e.getMessage());
    }

    @Test
    public void badUnionFields() {
        MetaDataException e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsBadUnion1Proto.getDescriptor()));
        assertEquals("Union field not_a_record is not a message", e.getMessage());
        e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsBadUnion2Proto.getDescriptor()));
        assertEquals("Union field _MySimpleRecord should not be repeated", e.getMessage());
    }

    @Test
    public void localMetaData() {
        // Record type moved from being imported to being present in the local descriptor
        RecordMetaDataProto.MetaData previouslyImportedMetaData = RecordMetaData.build(TestRecordsImportProto.getDescriptor()).toProto(null);
        RecordMetaDataBuilder nowFlatMetaData = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(TestRecordsImportFlatProto.getDescriptor())
                .setRecords(previouslyImportedMetaData);
        assertNotNull(nowFlatMetaData.getRecordType("MySimpleRecord"));
        assertSame(nowFlatMetaData.getRecordType("MySimpleRecord").getDescriptor(), TestRecordsImportFlatProto.MySimpleRecord.getDescriptor());
        assertNotNull(nowFlatMetaData.getRecordType("MyLongRecord"));
        assertSame(nowFlatMetaData.getRecordType("MyLongRecord").getDescriptor(), TestRecordsImportFlatProto.MyLongRecord.getDescriptor());
        nowFlatMetaData.build(true);

        // Record type moved from the descriptor to being in an imported file
        RecordMetaDataProto.MetaData previouslyFlatMetaData = RecordMetaData.build(TestRecordsImportFlatProto.getDescriptor()).toProto(null);
        RecordMetaDataBuilder nowImportedMetaData = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(TestRecordsImportProto.getDescriptor())
                .setRecords(previouslyFlatMetaData);
        assertNotNull(nowImportedMetaData.getRecordType("MySimpleRecord"));
        assertSame(nowImportedMetaData.getRecordType("MySimpleRecord").getDescriptor(), TestRecords1Proto.MySimpleRecord.getDescriptor());
        assertNotNull(nowImportedMetaData.getRecordType("MyLongRecord"));
        assertSame(nowImportedMetaData.getRecordType("MyLongRecord").getDescriptor(), TestRecords2Proto.MyLongRecord.getDescriptor());
        nowImportedMetaData.build(true);

        // The original meta-data
        RecordMetaDataProto.MetaData originalMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto(null);

        // Evolve the local file descriptor by adding a record type to the union
        RecordMetaDataBuilder evolvedMetaDataBuilder = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(TestRecords1EvolvedProto.getDescriptor())
                .setRecords(originalMetaData);
        MetaDataException e = assertThrows(MetaDataException.class, () -> evolvedMetaDataBuilder.getRecordType("AnotherRecord"));
        assertEquals("Unknown record type AnotherRecord", e.getMessage());
        assertSame(evolvedMetaDataBuilder.build(true).getUnionDescriptor(), TestRecords1EvolvedProto.RecordTypeUnion.getDescriptor());
    }

    @Test
    public void invalidLocalMetaData() {
        // Change the type of a field
        Descriptors.FileDescriptor updatedFile = MetaDataEvolutionValidatorTest.mutateField("MySimpleRecord", "str_value_indexed",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES));

        RecordMetaDataProto.MetaData originalMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto();
        RecordMetaDataBuilder evolvedMetaDataBuilder = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(updatedFile);
        MetaDataException e = assertThrows(MetaDataException.class, () -> evolvedMetaDataBuilder.setRecords(originalMetaData));
        assertEquals("field type changed", e.getMessage());
    }

    @Test
    public void validUnion() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsMarkedUnmarkedProto.getDescriptor());
        RecordMetaData recordMetaData = builder.build(true);
        assertNotNull(recordMetaData.getRecordType("MyMarkedRecord"));
        assertNotNull(recordMetaData.getRecordType("MyUnmarkedRecord1"));
        MetaDataException e = assertThrows(MetaDataException.class, () -> recordMetaData.getRecordType("MyUnmarkedRecord2"));
        assertEquals("Unknown record type MyUnmarkedRecord2", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestUnionDefaultNameProto.getDescriptor()));
        assertEquals("Union message type RecordTypeUnion cannot be a union field.", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestTwoUnionsProto.getDescriptor()));
        assertEquals("Only one union descriptor is allowed", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestRecordsUnionMissingRecordProto.getDescriptor()));
        assertEquals("Record message type MyMissingRecord must be a union field.", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestRecordsUnionMissingRecordProto.getDescriptor()));
        assertEquals("Record message type MyMissingRecord must be a union field.", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestRecordsUnionWithNestedProto.getDescriptor()));
        assertEquals("Union field _MyNestedRecord has type MyNestedRecord which is not a record", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestRecordsUnionWithImportedNestedProto.getDescriptor()));
        assertEquals("Union field _MyNestedRecord has type RestaurantReview which is not a record", e.getMessage());
    }

    @Test
    public void noPrimaryKey() {
        MetaDataException e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecordsNoPrimaryKeyProto.getDescriptor()).getRecordMetaData());
        assertEquals("Record type MyNoPrimaryKeyRecord must have a primary key", e.getMessage());
    }

    @Test
    public void updateRecords() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final int prevVersion = builder.getVersion();
        assertNotNull(builder.getRecordType("MySimpleRecord"));
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        assertNotNull(builder.getRecordType("MyOtherRecord"));
        builder.updateRecords(TestRecords1EvolvedProto.getDescriptor(), true);
        RecordMetaData recordMetaData = builder.build(true);
        assertNotNull(recordMetaData.getRecordType("MySimpleRecord"));
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1EvolvedProto.getDescriptor());
        assertNotNull(recordMetaData.getRecordType("MyOtherRecord"));
        assertSame(builder.getRecordType("MyOtherRecord").getDescriptor().getFile(), TestRecords1EvolvedProto.getDescriptor());
        assertNotNull(recordMetaData.getRecordType("AnotherRecord"));
        assertSame(builder.getRecordType("AnotherRecord").getDescriptor().getFile(), TestRecords1EvolvedProto.getDescriptor());
        assertEquals(builder.getRecordType("AnotherRecord").getSinceVersion().intValue(), builder.getVersion());
        assertThat(builder.getRecordType("AnotherRecord").getSinceVersion(), greaterThan(prevVersion));

        MetaDataException e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder()
                .setLocalFileDescriptor(TestRecords1EvolvedProto.getDescriptor())
                .setRecords(recordMetaData.toProto())
                .updateRecords(TestRecords1EvolvedProto.getDescriptor()));
        assertEquals("Updating the records descriptor is not allowed when the local file descriptor is set", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().updateRecords(TestRecords1EvolvedProto.getDescriptor()));
        assertEquals("Records descriptor is not set yet", e.getMessage());

    }
}
