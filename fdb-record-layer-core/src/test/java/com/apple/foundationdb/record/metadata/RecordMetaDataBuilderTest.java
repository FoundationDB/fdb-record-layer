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
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecordsBadUnion1Proto;
import com.apple.foundationdb.record.TestRecordsBadUnion2Proto;
import com.apple.foundationdb.record.TestRecordsChained1Proto;
import com.apple.foundationdb.record.TestRecordsDuplicateUnionFields;
import com.apple.foundationdb.record.TestRecordsDuplicateUnionFieldsReordered;
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link RecordMetaDataBuilder}.
 */
public class RecordMetaDataBuilderTest {
    private RecordMetaDataBuilder createBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                boolean useCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
        if (useCounterBasedSubspaceKey) {
            builder.enableCounterBasedSubspaceKeys();
        }
        builder.setRecords(fileDescriptor);
        return builder;
    }

    @Test
    public void caching() {
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), true);
        RecordMetaData metaData1 = builder.getRecordMetaData();
        assertSame(metaData1, builder.getRecordMetaData());
        builder.addIndex("MySimpleRecord", "MySimpleRecord$PRIMARY", "rec_no");
        RecordMetaData metaData2 = builder.getRecordMetaData();
        assertNotSame(metaData1, metaData2);
    }

    @Test
    public void testGetRecordTypeKeyTuple() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData = metaDataBuilder.build(false);
        Tuple t = metaData.getRecordType("MySimpleRecord").getRecordTypeKeyTuple();
        assertEquals(1, t.size());
        assertEquals(1, t.getLong(0));
    }

    @ParameterizedTest(name = "normalIndexDoesNotOverlapPrimaryKey [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void normalIndexDoesNotOverlapPrimaryKey(final boolean indexCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
        if (indexCounterBasedSubspaceKey) {
            builder.enableCounterBasedSubspaceKeys();
        }
        RecordMetaData metaData = builder.setRecords(TestRecords1Proto.getDescriptor()).getRecordMetaData();
        Index index = metaData.getIndex("MySimpleRecord$str_value_indexed");
        assertNotNull(index);
        assertNull(index.getPrimaryKeyComponentPositions());
        if (!indexCounterBasedSubspaceKey) {
            assertEquals(index.getName(), index.getSubspaceKey());
        } else {
            assertEquals(1L, index.getSubspaceKey());
        }
    }

    @ParameterizedTest(name = "primaryIndexDoesOverlapPrimaryKey [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void primaryIndexDoesOverlapPrimaryKey(final boolean indexCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), indexCounterBasedSubspaceKey);
        builder.addIndex("MySimpleRecord", "MySimpleRecord$PRIMARY", "rec_no");
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index = metaData.getIndex("MySimpleRecord$PRIMARY");
        assertNotNull(index);
        assertNotNull(index.getPrimaryKeyComponentPositions());
        assertArrayEquals(new int[] {0}, index.getPrimaryKeyComponentPositions());
        if (!indexCounterBasedSubspaceKey) {
            assertEquals(index.getName(), index.getSubspaceKey());
        } else {
            assertEquals(4L, index.getSubspaceKey());
        }
    }

    @ParameterizedTest(name = "indexOnNestedPrimaryKey [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void indexOnNestedPrimaryKey(final boolean indexCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = createBuilder(TestRecordsWithHeaderProto.getDescriptor(), indexCounterBasedSubspaceKey);
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
        if (!indexCounterBasedSubspaceKey) {
            assertEquals(index.getName(), index.getSubspaceKey());
        } else {
            assertEquals(1L, index.getSubspaceKey());
        }
    }

    @ParameterizedTest(name = "uniqueIndexOverlappingWithNestedPrimaryKey [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void uniqueIndexOverlappingWithNestedPrimaryKey(final boolean indexCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = createBuilder(TestRecordsWithHeaderProto.getDescriptor(), indexCounterBasedSubspaceKey);
        builder.getRecordType("MyRecord")
                .setPrimaryKey(field("header").nest(concat(field("num"), field("rec_no"))));
        builder.addIndex("MyRecord", new Index("MyRecord$num-str-unique-1",
                concat(field("header").nest(field("num")), field("str_value")),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        builder.addIndex("MyRecord", new Index("MyRecord$num-str-unique-2",
                concat(field("header").nest(field("num", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NULL_UNIQUE)), field("str_value")),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        RecordMetaData metaData = builder.getRecordMetaData();
        Index index1 = metaData.getIndex("MyRecord$num-str-unique-1");
        assertArrayEquals(new int[] {0, -1}, index1.getPrimaryKeyComponentPositions());
        Index index2 = metaData.getIndex("MyRecord$num-str-unique-2");
        assertArrayEquals(new int[] {0, -1}, index2.getPrimaryKeyComponentPositions());
        if (!indexCounterBasedSubspaceKey) {
            assertEquals(index1.getName(), index1.getSubspaceKey());
            assertEquals(index2.getName(), index2.getSubspaceKey());
        } else {
            assertEquals(1L, index1.getSubspaceKey());
            assertEquals(2L, index2.getSubspaceKey());
        }
    }

    @ParameterizedTest(name = "indexOnPartialNestedPrimaryKey [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void indexOnPartialNestedPrimaryKey(final boolean indexCounterBasedSubspaceKey) {
        RecordMetaDataBuilder builder = createBuilder(TestRecordsWithHeaderProto.getDescriptor(), indexCounterBasedSubspaceKey);
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
        if (!indexCounterBasedSubspaceKey) {
            assertEquals(index.getName(), index.getSubspaceKey());
        } else {
            assertEquals(1L, index.getSubspaceKey());
        }
    }

    @Test
    public void setEvolutionValidatorAfterRecords() {
        final MetaDataEvolutionValidator defaultValidator = MetaDataEvolutionValidator.getDefaultInstance();
        RecordMetaDataBuilder builder = createBuilder(TestRecords1Proto.getDescriptor(), true);
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
    public void selfContainedMetaData() {
        RecordMetaData recordMetaData = RecordMetaData.build(TestRecordsChained1Proto.getDescriptor());
        RecordMetaDataProto.MetaData metaData = recordMetaData.toProto();

        // Rebuild from proto
        RecordMetaData fromProto = RecordMetaData.build(metaData);
        MetaDataProtoTest.verifyEquals(recordMetaData, fromProto);

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
        assertEquals("Dependency not found", e.getMessage());
        assertThat(e.getLogInfo(), hasEntry(LogMessageKeys.VALUE.toString(), "test_records_1.proto"));
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
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsChained1Proto.getDescriptor());
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
    public void localMetaData() throws Descriptors.DescriptorValidationException {
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

        // Add an additional field to the record type union for _MySimpleRecord, and validate that field is *not* used when this is
        // used with a local file descriptor, but that it is used when the new file is set with updateRecords
        DescriptorProtos.FileDescriptorProto.Builder evolvedFileBuilder = TestRecords1EvolvedProto.getDescriptor().toProto().toBuilder();
        evolvedFileBuilder.getMessageTypeBuilderList().forEach(message -> {
            if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                message.getFieldBuilderList().forEach(field -> {
                    if (field.getName().equals("_MySimpleRecord")) {
                        field.setName("_MySimpleRecord_Old");
                    }
                });
                message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + TestRecords1EvolvedProto.getDescriptor().getPackage() + ".MySimpleRecord")
                        .setName("_MySimpleRecord_New")
                        .setNumber(message.getFieldBuilderList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto.Builder::getNumber).max().orElse(0) + 1)
                );
            }
        });
        Descriptors.FileDescriptor evolvedFile2 = Descriptors.FileDescriptor.buildFrom(evolvedFileBuilder.build(), TestRecords1EvolvedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        RecordMetaData metaData2 = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(evolvedFile2)
                .setRecords(originalMetaData)
                .build();
        assertSame(evolvedFile2.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord_Old"),
                metaData2.getUnionFieldForRecordType(metaData2.getRecordType("MySimpleRecord")));

        RecordMetaDataBuilder metaDataBuilder3 = RecordMetaData.newBuilder()
                .setRecords(originalMetaData);
        metaDataBuilder3.updateRecords(evolvedFile2);
        RecordMetaData metaData3 = metaDataBuilder3.build();
        assertSame(evolvedFile2.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord_New"),
                metaData3.getUnionFieldForRecordType(metaData3.getRecordType("MySimpleRecord")));
    }

    @Test
    public void localMetaDataWithRenamed() throws Descriptors.DescriptorValidationException {
        // Rename a record type
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.renameRecordType(protoBuilder, "MySimpleRecord", "MyNewSimpleRecord");
        Descriptors.FileDescriptor updatedFile = Descriptors.FileDescriptor.buildFrom(protoBuilder.getRecords(), TestRecords1Proto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));

        // Validate that the type name change happened
        assertNull(updatedFile.findMessageTypeByName("MySimpleRecord"));
        Descriptors.Descriptor newSimpleRecordDescriptor = updatedFile.findMessageTypeByName("MyNewSimpleRecord");
        assertNotNull(newSimpleRecordDescriptor);
        assertSame(newSimpleRecordDescriptor, updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MyNewSimpleRecord").getMessageType());

        RecordMetaData metaData2 = RecordMetaData.newBuilder()
                .setLocalFileDescriptor(updatedFile)
                .setRecords(metaData.toProto())
                .build();
        assertThrows(MetaDataException.class, () -> metaData2.getRecordType("MySimpleRecord"));
        assertNotNull(metaData2.getRecordType("MyNewSimpleRecord"));
        assertSame(newSimpleRecordDescriptor, metaData2.getRecordType("MyNewSimpleRecord").getDescriptor());
        assertEquals(Collections.singletonList(metaData2.getRecordType("MyNewSimpleRecord")),
                metaData2.recordTypesForIndex(metaData2.getIndex("MySimpleRecord$str_value_indexed")));
        assertSame(updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME), metaData2.getUnionDescriptor());

        MetaDataException e = assertThrows(MetaDataException.class, metaData2::toProto);
        assertEquals("cannot serialize meta-data with a local records descriptor to proto", e.getMessage());
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
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MyOtherRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        builder.updateRecords(TestRecords1EvolvedProto.getDescriptor(), true);
        RecordMetaData recordMetaData = builder.build(true);
        assertSame(TestRecords1EvolvedProto.getDescriptor(), recordMetaData.getRecordType("MySimpleRecord").getDescriptor().getFile());
        assertSame(TestRecords1EvolvedProto.getDescriptor(), recordMetaData.getRecordType("MyOtherRecord").getDescriptor().getFile());
        assertSame(TestRecords1EvolvedProto.getDescriptor(), recordMetaData.getRecordType("AnotherRecord").getDescriptor().getFile());
        assertEquals(recordMetaData.getVersion(), recordMetaData.getRecordType("AnotherRecord").getSinceVersion().intValue());
        assertThat(recordMetaData.getRecordType("AnotherRecord").getSinceVersion(), greaterThan(prevVersion));

        MetaDataException e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder()
                .setLocalFileDescriptor(TestRecords1EvolvedProto.getDescriptor())
                .setRecords(recordMetaData.toProto())
                .updateRecords(TestRecords1EvolvedProto.getDescriptor()));
        assertEquals("Updating the records descriptor is not allowed when the local file descriptor is set", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().updateRecords(TestRecords1EvolvedProto.getDescriptor()));
        assertEquals("Records descriptor is not set yet", e.getMessage());

    }

    @Test
    public void updateRecordsWithRenamed() throws Descriptors.DescriptorValidationException {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MyOtherRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        builder.addIndex("MyOtherRecord", "num_value_3_indexed");
        RecordMetaData metaData = builder.getRecordMetaData();
        assertEquals(Collections.singletonList(metaData.getRecordType("MyOtherRecord")),
                metaData.recordTypesForIndex(metaData.getIndex("MyOtherRecord$num_value_3_indexed")));

        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecords1Proto.getDescriptor().toProto().toBuilder();
        fileBuilder.getMessageTypeBuilderList().forEach(message -> {
            if (message.getName().equals("MyOtherRecord")) {
                message.setName("MyOtherOtherRecord");
            } else if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                message.getFieldBuilderList().forEach(field -> {
                    if (field.getTypeName().endsWith("MyOtherRecord")) {
                        field.setTypeName("MyOtherOtherRecord");
                    }
                });
            }
        });
        Descriptors.FileDescriptor updatedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), new Descriptors.FileDescriptor[]{TestRecords1Proto.getDescriptor()});
        builder.updateRecords(updatedFileDescriptor);
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), updatedFileDescriptor);
        assertThrows(MetaDataException.class, () -> builder.getRecordType("MyOtherRecord"));
        assertSame(builder.getRecordType("MyOtherOtherRecord").getDescriptor().getFile(), updatedFileDescriptor);
        metaData = builder.getRecordMetaData();
        assertEquals(Collections.singletonList(metaData.getRecordType("MyOtherOtherRecord")),
                metaData.recordTypesForIndex(metaData.getIndex("MyOtherRecord$num_value_3_indexed")));
        RecordMetaDataProto.MetaData metaDataProto = metaData.toProto();
        assertThat(metaDataProto.getRecordTypesList().stream().map(RecordMetaDataProto.RecordType::getName).collect(Collectors.toList()),
                containsInAnyOrder("MySimpleRecord", "MyOtherOtherRecord"));
        RecordMetaDataProto.Index indexProto = metaDataProto.getIndexesList().stream()
                .filter(index -> index.getName().equals("MyOtherRecord$num_value_3_indexed"))
                .findFirst()
                .get();
        assertEquals(Collections.singletonList("MyOtherOtherRecord"), indexProto.getRecordTypeList());
    }

    @Test
    public void updateRecordsWithRenamedAndNamedToOld() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MyOtherRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        builder.addIndex("MyOtherRecord", "num_value_3_indexed");
        RecordMetaData metaData = builder.getRecordMetaData();
        assertEquals(Collections.singletonList(metaData.getRecordType("MyOtherRecord")),
                metaData.recordTypesForIndex(metaData.getIndex("MyOtherRecord$num_value_3_indexed")));
    }

    @ParameterizedTest(name = "updateRecordsWithNewUnionField [reorderFields = {0}]")
    @BooleanSource
    public void updateRecordsWithNewUnionField(boolean reorderFields) {

        final RecordMetaData oldMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        final RecordType oldSimpleRecord = oldMetaData.getRecordType("MySimpleRecord");

        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MySimpleRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());
        assertSame(builder.getRecordType("MyOtherRecord").getDescriptor().getFile(), TestRecords1Proto.getDescriptor());

        Descriptors.FileDescriptor updatedFileDescriptor;
        if (reorderFields) {
            updatedFileDescriptor = TestRecordsDuplicateUnionFieldsReordered.getDescriptor();
        } else {
            updatedFileDescriptor = TestRecordsDuplicateUnionFields.getDescriptor();
        }
        builder.updateRecords(updatedFileDescriptor);

        RecordMetaData newMetaData = builder.getRecordMetaData();
        assertEquals(TestRecords1EvolvedProto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER,
                oldMetaData.getUnionFieldForRecordType(oldSimpleRecord).getNumber());
        RecordType newSimpleRecord = newMetaData.getRecordType("MySimpleRecord");
        assertSame(newMetaData.getUnionDescriptor().findFieldByName("_MySimpleRecord_new"),
                newMetaData.getUnionFieldForRecordType(newSimpleRecord));
        assertThat(oldMetaData.getUnionFieldForRecordType(oldSimpleRecord).getNumber(),
                lessThan(newMetaData.getUnionFieldForRecordType(newSimpleRecord).getNumber()));
        assertEquals(oldSimpleRecord.getSinceVersion(), newSimpleRecord.getSinceVersion());
        MetaDataEvolutionValidator.getDefaultInstance().validate(oldMetaData, newMetaData);
    }

    @Test
    public void testSetSubspaceKeyCounter() {
        // Test setting the counter without enabling it
        MetaDataException e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor())
                .setSubspaceKeyCounter(4L));
        assertEquals("Counter-based subspace keys not enabled", e.getMessage());

        // Test setting the counter to a value not greater than the current value
        e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder()
                .enableCounterBasedSubspaceKeys()
                .setRecords(TestRecords1Proto.getDescriptor())
                .setSubspaceKeyCounter(3L));
        assertEquals("Subspace key counter must be set to a value greater than its current value", e.getMessage());
        assertThat(e.getLogInfo(), both(hasEntry(LogMessageKeys.EXPECTED.toString(), (Object) "greater than 3")).and(hasEntry(LogMessageKeys.ACTUAL.toString(), 3L)));

        // Set to a random number
        long randomCounter = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE - 10);
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setSubspaceKeyCounter(randomCounter).setRecords(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData = builder.build(true);
        assertNotNull(metaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertEquals(randomCounter + 1, metaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(randomCounter + 2, metaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(randomCounter + 3, metaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());

        // enable and set counter in the proto.
        RecordMetaDataProto.MetaData.Builder protoBuilder = RecordMetaDataProto.MetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor().toProto());
        protoBuilder.setUsesSubspaceKeyCounter(true).setSubspaceKeyCounter(randomCounter);
        builder = RecordMetaData.newBuilder().setRecords(protoBuilder.build(), true);
        RecordMetaData metaDataFromProto = builder.build(true);
        assertNotNull(metaDataFromProto.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaDataFromProto.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaDataFromProto.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertEquals(randomCounter + 1, metaDataFromProto.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(randomCounter + 2, metaDataFromProto.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(randomCounter + 3, metaDataFromProto.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
    }

    @Test
    public void counterBasedSubspaceKeys() {
        // Records descriptor already set.
        MetaDataException e = assertThrows(MetaDataException.class, () ->
                RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor()).enableCounterBasedSubspaceKeys());
        assertEquals("Records descriptor has already been set.", e.getMessage());

        // Valid use of counter-based subspace keys assignment.
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(TestRecords1Proto.getDescriptor());
        builder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", "num_value_2");
        RecordMetaData metaData = builder.build(true);
        assertNotNull(metaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_2"));
        assertEquals(1L, metaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(2L, metaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(3L, metaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals(4L, metaData.getIndex("MySimpleRecord$num_value_2").getSubspaceKey());

        // Valid use of counter-based subspace keys assignment with meta-data proto.
        builder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(metaData.toProto());
        metaData = builder.build(true);
        assertNotNull(metaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_2"));
        assertEquals(1L, metaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(2L, metaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(3L, metaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals(4L, metaData.getIndex("MySimpleRecord$num_value_2").getSubspaceKey());

        // FormerIndex
        RecordMetaDataBuilder formerIndexBuilder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(TestRecords1Proto.getDescriptor());
        Object formerSubspaceKey = formerIndexBuilder.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey();
        formerIndexBuilder.removeIndex("MySimpleRecord$num_value_3_indexed");
        final RecordMetaData metaDataWithFormerIndex = formerIndexBuilder.build(true);
        FormerIndex formerIndex = metaDataWithFormerIndex.getFormerIndexes().stream().filter(index -> index.getFormerName().equals("MySimpleRecord$num_value_3_indexed")).findFirst().get();
        assertEquals(formerSubspaceKey, formerIndex.getSubspaceKey());

        // A common case that user had some existing meta-data in which all indexes had implicit subspace keys, and the user now decides to enable this feature.
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.toProto().toBuilder();
        protoBuilder.getIndexesBuilderList().forEach(RecordMetaDataProto.Index.Builder::clearSubspaceKey);
        metaData = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(protoBuilder.build()).getRecordMetaData();
        assertNotNull(metaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertEquals(metaData.getIndex("MySimpleRecord$str_value_indexed").getName(),
                metaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(metaData.getIndex("MySimpleRecord$num_value_unique").getName(),
                metaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(metaData.getIndex("MySimpleRecord$num_value_3_indexed").getName(),
                metaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
    }

    @Test
    public void explicitSubspaceKeys() {
        // Add an explicit subspace key and make sure the explicit subspace key is not overridden.
        Index indexWithExplicitSubspaceKey = new Index("indexWithExplicitSubspaceKey", concatenateFields("str_value_indexed", "num_value_3_indexed"));
        indexWithExplicitSubspaceKey.setSubspaceKey("explicitSubspaceKey");
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(TestRecords1Proto.getDescriptor());
        builder.addIndex("MySimpleRecord", indexWithExplicitSubspaceKey);
        RecordMetaData metaData = builder.build(true);
        assertNotNull(metaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(metaData.getIndex("indexWithExplicitSubspaceKey"));
        assertEquals(1L, metaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(2L, metaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(3L, metaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals("explicitSubspaceKey", metaData.getIndex("indexWithExplicitSubspaceKey").getSubspaceKey());

        // Setting explicit subspace key to something that can clash.
        Index clashingIndex = new Index("clashingIndex", concatenateFields("str_value_indexed", "num_value_3_indexed"));
        clashingIndex.setSubspaceKey(1L);
        final RecordMetaDataBuilder clashingBuilder = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(TestRecords1Proto.getDescriptor());
        clashingBuilder.addIndex("MySimpleRecord", clashingIndex);
        MetaDataException e = assertThrows(MetaDataException.class, () -> clashingBuilder.build(true));
        assertEquals("Same subspace key 1 used by both MySimpleRecord$str_value_indexed and clashingIndex", e.getMessage());
    }

    @Test
    public void counterBasedSubspaceKeysBackwardCompatibility() {
        // User had some old meta-data in which some indexes had implicit subspace keys.
        RecordMetaDataProto.MetaData.Builder protoBuilder = RecordMetaDataProto.MetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor().toProto());
        protoBuilder.addIndexesBuilder()
                .setName("preUpgradeIndex")
                .setType(IndexTypes.VALUE)
                .addRecordType("MySimpleRecord")
                .setRootExpression(Key.Expressions.field("num_value_2").toKeyExpression())
                .clearSubspaceKey();

        RecordMetaDataBuilder preUpgradeBuilder = RecordMetaData.newBuilder().setRecords(protoBuilder.build(), true);
        RecordMetaData preUpgradeMetaData = preUpgradeBuilder.build(true);
        assertNotNull(preUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(preUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(preUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(preUpgradeMetaData.getIndex("preUpgradeIndex"));
        assertEquals(preUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed").getName(),
                preUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(preUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique").getName(),
                preUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(preUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed").getName(),
                preUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals(preUpgradeMetaData.getIndex("preUpgradeIndex").getName(),
                preUpgradeMetaData.getIndex("preUpgradeIndex").getSubspaceKey());

        // User has chosen not to enable counter-based subspace keys after upgrade.
        RecordMetaDataBuilder afterUpgradeBuilder = RecordMetaData.newBuilder().setRecords(preUpgradeMetaData.toProto());
        afterUpgradeBuilder.addIndex("MySimpleRecord", "postUpgradeIndex", "num_value_2");
        RecordMetaData afterUpgradeMetaData = afterUpgradeBuilder.build(true);
        assertNotNull(afterUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(afterUpgradeMetaData.getIndex("preUpgradeIndex"));
        assertNotNull(afterUpgradeMetaData.getIndex("postUpgradeIndex"));
        assertEquals(afterUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed").getName(),
                afterUpgradeMetaData.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique").getName(),
                afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed").getName(),
                afterUpgradeMetaData.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals(afterUpgradeMetaData.getIndex("preUpgradeIndex").getName(),
                afterUpgradeMetaData.getIndex("preUpgradeIndex").getSubspaceKey());
        assertEquals(afterUpgradeMetaData.getIndex("postUpgradeIndex").getName(),
                afterUpgradeMetaData.getIndex("postUpgradeIndex").getSubspaceKey());

        // But later user decides to enable counter-based subspace keys assignment. Previous indexes will use name, newer indexes will use the counter.
        RecordMetaDataBuilder builderWithCounter = RecordMetaData.newBuilder().enableCounterBasedSubspaceKeys().setRecords(afterUpgradeMetaData.toProto());
        builderWithCounter.addIndex("MySimpleRecord", "postUpgradeWithCounterIndex", "num_value_2");
        RecordMetaData metaDataWithCounter = builderWithCounter.build(true);
        assertNotNull(metaDataWithCounter.getIndex("MySimpleRecord$str_value_indexed"));
        assertNotNull(metaDataWithCounter.getIndex("MySimpleRecord$num_value_unique"));
        assertNotNull(metaDataWithCounter.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(metaDataWithCounter.getIndex("preUpgradeIndex"));
        assertNotNull(metaDataWithCounter.getIndex("postUpgradeIndex"));
        assertNotNull(metaDataWithCounter.getIndex("postUpgradeWithCounterIndex"));
        assertEquals(metaDataWithCounter.getIndex("MySimpleRecord$str_value_indexed").getName(),
                metaDataWithCounter.getIndex("MySimpleRecord$str_value_indexed").getSubspaceKey());
        assertEquals(metaDataWithCounter.getIndex("MySimpleRecord$num_value_unique").getName(),
                metaDataWithCounter.getIndex("MySimpleRecord$num_value_unique").getSubspaceKey());
        assertEquals(metaDataWithCounter.getIndex("MySimpleRecord$num_value_3_indexed").getName(),
                metaDataWithCounter.getIndex("MySimpleRecord$num_value_3_indexed").getSubspaceKey());
        assertEquals(metaDataWithCounter.getIndex("preUpgradeIndex").getName(),
                metaDataWithCounter.getIndex("preUpgradeIndex").getSubspaceKey());
        assertEquals(metaDataWithCounter.getIndex("postUpgradeIndex").getName(),
                metaDataWithCounter.getIndex("postUpgradeIndex").getSubspaceKey());
        assertEquals(1L, metaDataWithCounter.getIndex("postUpgradeWithCounterIndex").getSubspaceKey());
    }

    @Test
    public void counterBasedSubspaceKeysProtoSettings() {
        MetaDataException e = assertThrows(MetaDataException.class, () -> RecordMetaData.build(
                RecordMetaDataProto.MetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor().toProto())
                        .clearSubspaceKeyCounter()
                        .setUsesSubspaceKeyCounter(true)
                        .build()));
        assertEquals("Error converting from protobuf", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.build(
                RecordMetaDataProto.MetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor().toProto())
                        .setSubspaceKeyCounter(5)
                        .setUsesSubspaceKeyCounter(false)
                        .build()));
        assertEquals("Error converting from protobuf", e.getMessage());

        e = assertThrows(MetaDataException.class, () -> RecordMetaData.build(
                RecordMetaDataProto.MetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor().toProto())
                        .clearUsesSubspaceKeyCounter()
                        .setSubspaceKeyCounter(5)
                        .build()));
        assertEquals("Error converting from protobuf", e.getMessage());
    }

    @Test
    void canSerializeAndDeserializeSyntheticRecordTypes() {
        RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        rmd.setSplitLongRecords(true);
        final RecordTypeBuilder simpleBuilder = rmd.getRecordType("MySimpleRecord");
        final RecordTypeBuilder otherBuilder = rmd.getRecordType("MyOtherRecord");

        JoinedRecordTypeBuilder joined = rmd.addJoinedRecordType("SimpleOtherJoin");
        joined.addConstituent(simpleBuilder);
        joined.addConstituent(otherBuilder);
        joined.addJoin("MySimpleRecord", "num_value_2", "MyOtherRecord", "rec_no");

        Index idx = new Index("joinIndex",
                Key.Expressions.concat(
                        field("MySimpleRecord").nest(Key.Expressions.concatenateFields("num_value_2", "num_value_unique")),
                        field("MyOtherRecord").nest(Key.Expressions.concat(
                                field("rec_no"),
                                field("num_value_2")
                        ))
                ),
                IndexTypes.VALUE,
                Map.of());
        rmd.addIndex("SimpleOtherJoin", idx);
        final RecordMetaData recordMetaData = rmd.build();

        final RecordMetaDataProto.MetaData metaDataProto = recordMetaData.toProto();

        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[] {
                RecordMetaDataProto.getDescriptor(),
                RecordMetaDataOptionsProto.getDescriptor()
        };

        RecordMetaData deserializedMetaData = RecordMetaData.newBuilder()
                .addDependencies(dependencies)
                .setRecords(metaDataProto)
                .build();

        assertEquals(recordMetaData.getRecordTypes().size(), deserializedMetaData.getRecordTypes().size(), "Incorrect record type count");
        assertIterableEquals(recordMetaData.getRecordTypes().keySet(), deserializedMetaData.getRecordTypes().keySet(), "Incorrect record type names");
        assertEquals(recordMetaData.getSyntheticRecordTypes().size(), deserializedMetaData.getSyntheticRecordTypes().size(), "Incorrect synthetic record type count");
        assertIterableEquals(recordMetaData.getSyntheticRecordTypes().keySet(), deserializedMetaData.getSyntheticRecordTypes().keySet(), "Incorrect synthetic record type names");
        assertEquals(recordMetaData.getAllIndexes().size(), deserializedMetaData.getAllIndexes().size(), "Incorrect index count");
        for (Index expectedIndex : recordMetaData.getAllIndexes()) {
            final Index actualIndex = deserializedMetaData.getIndex(expectedIndex.getName());
            assertNotNull(actualIndex, "Missing index " + expectedIndex);
            assertEquals(expectedIndex.getRootExpression(), actualIndex.getRootExpression(), "Incorrect index root expression");
        }
    }
}
