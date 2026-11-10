/*
 * MetaDataProtoEditorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsImportedAndNewProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor.FieldTypeMatch;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the metadata proto editor. These tests focus on just the editor itself.
 *
 * <p>There are further tests for this class in {@link FDBMetaDataStoreTest}. Those tests focus on end-to-end scenarios
 * where metadata are read from the database, edited, and written back.
 */
public class MetaDataProtoEditorUnitTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataProtoEditorUnitTest.class);

    @Nonnull
    private FieldTypeMatch fieldIsType(@Nonnull DescriptorProtos.FileDescriptorProto.Builder file,
                                       @Nonnull String messageName, @Nonnull String fieldName,
                                       @Nonnull String typeName) throws Descriptors.DescriptorValidationException {
        return fieldIsType(file.build(), messageName, fieldName, typeName);
    }

    @Nonnull
    private FieldTypeMatch fieldIsType(@Nonnull DescriptorProtos.FileDescriptorProto file,
                                       @Nonnull String messageName, @Nonnull String fieldName,
                                       @Nonnull String typeName) throws Descriptors.DescriptorValidationException {

        final DescriptorProtos.DescriptorProto record = file.getMessageTypeList().stream()
                .filter(message -> message.getName().equals(messageName))
                .findAny()
                .orElseThrow();
        final DescriptorProtos.FieldDescriptorProto field = record.getFieldList().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .orElseThrow();
        final Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
        final Descriptors.Descriptor typeDescriptor = fileDescriptor.getMessageTypes().stream()
                .filter(type -> type.getName().equals(messageName))
                .findAny().orElseThrow();
        return MetaDataProtoEditor.fieldIsType(file, typeDescriptor, field, typeName);
    }

    @Test
    public void fieldIsType() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto file = TestRecords1Proto.getDescriptor().toProto();
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
    }

    /**
     * An enum-typed field references its enum type, so it matches as nested within the message declaring that enum.
     */
    @Test
    public void fieldIsTypeEnum() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto file = TestRecordsEnumProto.getDescriptor().toProto();
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "MyShapeRecord", "size", "MyShapeRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MyShapeRecord", "size", "MyShapeRecord.Size"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MyShapeRecord", "size", ".com.apple.foundationdb.record.testenum.MyShapeRecord.Size"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MyShapeRecord", "size", "MyShapeRecord.Color"));
        // A primitive field references no named type at all.
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MyShapeRecord", "rec_name", "MyShapeRecord"));
    }

    @Test
    public void fieldIsTypeUnqualified() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecords1Proto.getDescriptor().toProto().toBuilder();
        final DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                .filter(message -> message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME))
                .flatMap(message -> message.getFieldBuilderList().stream())
                .filter(field -> field.getName().equals("_MySimpleRecord"))
                .findAny()
                .get();

        // Unqualify the field in the union descriptor
        fieldBuilder.setTypeName("MySimpleRecord");

        // Ensure that the field still resolves to the same type
        Descriptors.FileDescriptor modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecords1Proto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor simpleRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("MySimpleRecord");
        assertNotNull(simpleRecordDescriptor);
        assertSame(simpleRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord").getMessageType());

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));

        fieldBuilder.setTypeName("test1.MySimpleRecord");
        modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecords1Proto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        simpleRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("MySimpleRecord");
        assertNotNull(simpleRecordDescriptor);
        assertSame(simpleRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord").getMessageType());

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.test1"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));
    }

    @Test
    public void nestedFieldIsType() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto file = TestRecordsDoubleNestedProto.getDescriptor().toProto();
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord"));

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MiddleRecord", "middle", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MiddleRecord", "middle", "OuterRecord.MiddleRecord"));

        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MiddleRecord", "other_middle", "MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MiddleRecord", "other_middle", "OuterRecord.MiddleRecord"));
    }

    @Test
    public void nestedFieldIsTypeUnqualified() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecordsDoubleNestedProto.getDescriptor().toProto().toBuilder();
        final DescriptorProtos.FieldDescriptorProto.Builder innerBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                .filter(message -> message.getName().equals("OuterRecord"))
                .flatMap(message -> message.getFieldBuilderList().stream())
                .filter(field -> field.getName().equals("inner"))
                .findAny()
                .get();

        // Unqualify the inner field
        innerBuilder.setTypeName("MiddleRecord.InnerRecord");

        // Ensure that the type actually resolves to the same type
        final Descriptors.FileDescriptor[] dependencies = TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]);
        Descriptors.FileDescriptor modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), dependencies);
        Descriptors.Descriptor innerRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findNestedTypeByName("MiddleRecord").findNestedTypeByName("InnerRecord");
        assertNotNull(innerRecordDescriptor);
        assertSame(innerRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findFieldByName("inner").getMessageType());

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord"));
        // Note: MiddleRecord.InnerRecord does not exist, because `MiddleRecord` here qualifies to the root of the
        // document, and thus, there is no InnerRecord inside it
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OtherRecord"));

        innerBuilder.setTypeName("OuterRecord.MiddleRecord.InnerRecord");
        modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), dependencies);
        innerRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findNestedTypeByName("MiddleRecord").findNestedTypeByName("InnerRecord");
        assertNotNull(innerRecordDescriptor);
        assertSame(innerRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findFieldByName("inner").getMessageType());

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OtherRecord"));

        int originalUnionFieldNumber = modifiedFileDescriptor.findMessageTypeByName("RecordTypeUnion").findFieldByName("_OuterRecord").getNumber();
        RecordMetaData metaData = RecordMetaData.build(modifiedFileDescriptor);
        final RecordMetaDataProto.MetaData renamedProto =
                singleRename(metaData.toProto(), "OuterRecord", "OtterRecord", getDependencies(metaData));
        Descriptors.FileDescriptor renamedDescriptor = Descriptors.FileDescriptor.buildFrom(renamedProto.getRecords(), dependencies);
        final Descriptors.Descriptor renamedUnionDescriptor = renamedDescriptor.findMessageTypeByName("RecordTypeUnion");
        final Descriptors.FieldDescriptor unionField = renamedUnionDescriptor.findFieldByNumber(originalUnionFieldNumber);
        assertEquals("_OtterRecord", unionField.getName());
        assertSame(renamedDescriptor.findMessageTypeByName("OtterRecord"), unionField.getMessageType());
        assertEquals(List.of(), renamedDescriptor.getMessageTypes().stream()
                .filter(type -> type.getName().equals("OuterRecord")).collect(Collectors.toList()));
        assertEquals(Set.of("_OtterRecord", "_MiddleRecord"), renamedUnionDescriptor.getFields().stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
        assertEquals(Set.of("MiddleRecord"), getNestedTypeNames(renamedDescriptor.findMessageTypeByName("OtterRecord")));
        assertEquals(Set.of("InnerRecord"), getNestedTypeNames(renamedDescriptor.findMessageTypeByName("OtterRecord")
                .findNestedTypeByName("MiddleRecord")));
    }

    @Nonnull
    private static Set<String> getNestedTypeNames(final Descriptors.Descriptor messageDescriptor) {
        return messageDescriptor
                .getNestedTypes().stream()
                .map(Descriptors.Descriptor::getName)
                .collect(Collectors.toSet());
    }

    @Nonnull
    private static Descriptors.FileDescriptor[] getDependencies(final RecordMetaData metaData) {
        return metaData.getRecordsDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]);
    }

    /**
     * A naive implementation of {@link MetaDataProtoEditor#renameRecordTypes} for testing purposes. Applies
     * {@code renamer} to every top-level record type in {@code originalProto}, one type at a time via
     * {@link MetaDataProtoEditor#renameRecordType}.
     */
    @Nonnull
    private static RecordMetaDataProto.MetaData renameRecordTypesOneByOne(
            @Nonnull RecordMetaDataProto.MetaData originalProto,
            @Nonnull UnaryOperator<String> renamer,
            @Nonnull Descriptors.FileDescriptor[] dependencies) {
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        for (final String recordType : MetaDataProtoEditor.getRecordTypes(builder)) {
            MetaDataProtoEditor.renameRecordType(builder, recordType, renamer.apply(recordType), dependencies);
        }
        return builder.build();
    }

    /**
     * Renames a single record type, and asserts that the batched {@link MetaDataProtoEditor#renameRecordTypes} and an
     * equivalent {@link MetaDataProtoEditor#renameRecordType} call produce byte-for-byte the same metadata. Returns
     * the batched result.
     */
    @Nonnull
    private static RecordMetaDataProto.MetaData singleRename(@Nonnull RecordMetaDataProto.MetaData originalProto,
                                                             @Nonnull String recordTypeName,
                                                             @Nonnull String newRecordTypeName,
                                                             @Nonnull Descriptors.FileDescriptor[] dependencies) {
        final UnaryOperator<String> renamer = name -> name.equals(recordTypeName) ? newRecordTypeName : name;
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder, renamer, dependencies);
        final RecordMetaDataProto.MetaData batched = builder.build();
        assertEquals(renameRecordTypesOneByOne(originalProto, renamer, dependencies), batched);
        return batched;
    }

    /**
     * Asserts that applying {@code renamer} via the batched {@link MetaDataProtoEditor#renameRecordTypes} method
     * produces byte-for-byte the same metadata as applying it one type at a time via {@link #renameRecordTypesOneByOne}.
     */
    private static void crossCheckRenamedMetaData(
            @Nonnull RecordMetaDataProto.MetaData originalProto,
            @Nonnull UnaryOperator<String> renamer,
            @Nonnull Descriptors.FileDescriptor[] dependencies,
            @Nonnull RecordMetaDataProto.MetaData batchedResult) {
        final RecordMetaDataProto.MetaData oneByOne = renameRecordTypesOneByOne(originalProto, renamer, dependencies);
        // Compare via `RecordMetaData.build().toProto()` rather than the raw protos directly, since building a
        // `RecordMetaData` normalizes some fields (e.g., filling in `nullInterpretation`) that may otherwise differ
        // in representation, though not in meaning, depending on which path produced the raw proto.
        final RecordMetaData batchedRecordMetaData = RecordMetaData.build(batchedResult);
        final RecordMetaData oneByOneRecordMetaData = RecordMetaData.build(oneByOne);
        assertEquals(batchedRecordMetaData.toProto(), oneByOneRecordMetaData.toProto());
    }

    /**
     * Asserts that {@code renamer} is rejected by both the batched {@link MetaDataProtoEditor#renameRecordTypes} and
     * an equivalent one-by-one sequence of {@link MetaDataProtoEditor#renameRecordType} calls.
     */
    private static void crossCheckRenameRecordTypesIsRejected(
            @Nonnull RecordMetaDataProto.MetaData originalProto,
            @Nonnull UnaryOperator<String> renamer,
            @Nonnull Descriptors.FileDescriptor[] dependencies) {
        assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(originalProto.toBuilder(), renamer, dependencies));
        assertThrows(MetaDataException.class,
                () -> renameRecordTypesOneByOne(originalProto, renamer, dependencies));
    }

    /**
     * Asserts that {@code renamer} is rejected with exactly {@code expectedMessage} by both the batched
     * {@link MetaDataProtoEditor#renameRecordTypes}, which must leave its builder untouched, and an equivalent
     * one-by-one sequence of {@link MetaDataProtoEditor#renameRecordType} calls.
     */
    private static void crossCheckRenameRecordTypesIsRejected(
            @Nonnull RecordMetaDataProto.MetaData originalProto,
            @Nonnull UnaryOperator<String> renamer,
            @Nonnull Descriptors.FileDescriptor[] dependencies,
            @Nonnull String expectedMessage) {
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        assertEquals(expectedMessage, assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(builder, renamer, dependencies)).getMessage());
        assertEquals(originalProto, builder.build());
        assertEquals(expectedMessage, assertThrows(MetaDataException.class,
                () -> renameRecordTypesOneByOne(originalProto, renamer, dependencies)).getMessage());
    }

    /**
     * Asserts that renaming {@code recordTypeName} to {@code newRecordTypeName} via
     * {@link MetaDataProtoEditor#renameRecordType} is rejected with exactly {@code expectedMessage}, and that the
     * builder is left untouched.
     */
    private static void assertRenameRecordTypeRejected(@Nonnull RecordMetaDataProto.MetaData originalProto,
                                                       @Nonnull String recordTypeName,
                                                       @Nonnull String newRecordTypeName,
                                                       @Nonnull String expectedMessage) {
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordType(builder, recordTypeName, newRecordTypeName,
                        RecordMetaDataBuilder.getDependencies(originalProto, Map.of())));
        assertEquals(expectedMessage, exception.getMessage());
        assertEquals(originalProto, builder.build());
    }

    private void renameFieldTypes(@Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder, @Nonnull String oldTypeName, @Nonnull String newTypeName) {
        messageTypeBuilder.getFieldBuilderList().forEach(field -> {
            if (field.getTypeName().equals(oldTypeName)) {
                field.setTypeName(newTypeName);
            } else if (field.getTypeName().startsWith(oldTypeName) && field.getTypeName().charAt(oldTypeName.length()) == '.') {
                field.setTypeName(newTypeName + field.getTypeName().substring(oldTypeName.length()));
            }
        });
        messageTypeBuilder.getNestedTypeBuilderList().forEach(nestedMessage -> renameFieldTypes(nestedMessage, oldTypeName, newTypeName));
    }

    @Test
    public void renameOuterTypeWithNestedTypeWithSameName() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecordsDoubleNestedProto.getDescriptor().toProto().toBuilder();
        fileBuilder.getMessageTypeBuilderList().forEach(message -> {
            if (message.getName().equals("OuterRecord")) {
                message.getNestedTypeBuilderList().forEach(nestedMessage -> {
                    if (nestedMessage.getName().equals("MiddleRecord")) {
                        nestedMessage.setName("OuterRecord");
                    }
                });
                renameFieldTypes(message, ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord", "OuterRecord");
            } else {
                renameFieldTypes(message, ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.OuterRecord");
            }
        });

        // Make sure the types were renamed in a way that preserves type, etc.
        Descriptors.FileDescriptor modifiedFile = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor outerOuterRecord = modifiedFile.findMessageTypeByName("OuterRecord");
        assertNotNull(outerOuterRecord);
        Descriptors.Descriptor nestedOuterRecord = outerOuterRecord.findNestedTypeByName("OuterRecord");
        assertNotNull(nestedOuterRecord);
        assertNotSame(outerOuterRecord, nestedOuterRecord);
        assertSame(outerOuterRecord, nestedOuterRecord.findNestedTypeByName("InnerRecord").findFieldByName("outer").getMessageType());
        assertSame(nestedOuterRecord, outerOuterRecord.findFieldByName("middle").getMessageType());
        assertSame(nestedOuterRecord, outerOuterRecord.findFieldByName("inner").getMessageType().getContainingType());
        assertSame(nestedOuterRecord, modifiedFile.findMessageTypeByName("MiddleRecord").findFieldByName("other_middle").getMessageType());

        RecordMetaData metaData = RecordMetaData.build(modifiedFile);
        final RecordMetaDataProto.MetaData renamedProto =
                singleRename(metaData.toProto(), "OuterRecord", "OtterRecord", getDependencies(metaData));
        Descriptors.FileDescriptor renamedDescriptor = Descriptors.FileDescriptor.buildFrom(renamedProto.getRecords(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor renamedOuter = renamedDescriptor.findMessageTypeByName("OtterRecord");
        Descriptors.Descriptor renamedOuterOuter = renamedOuter.findNestedTypeByName("OuterRecord");
        assertSame(renamedOuterOuter, renamedOuter.findFieldByName("middle").getMessageType());
        assertSame(renamedOuterOuter, renamedOuter.findFieldByName("many_middle").getMessageType());
        assertSame(renamedDescriptor.findMessageTypeByName("OtherRecord"), renamedOuter.findFieldByName("other").getMessageType());
        Descriptors.Descriptor renamedOuterOuterInner = renamedOuterOuter.findNestedTypeByName("InnerRecord");
        assertSame(renamedOuterOuterInner, renamedOuterOuter.findFieldByName("inner").getMessageType());
        assertSame(renamedOuter, renamedOuterOuterInner.findFieldByName("outer").getMessageType());
    }

    public static RecordMetaDataProto.MetaData.Builder loadMetaData(@Nonnull String name) throws IOException {
        try (@Nullable InputStream input = MetaDataProtoEditorUnitTest.class.getResourceAsStream("/" + name);
                InputStreamReader reader = new InputStreamReader(Objects.requireNonNull(input,
                        () -> "No resource: " + name))) {
            RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(reader, builder);
            return builder;
        }
    }

    public static Stream<Arguments> renamableFiles() {
        // Provides two arguments, the name of the metadata json file, and extra assertions for after the rename.
        // Note: Explicitly spelling out the .json extensions here so you can Cmd+Click in the IDE to open the files.
        return Stream.concat(
                Stream.of(
                        "OneBoringType.json",
                        "TwoBoringTypes.json",
                        "TwoBoringTypesInPackage.json",
                        "DuplicateUnionFields.json",
                        "DuplicateNonCanonicalUnionFields.json",
                        "NonCanonicalUnionFields.json",
                        "NestedMessageSameName.json",
                        "OneTypeWithIndexes.json",
                        "MultiTypeIndex.json",
                        "UniversalIndex.json"
                ).map(filename -> Arguments.of(filename, (Consumer<RecordMetaData>) renamed -> { })),
                Stream.of(
                        Arguments.of("UnnestedExternalType.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // "parent" (T1) is a top-level RECORD type and gets renamed; "child" names the
                                    // dependency-defined UUID type, which is not a top-level record type and so is
                                    // left untouched.
                                    assertEquals(Map.of("parent", simpleRename("T1"), "child", "UUID"),
                                            constituentTypeNames(renamed));
                                }),
                        Arguments.of("UnnestedInternal.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // "parent" (T2) is a top-level RECORD type and gets renamed; "child" names T1,
                                    // which has NESTED usage and so is left untouched.
                                    assertEquals(Map.of("parent", simpleRename("T2"), "child", "T1"),
                                            constituentTypeNames(renamed));
                                }),
                        Arguments.of("UnnestedRenamed.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // "child" names T1, which is itself a renamed RECORD type, so it follows the rename
                                    // just like "parent" does.
                                    assertEquals(Map.of("parent", simpleRename("T2"), "child", simpleRename("T1")),
                                            constituentTypeFullNames(renamed));
                                }),
                        Arguments.of("UnnestedRenamedNested.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // "child" names T1.A1, which is nested within the renamed T1, so its fully
                                    // qualified name follows the rename of T1.
                                    assertEquals(
                                            Map.of("parent", simpleRename("T2"), "child", simpleRename("T1") + ".A1"),
                                            constituentTypeFullNames(renamed));
                                }),
                        Arguments.of("Joined.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    final SyntheticRecordType<?> join = renamed.getSyntheticRecordType("JOIN");
                                    assertEquals(Set.of(simpleRename("T1"), simpleRename("T2")),
                                            join.getConstituents().stream()
                                                    .map(constituent -> constituent.getRecordType().getName())
                                                    .collect(Collectors.toSet()));
                                }),
                        Arguments.of("AlsoInDependency.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    final Descriptors.Descriptor uuidType = getMessage(renamed, simpleRename("UUID"));
                                    assertEquals(uuidType,
                                            getFieldMessageType(renamed, simpleRename("T2"), "uuid"));
                                    assertNotEquals(uuidType,
                                            getFieldMessageType(renamed, simpleRename("T2"), "uuid2"));
                                }),
                        Arguments.of("NestedAndRecordType.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // if this assertion fails, it does not have a good toString, but you can add `.toProto()` to both for a better
                                    // toString
                                    assertEquals(getMessage(renamed, simpleRename("T1")),
                                            getFieldMessageType(renamed, simpleRename("T2"), "T1"));
                                }),
                        Arguments.of("NestedMessage.json",
                                (Consumer<RecordMetaData>) renamed -> {
                                    // if this assertion fails, it does not have a good toString, but you can add `.toProto()` to both for a better
                                    // toString
                                    assertEquals(getMessage(renamed, "T1"),
                                            getFieldMessageType(renamed, simpleRename("T2"), "T1"));
                                })
                ));
    }

    @Nonnull
    private static Descriptors.Descriptor getMessage(final RecordMetaData renamed, final String T1) {
        return renamed.getRecordsDescriptor().getMessageTypes().stream()
                .filter(type -> type.getName().equals(T1))
                .findFirst().orElseThrow();
    }

    @Nonnull
    private static Descriptors.Descriptor getFieldMessageType(final RecordMetaData renamed, String typeName, String fieldName) {
        return renamed.getRecordType(typeName)
                .getDescriptor().getFields()
                .stream().filter(field -> field.getName().equals(fieldName))
                .findFirst().orElseThrow().getMessageType();
    }

    /**
     * Maps each constituent’s own name (e.g. "parent", "child") to the name of the record type it names, for the
     * unnested synthetic record type named {@code "__3_syntheticType_1"} in the fixtures that use it.
     */
    @Nonnull
    private static Map<String, String> constituentTypeNames(final RecordMetaData renamed) {
        return renamed.getSyntheticRecordType("__3_syntheticType_1").getConstituents().stream()
                .collect(Collectors.toMap(SyntheticRecordType.Constituent::getName,
                        constituent -> constituent.getRecordType().getName()));
    }

    /**
     * Like {@link #constituentTypeNames}, but maps each constituent to the fully qualified name of the message type it
     * names, which also tells apart a type nested within a renamed type.
     */
    @Nonnull
    private static Map<String, String> constituentTypeFullNames(final RecordMetaData renamed) {
        return renamed.getSyntheticRecordType("__3_syntheticType_1").getConstituents().stream()
                .collect(Collectors.toMap(SyntheticRecordType.Constituent::getName,
                        constituent -> constituent.getRecordType().getDescriptor().getFullName()));
    }

    /**
     * Tests that prefixing every record type name in each of the renamable fixtures works, both batched and one by one.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void renameWithSimplePrefix(String name, Consumer<RecordMetaData> extraAssertions) throws IOException {
        final RecordMetaData renamed = runRename(name);
        extraAssertions.accept(renamed);
    }

    /**
     * Tests that the rename rejects a renamer that maps two distinct record types to the same new name.
     */
    @Test
    void renameRejectsCollidingRenames() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> "Collision",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that a batch that swaps the names of two record types must succeed (since neither rename target collides
     * with an “unrenamed” type). Unlike the other rename tests here, this one is batched-only because a swap is
     * order-dependent for the one-by-one path (e.g., renaming "UUID" to "T2" first would collide with the
     * not-yet-renamed "T2").
     */
    @Test
    void renameRecordTypesAllowsSwappingTwoNames() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("AlsoInDependency.json").build();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder,
                name -> name.equals("UUID") ? "T2" : name.equals("T2") ? "UUID" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        assertEquals(Set.of("UUID", "T2"), renamed.getRecordTypes().keySet());
        // Content moves with the name: The original single-field shape of UUID is now under "T2", and vice versa.
        assertEquals(1, renamed.getRecordType("T2").getDescriptor().getFields().size());
        assertEquals(3, renamed.getRecordType("UUID").getDescriptor().getFields().size());
    }

    /**
     * Tests that a batch that swaps two names must succeed even when neither type has a canonically named union field,
     * so that no union field name changes and only their {@code typeName} references are swapped.
     */
    @Test
    void renameRecordTypesAllowsSwappingTwoNamesWithoutCanonicalUnionFields() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("NonCanonicalUnionFields.json").build();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder,
                name -> name.equals("T1") ? "T2" : name.equals("T2") ? "T1" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        assertEquals(Set.of("T1", "T2"), renamed.getRecordTypes().keySet());
        // Content moves with the name: The original two-field shape of T2 is now under "T1", and vice versa.
        assertEquals(1, renamed.getRecordType("T2").getDescriptor().getFields().size());
        assertEquals(2, renamed.getRecordType("T1").getDescriptor().getFields().size());
        // The union field names are untouched, since neither was canonical to begin with, but "a" now points at the
        // type called "T2" and "b" at the one called "T1".
        final Descriptors.Descriptor union = getMessage(renamed, RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        assertEquals("T2", union.findFieldByName("a").getMessageType().getName());
        assertEquals("T1", union.findFieldByName("b").getMessageType().getName());
    }

    /**
     * Tests that the rename rejects a renamer whose target is an existing top-level {@code NESTED} type. Renaming
     * {@code T2} to {@code T1} in {@code NestedMessage.json} collides with the {@code NESTED}-usage {@code T1}, even
     * though {@code T1} is not itself a renamable {@code RECORD}-usage type.
     */
    @Test
    void renameRejectsNameOfNestedType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("NestedMessage.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T2") ? "T1" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that a record type may be renamed to the name of a message nested inside another type, since the two live
     * in different scopes. References to the nested message must keep pointing at it rather than at the newcomer.
     */
    @Test
    void renameAllowsNameOfNestedMessage() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("NestedMessageSameName.json").build();
        final RecordMetaData renamed = runRename(originalProto,
                name -> name.equals("T1") ? "Inner" : name,
                newName -> newName.equals("Inner") ? "T1" : newName);
        assertEquals(Set.of("Inner", "T2"), renamed.getRecordTypes().keySet());
        // T2’s "inner" field still resolves to T2.Inner, not to the newly named top-level "Inner".
        final Descriptors.Descriptor nestedInner = getFieldMessageType(renamed, "T2", "inner");
        assertEquals("T2.Inner", nestedInner.getFullName());
        assertNotSame(getMessage(renamed, "Inner"), nestedInner);
    }

    /**
     * Tests that when several union fields reference the type being renamed, only the canonically named one is renamed
     * along with it, and all of them still resolve to the renamed type.
     */
    @Test
    void renameUpdatesOnlyTheCanonicalOfSeveralUnionFields() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("DuplicateUnionFields.json").build();
        final RecordMetaData renamed = runRename(originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);
        final Descriptors.Descriptor union = getMessage(renamed, RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        final Descriptors.Descriptor renamedT1 = getMessage(renamed, simpleRename("T1"));
        // "_T1" was canonical for T1 and follows the rename; "_T1_1" was not and keeps its name. Both still point at
        // the renamed type.
        assertEquals(Set.of("_" + simpleRename("T1"), "_T1_1", "_" + simpleRename("T2")),
                union.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
        assertSame(renamedT1, union.findFieldByName("_" + simpleRename("T1")).getMessageType());
        assertSame(renamedT1, union.findFieldByName("_T1_1").getMessageType());
    }

    /**
     * Tests that the rename rejects any renaming when the union references a record type backed by a message type
     * nested within another one, even if the renamer leaves that record type alone or is the identity. In the fixture,
     * the union references {@code T2.Inner}, which makes {@code Inner} such a record type.
     */
    @Test
    void renameRejectsNestedRecordType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("UnionFieldToNestedType.json").build();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        for (final UnaryOperator<String> renamer : List.<UnaryOperator<String>>of(
                name -> name.equals("T2") ? simpleRename("T2") : name,
                name -> name.equals("T2") ? "Inner" : name,
                UnaryOperator.identity())) {
            final MetaDataException exception = assertThrows(MetaDataException.class,
                    () -> MetaDataProtoEditor.renameRecordTypes(originalProto.toBuilder(), renamer, dependencies));
            assertEquals("Renaming record types with record types backed by nested message types is not supported",
                    exception.getMessage());
            crossCheckRenameRecordTypesIsRejected(originalProto, renamer, dependencies);
        }
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} rejects renaming a {@code RECORD}-usage type to the name
     * of a record type backed by a message type nested within another one. In the fixture, the union references
     * {@code T2.Inner} through a non-canonically named field, so that the rename does not also collide on the
     * canonical union field name {@code _Inner}.
     */
    @Test
    void renameRecordTypeRejectsNameOfNestedRecordType() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnionFieldToNestedType.json");
        builder.getRecordsBuilder().addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("T1")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("ID")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)));
        for (final DescriptorProtos.DescriptorProto.Builder messageType :
                builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                for (final DescriptorProtos.FieldDescriptorProto.Builder field : messageType.getFieldBuilderList()) {
                    if (field.getName().equals("_Inner")) {
                        field.setName("inner");
                    }
                }
                messageType.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("_T1")
                        .setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("T1"));
            }
        }
        assertRenameRecordTypeRejected(builder.build(), "T1", "Inner",
                "Cannot rename record type as a record type of the new name already exists");
    }

    /**
     * Tests that the rename works against a union message type that is not called {@code RecordTypeUnion} but declares
     * {@code UNION} usage explicitly.
     */
    @Test
    void renameWithExplicitlyMarkedUnion() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        for (final DescriptorProtos.DescriptorProto.Builder messageType :
                builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                messageType.setName("MyUnion");
                messageType.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record,
                        RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION)
                                .build());
            }
        }
        final RecordMetaData renamed = runRename(builder.build(),
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);
        assertEquals(Set.of(simpleRename("T1"), simpleRename("T2")), renamed.getRecordTypes().keySet());
        // The union keeps its own name, and its canonical fields follow the types they reference.
        final Descriptors.Descriptor union = getMessage(renamed, "MyUnion");
        assertEquals(Set.of("_" + simpleRename("T1"), "_" + simpleRename("T2")),
                union.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of a (distinct, un-renamed) existing
     * type.
     */
    @Test
    void renameRejectsNameOfExistingType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? "T2" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of a synthetic record type, whether
     * joined or unnested, and that the one-by-one rename rejects it with the same message.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "Joined.json, T1, JOIN",
            "UnnestedInternal.json, T2, __3_syntheticType_1",
    })
    void renameRejectsNameOfSyntheticType(String name, String recordType, String syntheticTypeName)
            throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        final String expectedMessage =
                "Cannot rename record type as a synthetic record type of the new name already exists";
        crossCheckRenameRecordTypesIsRejected(originalProto,
                typeName -> typeName.equals(recordType) ? syntheticTypeName : typeName,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                expectedMessage);
        assertRenameRecordTypeRejected(originalProto, recordType, syntheticTypeName, expectedMessage);
    }

    /**
     * Tests that renaming a {@code NESTED} type to its own name via {@link MetaDataProtoEditor#renameRecordType}
     * is a no-op even when a synthetic record type shares that name, which {@link RecordMetaDataBuilder} allows for a
     * {@code NESTED} type.
     */
    @Test
    void renameRecordTypeToSameNameIgnoresSyntheticTypeName() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("Joined.json");
        builder.getRecordsBuilder().addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("JOIN"));
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        MetaDataProtoEditor.renameRecordType(builder, "JOIN", "JOIN",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals(originalProto, builder.build());
    }

    /**
     * Tests that the rename rejects any renaming when the union references an imported record type, i.e., one whose
     * message type is defined in a dependency file rather than in {@code MetaData.records}, even for an identity
     * renamer. In the fixture, every record type is imported.
     */
    @Test
    void renameRejectsImportedType() {
        final RecordMetaDataProto.MetaData originalProto =
                RecordMetaData.build(TestRecordsImportProto.getDescriptor()).toProto();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        for (final UnaryOperator<String> renamer : List.<UnaryOperator<String>>of(
                MetaDataProtoEditorUnitTest::simpleRename, UnaryOperator.identity())) {
            final MetaDataException exception = assertThrows(MetaDataException.class,
                    () -> MetaDataProtoEditor.renameRecordTypes(originalProto.toBuilder(), renamer, dependencies));
            assertEquals("Renaming record types with imported record types is not supported", exception.getMessage());
            crossCheckRenameRecordTypesIsRejected(originalProto, renamer, dependencies);
        }
    }

    /**
     * Tests that the rename rejects an imported record type even where {@code MetaData.records} also declares an
     * unrelated top-level message type of the same name, before anything has been modified. Unlike the other exception
     * tests here, this one is batched-only, because {@link MetaDataProtoEditor#renameRecordType} renames the local
     * message type in this situation instead of rejecting the rename.
     */
    @Test
    void renameRecordTypesRejectsImportedTypeShadowedByLocalMessage() {
        final RecordMetaDataProto.MetaData originalProto =
                RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor()).toProto();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        // "MySimpleRecord" names the imported record type, but is also the name of a local NESTED message type.
        assertEquals(List.of("MySimpleRecord", "MyOtherRecord"),
                MetaDataProtoEditor.getRecordTypes(originalProto.toBuilder()));
        assertEquals("com.apple.foundationdb.record.test1.MySimpleRecord",
                RecordMetaData.build(originalProto).getRecordType("MySimpleRecord")
                        .getDescriptor().getFullName());

        final Set<String> renamerSawNames = new LinkedHashSet<>();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(builder, name -> {
                    renamerSawNames.add(name);
                    return simpleRename(name);
                }, dependencies));
        assertEquals("Renaming record types with imported record types is not supported", exception.getMessage());
        // The rename is rejected before the renamer is consulted, and before anything has been modified.
        assertEquals(Set.of(), renamerSawNames);
        assertEquals(originalProto, builder.build());
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} allows renaming the union to the name of an imported
     * record type, since only a {@code RECORD}-usage type shares the record type namespace. The fixture first moves the
     * local {@code NESTED} type of that name out of the way, so that the name refers to the imported record type only.
     */
    @Test
    void renameRecordTypeAllowsRenamingUnionToNameOfImportedType() {
        final RecordMetaDataProto.MetaData originalProto =
                RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor()).toProto();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordType(builder, "MySimpleRecord", "MyLocalSimpleRecord", dependencies);
        MetaDataProtoEditor.renameRecordType(builder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "MySimpleRecord",
                dependencies);
        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        assertEquals("MySimpleRecord", renamed.getUnionDescriptor().getName());
        assertEquals("com.apple.foundationdb.record.test1.MySimpleRecord",
                renamed.getRecordType("MySimpleRecord").getDescriptor().getFullName());
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} rejects renaming a {@code RECORD}-usage type to the name
     * of an imported record type that the union references, even if {@code MetaData.record_types} does not list it.
     */
    @Test
    void renameRecordTypeRejectsNameOfImportedTypeMissingFromRecordTypes() {
        final RecordMetaDataProto.MetaData originalProto =
                RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor()).toProto();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        // Move the local NESTED type out of the way, and drop the imported record type from the record type list.
        MetaDataProtoEditor.renameRecordType(builder, "MySimpleRecord", "MyLocalSimpleRecord",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        final List<RecordMetaDataProto.RecordType> recordTypes = builder.getRecordTypesList().stream()
                .filter(recordType -> !recordType.getName().equals("MySimpleRecord"))
                .toList();
        builder.clearRecordTypes().addAllRecordTypes(recordTypes);
        assertRenameRecordTypeRejected(builder.build(), "MyOtherRecord", "MySimpleRecord",
                "Cannot rename record type as a record type of the new name already exists");
    }

    /**
     * Tests that the rename determines the record types from the union message type rather than from
     * {@code MetaData.record_types}, the same way {@link RecordMetaDataBuilder} does. In the fixture,
     * {@code MetaData.record_types} is empty, which is valid as long as the primary keys come from the
     * {@code (field).primary_key} extension instead. This test is batched-only, because the one-by-one renaming of
     * {@link #renameRecordTypesOneByOne} only visits the record types listed in {@code MetaData.record_types}. See
     * {@link #renameRecordTypeRenamesRecordTypeMissingFromRecordTypes} for the {@code renameRecordType} counterpart.
     */
    @Test
    void renameRecordTypesRenamesRecordTypesMissingFromRecordTypes() {
        final RecordMetaDataProto.MetaData originalProto = RecordMetaData.build(TestRecords1Proto.getDescriptor())
                .toProto().toBuilder().clearRecordTypes().clearIndexes().build();
        final RecordMetaData originalMetaData = RecordMetaData.newBuilder().setRecords(originalProto, true).build();
        assertEquals(Set.of("MySimpleRecord", "MyOtherRecord"), originalMetaData.getRecordTypes().keySet());

        final Set<String> renamerSawNames = new LinkedHashSet<>();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder, name -> {
            renamerSawNames.add(name);
            return simpleRename(name);
        }, RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals(Set.of("MySimpleRecord", "MyOtherRecord"), renamerSawNames);
        assertEquals(List.of(), MetaDataProtoEditor.getRecordTypes(builder));

        final RecordMetaData renamed = RecordMetaData.newBuilder().setRecords(builder.build(), true).build();
        assertEquals(Set.of(simpleRename("MySimpleRecord"), simpleRename("MyOtherRecord")),
                renamed.getRecordTypes().keySet());
        // Indexes declared through the `(field).index` extension are named after their record type, so they follow
        // the rename too.
        assertNotNull(originalMetaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(renamed.getIndex(simpleRename("MySimpleRecord") + "$num_value_3_indexed"));
        // Assert that `MetaDataEvolutionValidator` rejects the result as an evolution of the original. Since the
        // subspace key of an index defaults to its name, a renamed index is a different index.
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataEvolutionValidator.newBuilder()
                        .setAllowNoVersionChange(true)
                        .build()
                        .validate(originalMetaData, renamed));
        assertEquals("index missing in new meta-data", exception.getMessage());
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} renames a record type that has a union field but no entry
     * in {@code MetaData.record_types}, consistent with {@link MetaDataProtoEditor#renameRecordTypes}. The message
     * type, its canonical union field and the indexes on it are renamed, while the record type list is left alone.
     */
    @Test
    void renameRecordTypeRenamesRecordTypeMissingFromRecordTypes() {
        final RecordMetaDataProto.MetaData.Builder builder =
                RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto().toBuilder();
        // Drop MySimpleRecord from the record type list, leaving its _MySimpleRecord union field in place. The union
        // field is what makes the rename resolve MySimpleRecord as a RECORD-usage type in the first place.
        final List<RecordMetaDataProto.RecordType> recordTypes = builder.getRecordTypesList().stream()
                .filter(recordType -> !recordType.getName().equals("MySimpleRecord"))
                .toList();
        builder.clearRecordTypes().addAllRecordTypes(recordTypes);
        final RecordMetaDataProto.MetaData originalProto = builder.build();

        MetaDataProtoEditor.renameRecordType(builder, "MySimpleRecord", "MyNewSimpleRecord",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals(List.of("MyNewSimpleRecord", "MyOtherRecord", RecordMetaDataBuilder.DEFAULT_UNION_NAME),
                builder.getRecords().getMessageTypeList().stream()
                        .map(DescriptorProtos.DescriptorProto::getName).toList());
        final DescriptorProtos.DescriptorProto union = builder.getRecords().getMessageTypeList().stream()
                .filter(messageType -> messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME))
                .findFirst().orElseThrow();
        assertEquals(List.of("_MyNewSimpleRecord", "_MyOtherRecord"),
                union.getFieldList().stream().map(DescriptorProtos.FieldDescriptorProto::getName).toList());
        assertEquals(List.of("MyOtherRecord"), MetaDataProtoEditor.getRecordTypes(builder));
        // Every index on MySimpleRecord now names the renamed record type instead.
        final List<RecordMetaDataProto.Index> originalIndexes = originalProto.getIndexesList().stream()
                .filter(index -> index.getRecordTypeList().contains("MySimpleRecord"))
                .toList();
        assertFalse(originalIndexes.isEmpty());
        for (final RecordMetaDataProto.Index index : builder.getIndexesList()) {
            assertFalse(index.getRecordTypeList().contains("MySimpleRecord"));
        }
        assertEquals(originalIndexes.size(), builder.getIndexesList().stream()
                .filter(index -> index.getRecordTypeList().contains("MyNewSimpleRecord"))
                .count());
        // The renamed metadata still builds, with the renamed record type taking its primary key from the
        // `(field).primary_key` extension.
        final RecordMetaData renamed = RecordMetaData.newBuilder().setRecords(builder.build(), true).build();
        assertEquals(Set.of("MyNewSimpleRecord", "MyOtherRecord"), renamed.getRecordTypes().keySet());
    }

    /**
     * Tests that a record type can be renamed in a metadata that declares a package and an unnested record type whose
     * parent is a different record type. The parent constituent names its record type by simple name, which does not
     * resolve to a descriptor once the records descriptor declares a package, so it must not be looked up.
     */
    @Test
    void renameWithPackageAndUnnestedTypeOfOtherParent() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnnestedInternal.json");
        final DescriptorProtos.FileDescriptorProto.Builder records = builder.getRecordsBuilder();
        records.setPackage("com.example");
        records.addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("T3")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("ID")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)));
        for (final DescriptorProtos.DescriptorProto.Builder messageType : records.getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                messageType.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("_T3")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("T3"));
            }
        }
        builder.addRecordTypes(builder.getRecordTypes(0).toBuilder().setName("T3"));
        // A non-parent constituent names its type by its fully qualified name.
        builder.getUnnestedRecordTypesBuilder(0).getNestedConstituentsBuilder(1).setTypeName("com.example.T1");
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        // Ensure that the original metadata is valid.
        RecordMetaData.build(originalProto);
        final RecordMetaData renamed = runRename(originalProto,
                name -> name.equals("T3") ? simpleRename("T3") : name,
                name -> name.equals(simpleRename("T3")) ? "T3" : name);
        assertEquals(Set.of("T2", simpleRename("T3")), renamed.getRecordTypes().keySet());
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} renames a {@code NESTED} type that a non-parent unnested
     * constituent uses, rewriting the constituent along with it. In the fixture, the {@code child} constituent names
     * the {@code NESTED} type {@code T1}.
     */
    @Test
    void renameRecordTypeRenamesNestedTypeUsedByNonParentConstituent() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnnestedInternal.json");
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        MetaDataProtoEditor.renameRecordType(builder, "T1", simpleRename("T1"),
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        assertEquals(Map.of("parent", "T2", "child", simpleRename("T1")), constituentTypeFullNames(renamed));
    }

    /**
     * Tests that renaming rewrites the fully qualified name of a non-parent unnested constituent whose type is nested,
     * one or two levels deep, within a renamed type in a records descriptor that declares a package.
     */
    @ParameterizedTest
    @ValueSource(strings = {"A1", "A1.B1"})
    void renameWithPackageAndNonParentConstituentNestedInRenamedType(String nestedPath) throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnnestedRenamedNested.json");
        final DescriptorProtos.FileDescriptorProto.Builder records = builder.getRecordsBuilder();
        records.setPackage("com.example");
        for (final DescriptorProtos.DescriptorProto.Builder messageType : records.getMessageTypeBuilderList()) {
            if (messageType.getName().equals("T1")) {
                // Nest B1 within T1.A1.
                messageType.getNestedTypeBuilder(0).addNestedType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("B1")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("B1V")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)));
            } else if (messageType.getName().equals("T2")) {
                for (final DescriptorProtos.FieldDescriptorProto.Builder field : messageType.getFieldBuilderList()) {
                    if (field.getName().equals("T1")) {
                        field.setTypeName("T1." + nestedPath);
                    }
                }
            }
        }
        builder.getUnnestedRecordTypesBuilder(0).getNestedConstituentsBuilder(1)
                .setTypeName("com.example.T1." + nestedPath);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        assertEquals(Map.of("parent", "com.example.T2", "child", "com.example.T1." + nestedPath),
                constituentTypeFullNames(RecordMetaData.build(originalProto)));

        final RecordMetaData renamed = runRename(originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);
        assertEquals(Map.of("parent", "com.example." + simpleRename("T2"),
                        "child", "com.example." + simpleRename("T1") + "." + nestedPath),
                constituentTypeFullNames(renamed));
    }

    /**
     * Tests that renaming a {@code NESTED} type leaves alone an unnested record type whose parent constituent names
     * an imported record type of the same simple name. Only renaming a {@code RECORD}-usage type updates parent
     * constituents.
     */
    @Test
    void renameRecordTypeOfNestedTypeLeavesParentConstituentOfImportedTypeAlone() {
        final RecordMetaDataProto.MetaData.Builder builder =
                RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor()).toProto().toBuilder();
        builder.addUnnestedRecordTypes(RecordMetaDataProto.UnnestedRecordType.newBuilder()
                .setName("Unnested")
                .addNestedConstituents(RecordMetaDataProto.UnnestedRecordType.NestedConstituent.newBuilder()
                        .setName("parent")
                        .setTypeName("MySimpleRecord")));
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        MetaDataProtoEditor.renameRecordType(builder, "MySimpleRecord", "MyLocalSimpleRecord",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals("MySimpleRecord", builder.getUnnestedRecordTypes(0).getNestedConstituents(0).getTypeName());
        assertEquals(originalProto.getUnnestedRecordTypesList(), builder.getUnnestedRecordTypesList());
    }

    /**
     * Tests that renaming a top-level record type does not get blocked by an unrelated nested type that merely shares
     * its simple name. In the fixture, the top-level {@code T1} is renamed while an unnested constituent references
     * {@code T2}’s own nested type, also named {@code T1}; only a fully-qualified comparison tells the two apart.
     */
    @Test
    void renameNotBlockedByShadowedNestedTypeName() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("UnnestedShadowedName.json").build();
        // Ensure that the original metadata is valid.
        RecordMetaData.build(originalProto);
        final RecordMetaData renamed = runRename(originalProto,
                name -> name.equals("T1") ? simpleRename("T1") : name,
                name -> name.equals(simpleRename("T1")) ? "T1" : name);
        assertEquals(Set.of(simpleRename("T1"), "T2"), renamed.getRecordTypes().keySet());
        // The shadowing nested type keeps its own name, and the constituent still points at it rather than at the
        // renamed top-level type. (Constituent type names are reported as simple names, so "T1" here is T2’s nested
        // type; the descriptor's full name below distinguishes it from the renamed top-level one.)
        assertEquals(Map.of("parent", "T2", "child", "T1"), constituentTypeNames(renamed));
        assertEquals("T2.T1", renamed.getSyntheticRecordType("__3_syntheticType_1").getConstituents().stream()
                .filter(constituent -> constituent.getName().equals("child"))
                .findFirst().orElseThrow().getRecordType().getDescriptor().getFullName());
    }

    /**
     * The mirror image of {@link #renameNotBlockedByShadowedNestedTypeName}. The fixture is the same but for the
     * constituent naming the top-level {@code T1} instead of {@code T2}’s shadowing nested type of the same name, so
     * renaming {@code T1} must now rename the constituent too, while the shadowing nested type keeps its name.
     */
    @Test
    void renameNotHiddenByShadowedNestedTypeName() throws IOException {
        final RecordMetaDataProto.MetaData originalProto =
                loadMetaData("UnnestedShadowedNameTopLevel.json").build();
        // Ensure that the original metadata is valid.
        RecordMetaData.build(originalProto);
        final RecordMetaData renamed = runRename(originalProto,
                name -> name.equals("T1") ? simpleRename("T1") : name,
                name -> name.equals(simpleRename("T1")) ? "T1" : name);
        assertEquals(Map.of("parent", "T2", "child", simpleRename("T1")), constituentTypeFullNames(renamed));
        assertNotNull(getMessage(renamed, "T2").findNestedTypeByName("T1"));
    }

    /**
     * Tests that the rename rejects a renamer whose canonical union field name would collide with an existing,
     * non-canonically-named union field of another (un-renamed) type.
     */
    @Test
    void renameRejectsUnionFieldCollision() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("DuplicateUnionFields.json").build();
        final String expectedMessage = "Cannot rename union field because a field of the new name already exists";
        crossCheckRenameRecordTypesIsRejected(originalProto,
                name -> name.equals("T2") ? "T1_1" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                expectedMessage);
        assertRenameRecordTypeRejected(originalProto, "T2", "T1_1", expectedMessage);
    }

    /**
     * Tests that the rename rejects any renaming when the metadata has {@code user_defined_functions}, since a
     * user-defined function may be a string that references record types by name in ways that renaming cannot safely
     * account for. A single {@code RECORD}-usage rename via {@link MetaDataProtoEditor#renameRecordType} must leave
     * the builder untouched.
     */
    @Test
    void renameRejectsUserDefinedFunctions() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addUserDefinedFunctions(RecordMetaDataProto.PUserDefinedFunction.newBuilder().build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                "Renaming record types with UserDefinedFunctions is not supported");
        assertRenameRecordTypeRejected(originalProto, "T1", simpleRename("T1"),
                "Renaming record types with UserDefinedFunctions is not supported");
    }

    /**
     * Tests that {@link MetaDataProtoEditor#renameRecordType} rejects renaming a {@code NESTED} type or the union when
     * the metadata has {@code user_defined_functions}, just as it does for a {@code RECORD}-usage type.
     */
    @Test
    void renameRecordTypeRejectsUserDefinedFunctionsForAnyUsage() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("NestedMessage.json");
        builder.addUserDefinedFunctions(RecordMetaDataProto.PUserDefinedFunction.newBuilder().build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final String expectedMessage = "Renaming record types with UserDefinedFunctions is not supported";
        assertRenameRecordTypeRejected(originalProto, "T1", simpleRename("T1"), expectedMessage);
        assertRenameRecordTypeRejected(originalProto, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "MyUnion",
                expectedMessage);
    }

    /**
     * Tests that the rename rejects any renaming when the metadata declares views, whose definition is a SQL string
     * that may reference record types by name.
     */
    @Test
    void renameRejectsViews() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addViews(RecordMetaDataProto.PView.newBuilder()
                .setName("V1").setDefinition("SELECT * FROM T1"));
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(originalProto, MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                "Renaming record types with views is not supported");
        assertRenameRecordTypeRejected(originalProto, "T1", simpleRename("T1"),
                "Renaming record types with views is not supported");
    }

    /**
     * Tests that the rename rejects any renaming when the metadata declares stored queries, whose query is a SQL
     * string that may reference record types by name.
     */
    @Test
    void renameRejectsStoredQueries() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addStoredQueries(RecordMetaDataProto.PStoredQuery.newBuilder()
                .setName("Q1").setQuery("SELECT * FROM T1"));
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(originalProto, MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                "Renaming record types with stored queries is not supported");
        assertRenameRecordTypeRejected(originalProto, "T1", simpleRename("T1"),
                "Renaming record types with stored queries is not supported");
    }

    /**
     * Tests that the rename rejects a {@code RECORD}-usage type that (for whatever reason) carries the default union
     * name while some other message type is the actual union. Renaming such a type would set its {@code record.usage}
     * option to {@code UNION} and leave the metadata with two types claiming to be the union. Note that
     * {@link RecordMetaDataBuilder} rejects such a records descriptor outright, so the rename can only ever encounter
     * it as a raw proto. The point of the check is to report it rather than silently make it worse.
     */
    @Test
    void renameRejectsDefaultUnionNamedType() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        for (final DescriptorProtos.DescriptorProto.Builder messageType :
                builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                // Make the real union a differently named type that declares UNION usage explicitly, and point its
                // first field at the type that is about to take over the default union name.
                messageType.setName("MyUnion");
                messageType.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record,
                        RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION)
                                .build());
                messageType.getFieldBuilder(0).setTypeName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
            } else if (messageType.getName().equals("T1")) {
                messageType.setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
            }
        }
        builder.getRecordTypesBuilder(0).setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final String expectedMessage = "Cannot rename a non-union record type that has the default union name";
        crossCheckRenameRecordTypesIsRejected(originalProto, MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                expectedMessage);
        assertRenameRecordTypeRejected(originalProto, RecordMetaDataBuilder.DEFAULT_UNION_NAME,
                simpleRename(RecordMetaDataBuilder.DEFAULT_UNION_NAME), expectedMessage);
    }

    /**
     * Tests that the rename rejects a renamer that maps a non-union record type to the default union name. In the
     * fixture, the union is called {@code MyUnion} and declares {@code UNION} usage explicitly, so that the default
     * union name is not otherwise taken.
     */
    @Test
    void renameRejectsDefaultUnionName() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        for (final DescriptorProtos.DescriptorProto.Builder messageType :
                builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                messageType.setName("MyUnion");
                messageType.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record,
                        RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION)
                                .build());
            }
        }
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final String expectedMessage = "Cannot rename record type to the default union name";
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? RecordMetaDataBuilder.DEFAULT_UNION_NAME : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                expectedMessage);
        assertRenameRecordTypeRejected(originalProto, "T1", RecordMetaDataBuilder.DEFAULT_UNION_NAME, expectedMessage);
    }

    /**
     * Tests that the rename rejects any renaming when the union message type itself declares a nested type, including
     * a rename of the union itself via {@link MetaDataProtoEditor#renameRecordType}.
     */
    @Test
    void renameRejectsNestedTypeInUnion() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        for (final DescriptorProtos.DescriptorProto.Builder messageType : builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                messageType.addNestedType(DescriptorProtos.DescriptorProto.newBuilder().setName("Nested"));
            }
        }
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final String expectedMessage = "Nested types in union type not supported";
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()),
                expectedMessage);
        assertRenameRecordTypeRejected(originalProto, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "MyUnion",
                expectedMessage);
    }

    /**
     * Tests that the rename rejects any renaming when {@code MetaData.records} has no union message type at all.
     */
    @Test
    void renameRejectsMissingUnion() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        final DescriptorProtos.FileDescriptorProto.Builder recordsBuilder = builder.getRecordsBuilder();
        final List<DescriptorProtos.DescriptorProto> withoutUnion = recordsBuilder.getMessageTypeList().stream()
                .filter(messageType -> !messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME))
                .collect(Collectors.toList());
        recordsBuilder.clearMessageType().addAllMessageType(withoutUnion);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects any renaming when an unnested record type’s non-parent constituent names a type
     * that cannot be resolved in the file descriptor.
     */
    @Test
    void renameRejectsMissingNestedConstituentDescriptor() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnnestedInternal.json");
        // Ensure the original metadata is valid.
        RecordMetaData.build(builder.build());
        builder.getUnnestedRecordTypesBuilder(0).getNestedConstituentsBuilder(1).setTypeName("DoesNotExist");
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that renaming every record type twice in a row gives the same result as a single rename that applies the
     * prefix twice.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void renameTwice(String name) throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData(name);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final RecordMetaData originalMetaData = RecordMetaData.build(originalProto);
        final Descriptors.FileDescriptor[] dependencies = RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        MetaDataProtoEditor.renameRecordTypes(builder, MetaDataProtoEditorUnitTest::simpleRename, dependencies);

        final RecordMetaDataProto.MetaData firstRename = builder.build();
        crossCheckRenamedMetaData(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                dependencies,
                firstRename);
        basicRenameAsserts(firstRename, originalMetaData,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        // again
        MetaDataProtoEditor.renameRecordTypes(builder, MetaDataProtoEditorUnitTest::simpleRename, dependencies);

        final RecordMetaDataProto.MetaData secondRename = builder.build();
        crossCheckRenamedMetaData(
                firstRename,
                MetaDataProtoEditorUnitTest::simpleRename,
                dependencies,
                secondRename);
        basicRenameAsserts(secondRename, RecordMetaData.build(firstRename),
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        final RecordMetaDataProto.MetaData.Builder restartBuilder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(
                restartBuilder,
                oldName -> simpleRename(simpleRename(oldName)),
                dependencies);
        assertEquals(builder.build(), secondRename);

    }

    @ParameterizedTest
    @BooleanSource("t1Conflicts")
    void renameRecordTypesWithConflictingName(boolean t1Conflicts) throws IOException {
        // Set up a metadata in which one type already holds the prefixed name of the other, then prefix everything.
        // Switch which one gets renamed first based on `t1Conflicts` to make sure it is consistent regardless of the
        // ordering of types. Batched renaming validates the mapping as a whole, so the shift succeeds either way;
        // this is exactly the kind of scenario where one-by-one renaming is *not* expected to match the batched
        // renaming, so we use `runRenameBatchedOnly()` here rather than `runRename()`.
        final String prefix = "__Q_";
        final RecordMetaData withConflict = runRenameBatchedOnly(loadMetaData("TwoBoringTypes.json").build(),
                oldName -> {
                    if (t1Conflicts) {
                        return !oldName.equals("T1") ? prefix + "T1" : oldName;
                    } else {
                        return oldName.equals("T1") ? prefix + "T2" : oldName;
                    }
                },
                newName -> newName.startsWith(prefix) ? newName.substring(prefix.length()) : newName);
        final String conflicting = t1Conflicts ? "T1" : "T2";
        assertEquals(Set.of(conflicting, prefix + conflicting), withConflict.getRecordTypes().keySet());

        final RecordMetaData prefixed = runRenameBatchedOnly(withConflict.toProto(),
                oldName -> prefix + oldName,
                newName -> newName.substring(prefix.length()));
        assertEquals(Set.of(prefix + conflicting, prefix + prefix + conflicting),
                prefixed.getRecordTypes().keySet());
    }

    /**
     * Tests that a renamer that returns every type’s own name unchanged is a true no-op, both batched and one by one.
     * The metadata must come out byte-for-byte identical to how it went in.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void renameToSameNameIsNoOp(String name) throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder, UnaryOperator.identity(), dependencies);
        assertEquals(originalProto, builder.build());
        assertEquals(originalProto, renameRecordTypesOneByOne(originalProto, UnaryOperator.identity(), dependencies));
    }

    /**
     * Exercises {@link MetaDataProtoEditor#renameRecordTypes} with a large number of record types, to get a rough
     * sense of how it scales.
     */
    @Test
    @Tag(Tags.Performance)
    void renameManyRecordTypes() {
        final int typeCount = 200;
        final RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();
        builder.setRecords(DescriptorProtos.FileDescriptorProto.newBuilder()
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME)));
        for (int i = 0; i < typeCount; i++) {
            MetaDataProtoEditor.addRecordType(builder,
                    DescriptorProtos.DescriptorProto.newBuilder()
                            .setName("T" + i)
                            .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                    .setName("ID")
                                    .setNumber(1)
                                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                            .build(),
                    Key.Expressions.field("ID"));
        }

        final long startNanos = System.nanoTime();
        MetaDataProtoEditor.renameRecordTypes(builder, MetaDataProtoEditorUnitTest::simpleRename, new Descriptors.FileDescriptor[0]);
        final long elapsedNanos = System.nanoTime() - startNanos;
        LOGGER.info("Renamed {} record types in {} ms", typeCount, elapsedNanos / 1e6);

        assertEquals(typeCount, RecordMetaData.build(builder.build()).getRecordTypes().size());
    }

    /**
     * Tests that renaming the record types of a metadata whose indexes come from the {@code (field).index} extension
     * renames the record types the indexes apply to, and leaves the indexes otherwise unchanged.
     */
    @Test
    void renameWithAnnotations() {
        final RecordMetaDataProto.MetaData original = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor())
                .build().toProto();

        assertNotEquals(0, original.getIndexesCount());
        final RecordMetaData renamed = runRename(original,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        assertEquals(original.getIndexesList(),
                renamed.toProto().getIndexesList()
                        .stream().map(renamedIndex -> {
                            final RecordMetaDataProto.Index.Builder builder = renamedIndex.toBuilder();
                            final List<String> newTypes = builder.getRecordTypeList().stream()
                                    .map(MetaDataProtoEditorUnitTest::simpleRenameUndo)
                                    .collect(Collectors.toList());
                            builder.clearRecordType();
                            builder.addAllRecordType(newTypes);
                            return builder.build();
                        })
                        .collect(Collectors.toList()));
    }

    /**
     * Renaming a record type that has enum-typed fields, whose type names have to follow the renamed enclosing type.
     */
    @Test
    void renameWithEnumFields() {
        final RecordMetaDataProto.MetaData original = RecordMetaData.newBuilder()
                .setRecords(TestRecordsEnumProto.getDescriptor())
                .build().toProto();

        final RecordMetaData renamed = runRename(original,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        // The enum fields still resolve to their nested enum types, which are unchanged but for their enclosing type.
        final Descriptors.Descriptor shapeRecord = getMessage(renamed, simpleRename("MyShapeRecord"));
        for (final String fieldName : List.of("size", "color", "shape")) {
            final Descriptors.EnumDescriptor originalEnum =
                    TestRecordsEnumProto.MyShapeRecord.getDescriptor().findFieldByName(fieldName).getEnumType();
            final Descriptors.EnumDescriptor renamedEnum = shapeRecord.findFieldByName(fieldName).getEnumType();
            assertEquals(originalEnum.toProto(), renamedEnum.toProto());
            assertEquals(simpleRename(originalEnum.getContainingType().getName()),
                    renamedEnum.getContainingType().getName());
        }
    }

    @Nonnull
    private static String simpleRenameUndo(final String newName) {
        assertEquals("__x_", newName.substring(0, 4));
        return newName.substring(4);
    }

    @Nonnull
    private static String simpleRename(final String oldName) {
        return "__x_" + oldName;
    }

    @Nonnull
    private RecordMetaData runRename(final String name) throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        return runRename(originalProto, MetaDataProtoEditorUnitTest::simpleRename, MetaDataProtoEditorUnitTest::simpleRenameUndo);
    }

    /**
     * Performs the rename via the batched {@link MetaDataProtoEditor#renameRecordTypes} method, then cross-checks that
     * an equivalent one-by-one sequence of {@link MetaDataProtoEditor#renameRecordType} calls produces byte-for-byte
     * the same metadata.
     *
     * @see #runRenameBatchedOnly
     */
    @Nonnull
    private RecordMetaData runRename(final RecordMetaDataProto.MetaData originalProto,
                                     final UnaryOperator<String> rename,
                                     final Function<String, String> undoRename) {
        final RecordMetaData renamed = runRenameBatchedOnly(originalProto, rename, undoRename);
        final Descriptors.FileDescriptor[] dependencies = RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        crossCheckRenamedMetaData(originalProto, rename, dependencies, renamed.toProto());
        return renamed;
    }

    /**
     * Performs the rename using (only) the batched {@link MetaDataProtoEditor#renameRecordTypes} method, without any
     * cross-checking. This helper is used for renames where the one-by-one path is not expected to match; in
     * particular, renamings whose validity is sensitive to iteration order (see
     * {@link #renameRecordTypesWithConflictingName}).
     *
     * @see #runRename
     */
    @Nonnull
    private RecordMetaData runRenameBatchedOnly(final RecordMetaDataProto.MetaData originalProto,
                                                final UnaryOperator<String> rename,
                                                final Function<String, String> undoRename) {
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        final RecordMetaData originalMetaData = RecordMetaData.build(originalProto);
        final Descriptors.FileDescriptor[] dependencies = RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        MetaDataProtoEditor.renameRecordTypes(builder, rename, dependencies);
        final RecordMetaDataProto.MetaData build = builder.build();
        return basicRenameAsserts(build, originalMetaData, rename, undoRename);
    }

    @Nonnull
    private static RecordMetaData basicRenameAsserts(final RecordMetaDataProto.MetaData build,
                                                     final RecordMetaData originalMetaData,
                                                     final Function<String, String> renamer,
                                                     final Function<String, String> undoRename) {
        final RecordMetaData renamed = RecordMetaData.build(build);
        // Renaming record types has to be a valid evolution of the original metadata. It does not bump the version,
        // hence `setAllowNoVersionChange`.
        MetaDataEvolutionValidator.newBuilder()
                .setAllowNoVersionChange(true)
                .build()
                .validate(originalMetaData, renamed);
        final Set<String> expectedNewNames = originalMetaData.getRecordTypes().keySet()
                .stream().map(renamer)
                .collect(Collectors.toSet());
        assertEquals(expectedNewNames, renamed.getRecordTypes().keySet());
        assertEquals(expectedNewNames,
                renamed.getRecordTypes().values().stream().map(RecordType::getName)
                        .collect(Collectors.toSet()));
        for (final RecordType type : renamed.getRecordTypes().values()) {
            assertEquals(type.getAllIndexes(),
                    originalMetaData.getRecordType(undoRename.apply(type.getName()))
                            .getAllIndexes());
        }
        assertEquals(originalMetaData.getUniversalIndexes(), renamed.getUniversalIndexes());
        return renamed;
    }

    /**
     * This test solely exists to decrease the chance that someone will add something to the metadata protobuf, and not
     * update the {@link MetaDataProtoEditor}. Any new field that can reference a record type has to be either rewritten
     * by {@link MetaDataProtoEditor#renameRecordTypes} or rejected by it.
     */
    @Test
    void validateMetaDataCoverage() {
        assertEquals(Set.of(
                        "split_long_records", "version", "former_indexes", "record_count_key",
                        "store_record_versions", "dependencies", "subspace_key_counter", "uses_subspace_key_counter",
                        // the below reference record types, and are rewritten by the rename
                        "records", "indexes", "record_types", "joined_record_types", "unnested_record_types",
                        // the below may reference record types from within a string, so the rename rejects them
                        "user_defined_functions", "views", "stored_queries"),
                RecordMetaDataProto.MetaData.getDescriptor().getFields().stream()
                        .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toSet()));
    }
}
