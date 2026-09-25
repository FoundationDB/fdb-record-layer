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
import org.assertj.core.api.Assertions;
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void simplePrefix(String name, Consumer<RecordMetaData> extraAssertions) throws IOException {
        final RecordMetaData renamed = runRename(name);
        extraAssertions.accept(renamed);
    }

    /**
     * Tests that the rename rejects a renamer that maps two distinct record types to the same new name.
     */
    @Test
    void batchedRejectsCollidingRenames() throws IOException {
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
    void batchedAllowsSwappingTwoNames() throws IOException {
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
    void batchedAllowsSwappingTwoNamesWithoutCanonicalUnionFields() throws IOException {
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
    void batchedRejectsRenameToNestedType() throws IOException {
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
    void batchedAllowsRenameToNestedMessageName() throws IOException {
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
    void batchedRenamesOnlyTheCanonicalOfSeveralUnionFields() throws IOException {
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
     * Tests that a union field referencing a message nested inside a renamed type has its {@code typeName} rewritten
     * to follow the renamed parent. The nested type itself is registered as a record type but is not top-level, so the
     * renamer is never applied to it.
     */
    @Test
    void batchedRenamesUnionFieldReferenceToNestedType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("UnionFieldToNestedType.json").build();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder,
                name -> name.equals("T2") ? simpleRename("T2") : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        // "Inner" cannot be renamed, since it is not a top-level message type, but it moves with its parent.
        assertEquals(Set.of(simpleRename("T2"), "Inner"), renamed.getRecordTypes().keySet());
        assertEquals(simpleRename("T2") + ".Inner",
                renamed.getRecordType("Inner").getDescriptor().getFullName());
        final Descriptors.Descriptor union = getMessage(renamed, RecordMetaDataBuilder.DEFAULT_UNION_NAME);
        assertEquals(simpleRename("T2") + ".Inner",
                union.findFieldByName("_Inner").getMessageType().getFullName());
    }

    /**
     * Tests that the rename works against a union message type that is not called {@code RecordTypeUnion} but declares
     * {@code UNION} usage explicitly.
     */
    @Test
    void batchedRenamesWithExplicitlyMarkedUnion() throws IOException {
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
     * The rename must reject the schemas that rename a type used by a non-parent unnested constituent.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "UnnestedRenamed.json",
            "UnnestedRenamedNested.json",
    })
    void unsupported(String name) throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        // Ensure that the original metadata is valid.
        RecordMetaData.build(originalProto);
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of a (distinct, un-renamed) existing
     * type.
     */
    @Test
    void batchedRejectsRenameToExistingType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? "T2" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of an existing record type that is
     * registered in {@code MetaData.record_types} but has no corresponding message type in {@code MetaData.records}
     * (as would be the case for a record type whose message is defined in a dependency file, i.e., “imported”).
     */
    @Test
    void batchedRejectsRenameToImportedType() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder().setName("Imported").build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? "Imported" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of a record type backed by a nested
     * message type. In the fixture, the union references {@code T2.Inner}, which makes {@code Inner} a record type.
     */
    @Test
    void batchedRejectsRenameToNestedRecordType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("UnionFieldToNestedType.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T2") ? "Inner" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer that maps a record type to the name of a synthetic record type, whether
     * joined or unnested. Unlike the other exception tests here, this one is batched-only because
     * {@link MetaDataProtoEditor#renameRecordType} does not check for such a collision at all.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "Joined.json, T1, JOIN",
            "UnnestedInternal.json, T2, __3_syntheticType_1",
    })
    void batchedRejectsRenameToSyntheticType(String name, String recordType, String syntheticTypeName)
            throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(
                        originalProto.toBuilder(),
                        typeName -> typeName.equals(recordType) ? syntheticTypeName : typeName,
                        RecordMetaDataBuilder.getDependencies(originalProto, Map.of())));
        assertEquals("Cannot rename record type as a synthetic record type of the new name already exists",
                exception.getMessage());
    }

    /**
     * Tests that an imported record type, i.e., one registered in {@code MetaData.record_types} whose message type
     * lives in a dependency file rather than in {@code MetaData.records}, is left untouched by the rename, and that the
     * renamer is not applied to it. (This is a documented divergence from {@link MetaDataProtoEditor#renameRecordType},
     * which would reject such a rename outright.)
     */
    @Test
    void batchedSkipsImportedType() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder().setName("Imported").build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final Descriptors.FileDescriptor[] dependencies =
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of());

        // The batched path renames the real top-level types, and never consults the renamer for the imported one.
        final Set<String> renamerSawNames = new LinkedHashSet<>();
        final RecordMetaDataProto.MetaData.Builder batchedBuilder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(batchedBuilder, name -> {
            renamerSawNames.add(name);
            return simpleRename(name);
        }, dependencies);
        assertEquals(Set.of("T1", "T2"), renamerSawNames);
        assertEquals(List.of(simpleRename("T1"), simpleRename("T2"), "Imported"),
                MetaDataProtoEditor.getRecordTypes(batchedBuilder));

        // The one-by-one path, by contrast, rejects the very same rename.
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordType(originalProto.toBuilder(), "Imported",
                        simpleRename("Imported"), dependencies));
        assertEquals("No record type found with name Imported", exception.getMessage());
    }

    /**
     * Tests the case where a record type name refers to an imported message type while {@code MetaData.records} also
     * declares an unrelated top-level message type of the same name. The renamer is not consulted for the name at all,
     * so the imported record type keeps its name and the local message type is left alone too, being a {@code NESTED}
     * type like any other. Unlike the other rename tests here, this one is batched-only, because
     * {@link MetaDataProtoEditor#renameRecordType} does rename the local message type in this situation.
     */
    @Test
    void batchedSkipsImportedTypeShadowedByLocalMessage() {
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
        MetaDataProtoEditor.renameRecordTypes(builder, name -> {
            renamerSawNames.add(name);
            return simpleRename(name);
        }, dependencies);
        final RecordMetaDataProto.MetaData renamedProto = builder.build();

        // The renamer is never consulted for the imported record type, even though a local message type shares its
        // name, so only the genuinely local record type is renamed.
        assertEquals(Set.of("MyOtherRecord"), renamerSawNames);
        assertEquals(List.of("MySimpleRecord", simpleRename("MyOtherRecord")),
                MetaDataProtoEditor.getRecordTypes(builder));

        final RecordMetaData renamed = RecordMetaData.build(renamedProto);
        assertEquals("com.apple.foundationdb.record.test1.MySimpleRecord",
                renamed.getRecordType("MySimpleRecord").getDescriptor().getFullName());
        // The local NESTED message type keeps its name, and both fields still point where they did.
        assertEquals("MySimpleRecord",
                getFieldMessageType(renamed, simpleRename("MyOtherRecord"), "simple").getName());
        assertEquals("com.apple.foundationdb.record.test1.MySimpleRecord",
                getFieldMessageType(renamed, simpleRename("MyOtherRecord"), "imported_simple").getFullName());
    }

    /**
     * Tests that the rename determines the record types from the union message type rather than from
     * {@code MetaData.record_types}, the same way {@link RecordMetaDataBuilder} does. In the fixture,
     * {@code MetaData.record_types} is empty, which is valid as long as the primary keys come from the
     * {@code (field).primary_key} extension instead. Unlike the other rename tests here, this one is batched-only,
     * because {@link MetaDataProtoEditor#renameRecordType} rejects such metadata for lacking the record type entry.
     */
    @Test
    void batchedRenamesRecordTypesMissingFromRecordTypes() {
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
        // the rename too. That is inherent to such metadata, which is why this is not a valid evolution of the original.
        assertNotNull(originalMetaData.getIndex("MySimpleRecord$num_value_3_indexed"));
        assertNotNull(renamed.getIndex(simpleRename("MySimpleRecord") + "$num_value_3_indexed"));
    }

    /**
     * Tests that renaming a top-level record type does not get blocked by an unrelated nested type that merely shares
     * its simple name. In the fixture, the top-level {@code T1} is renamed while an unnested constituent references
     * {@code T2}’s own nested type, also named {@code T1}; only a fully-qualified comparison tells the two apart.
     */
    @Test
    void shadowedNestedTypeNameDoesNotBlockRename() throws IOException {
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
     * The mirror image of {@link #shadowedNestedTypeNameDoesNotBlockRename}. The fixture is the same but for the
     * constituent naming the top-level {@code T1} instead of {@code T2}’s shadowing nested type of the same name, so
     * renaming {@code T1} must now be rejected.
     */
    @Test
    void shadowedNestedTypeNameDoesNotHideRename() throws IOException {
        final RecordMetaDataProto.MetaData originalProto =
                loadMetaData("UnnestedShadowedNameTopLevel.json").build();
        // Ensure that the original metadata is valid.
        RecordMetaData.build(originalProto);
        crossCheckRenameRecordTypesIsRejected(originalProto,
                name -> name.equals("T1") ? simpleRename("T1") : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects a renamer whose canonical union field name would collide with an existing,
     * non-canonically-named union field of another (un-renamed) type. Unlike the other exception tests here, this
     * one is batched-only because renaming one type at a time via {@link MetaDataProtoEditor#renameRecordType} silently
     * leaves the colliding field under its old name instead of throwing (see also {@link #conflictingName}).
     */
    @Test
    void batchedRejectsUnionFieldCollision() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("DuplicateUnionFields.json").build();
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(
                        originalProto.toBuilder(),
                        name -> name.equals("T2") ? "T1_1" : name,
                        RecordMetaDataBuilder.getDependencies(originalProto, Map.of())));
        Assertions.assertThat(exception.getMessage())
                .isEqualTo("Cannot rename union field because a field of the new name already exists");
    }

    /**
     * Tests that the rename rejects any renaming when the metadata has {@code user_defined_functions}, since a
     * user-defined function may be a string that references record types by name in ways that renaming cannot safely
     * account for.
     */
    @Test
    void batchedRejectsUserDefinedFunctions() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addUserDefinedFunctions(RecordMetaDataProto.PUserDefinedFunction.newBuilder().build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects any renaming when the metadata declares views, whose definition is a SQL string
     * that may reference record types by name. Unlike {@link #batchedRejectsUserDefinedFunctions}, this one is
     * batched-only because {@link MetaDataProtoEditor#renameRecordType} does not check for views.
     */
    @Test
    void batchedRejectsViews() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addViews(RecordMetaDataProto.PView.newBuilder()
                .setName("V1").setDefinition("SELECT * FROM T1"));
        assertBatchedRenameRejected(builder.build(), "Renaming record types with views is not supported");
    }

    /**
     * Tests that the rename rejects any renaming when the metadata declares stored queries, whose query is a SQL
     * string that may reference record types by name. Unlike {@link #batchedRejectsUserDefinedFunctions}, this one is
     * batched-only because {@link MetaDataProtoEditor#renameRecordType} does not check for stored queries.
     */
    @Test
    void batchedRejectsStoredQueries() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addStoredQueries(RecordMetaDataProto.PStoredQuery.newBuilder()
                .setName("Q1").setQuery("SELECT * FROM T1"));
        assertBatchedRenameRejected(builder.build(), "Renaming record types with stored queries is not supported");
    }

    /**
     * Tests that the rename rejects a {@code RECORD}-usage type that (for whatever reason) carries the default union
     * name while some other message type is the actual union. Renaming such a type would set its {@code record.usage}
     * option to {@code UNION} and leave the metadata with two types claiming to be the union. Note that
     * {@link RecordMetaDataBuilder} rejects such a records descriptor outright, so the rename can only ever encounter
     * it as a raw proto. The point of the check is to report it rather than silently make it worse.
     */
    @Test
    void batchedRejectsRenamingDefaultUnionNamedType() throws IOException {
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
        assertBatchedRenameRejected(builder.build(),
                "Cannot rename a non-union record type that has the default union name");
    }

    /**
     * Asserts that the batched {@link MetaDataProtoEditor#renameRecordTypes} rejects {@link #simpleRename} on
     * {@code originalProto} with exactly {@code expectedMessage}.
     */
    private static void assertBatchedRenameRejected(@Nonnull RecordMetaDataProto.MetaData originalProto,
                                                    @Nonnull String expectedMessage) {
        final MetaDataException exception = assertThrows(MetaDataException.class,
                () -> MetaDataProtoEditor.renameRecordTypes(originalProto.toBuilder(),
                        MetaDataProtoEditorUnitTest::simpleRename,
                        RecordMetaDataBuilder.getDependencies(originalProto, Map.of())));
        assertEquals(expectedMessage, exception.getMessage());
    }

    /**
     * Tests that the rename rejects a renamer that maps a non-union record type to the default union name.
     */
    @Test
    void batchedRejectsRenameToDefaultUnionName() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? RecordMetaDataBuilder.DEFAULT_UNION_NAME : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that the rename rejects any renaming when the union message type itself declares a nested type.
     */
    @Test
    void batchedRejectsNestedTypeInUnion() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        for (final DescriptorProtos.DescriptorProto.Builder messageType : builder.getRecordsBuilder().getMessageTypeBuilderList()) {
            if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                messageType.addNestedType(DescriptorProtos.DescriptorProto.newBuilder().setName("Nested"));
            }
        }
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossCheckRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * Tests that a record type whose name contains a {@code '.'} cannot correspond to any message type in
     * {@code MetaData.records}, since message type names are always simple identifiers. It is therefore treated as
     * imported and skipped, rather than rejected.
     */
    @Test
    void batchedSkipsDottedRecordTypeName() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder().setName("a.b").build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final Descriptors.FileDescriptor[] dependencies = RecordMetaDataBuilder.getDependencies(originalProto, Map.of());
        final RecordMetaDataProto.MetaData.Builder renamed = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(renamed, name -> name.equals("a.b") ? "c" : name, dependencies);
        assertEquals(List.of("T1", "T2", "a.b"), MetaDataProtoEditor.getRecordTypes(renamed));
    }

    /**
     * Tests that the rename rejects any renaming when {@code MetaData.records} has no union message type at all.
     */
    @Test
    void batchedRejectsMissingUnion() throws IOException {
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
    void batchedRejectsMissingNestedConstituentDescriptor() throws IOException {
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void doubleRename(String name) throws IOException {
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
    void conflictingName(boolean t1Conflicts) throws IOException {
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
     * Tests that a renamer that returns every type’s own name unchanged is a true no-op. The metadata must come out
     * byte-for-byte identical to how it went in.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void identityRenameIsNoOp(String name) throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData(name).build();
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(builder, UnaryOperator.identity(),
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals(originalProto, builder.build());
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

    @Test
    void withAnnotations() {
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
    void withEnumFields() {
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
     * cross-checking. This helper is used for renames where the one-by-one path is not expected to match;
     * in particular, renamings whose validity is sensitive to iteration order (see {@link #conflictingName}).
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
