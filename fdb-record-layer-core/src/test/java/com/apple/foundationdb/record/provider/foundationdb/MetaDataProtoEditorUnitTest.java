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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.metadata.Key;
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
 * <p>There are more tests for this class (in action) in {@link FDBMetaDataStoreTest}. Those tests are more end-to-end,
 * and they are for doing things like testing that when the metadata are read from the database, edited, and written
 * back, everything works.
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
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.renameRecordType(metaDataProtoBuilder, "OuterRecord", "OtterRecord",
                getDependencies(metaData));
        Descriptors.FileDescriptor renamedDescriptor = Descriptors.FileDescriptor.buildFrom(metaDataProtoBuilder.getRecords(), dependencies);
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

        // Cross-check: Renaming the same single type via the batched `renameRecordTypes()` method should produce
        // a byte-for-byte identical result.
        final RecordMetaDataProto.MetaData.Builder batchedBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.renameRecordTypes(
                batchedBuilder,
                name -> name.equals("OuterRecord") ? "OtterRecord" : name,
                getDependencies(metaData));
        assertEquals(metaDataProtoBuilder.build(), batchedBuilder.build());
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
     * Asserts that applying {@code renamer} via the batched {@link MetaDataProtoEditor#renameRecordTypes} method
     * produces byte-for-byte the same metadata as applying it one type at a time via {@link #renameRecordTypesOneByOne}.
     * Note though that this cross-check is only applicable to renamings whose validity depends on iteration order
     * (see {@link #conflictingName}).
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
    private static void crossRenameRecordTypesIsRejected(
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
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.renameRecordType(metaDataProtoBuilder, "OuterRecord", "OtterRecord",
                        getDependencies(metaData));
        Descriptors.FileDescriptor renamedDescriptor = Descriptors.FileDescriptor.buildFrom(metaDataProtoBuilder.getRecords(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor renamedOuter = renamedDescriptor.findMessageTypeByName("OtterRecord");
        Descriptors.Descriptor renamedOuterOuter = renamedOuter.findNestedTypeByName("OuterRecord");
        assertSame(renamedOuterOuter, renamedOuter.findFieldByName("middle").getMessageType());
        assertSame(renamedOuterOuter, renamedOuter.findFieldByName("many_middle").getMessageType());
        assertSame(renamedDescriptor.findMessageTypeByName("OtherRecord"), renamedOuter.findFieldByName("other").getMessageType());
        Descriptors.Descriptor renamedOuterOuterInner = renamedOuterOuter.findNestedTypeByName("InnerRecord");
        assertSame(renamedOuterOuterInner, renamedOuterOuter.findFieldByName("inner").getMessageType());
        assertSame(renamedOuter, renamedOuterOuterInner.findFieldByName("outer").getMessageType());

        // Cross-check: Renaming the same single type via the batched `renameRecordTypes()` should produce a
        // byte-for-byte identical result.
        final RecordMetaDataProto.MetaData.Builder batchedBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.renameRecordTypes(batchedBuilder,
                name -> name.equals("OuterRecord") ? "OtterRecord" : name, getDependencies(metaData));
        assertEquals(metaDataProtoBuilder.build(), batchedBuilder.build());
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
        // Note: Explicitly having the .json here so that you can Cmd+click in the IDE to jump to the file
        return Stream.concat(
                Stream.of(
                        "OneBoringType.json",
                        "TwoBoringTypes.json",
                        "TwoBoringTypesInPackage.json",
                        "DuplicateUnionFields.json",
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
     * Maps each constituent's own name (e.g. "parent", "child") to the name of the record type it names, for the
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
     * The rename must reject a renamer that maps two distinct record types to the same new name.
     */
    @Test
    void batchedRejectsCollidingRenames() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossRenameRecordTypesIsRejected(
                originalProto,
                name -> "Collision",
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * A batch that swaps two record types' names must succeed, since neither rename target collides with an
     * "unrenamed" type — both are moving. This exercises the {@code beingRenamed} exclusion in
     * {@code validateUnionFieldRenames()} (and the analogous exclusion in {@code analyzeRecordTypeRenames()}),
     * without which this scenario would be spuriously rejected. Unlike the other rename tests here, this one is
     * batched-only: like {@link #conflictingName}, a swap is order-dependent for the one-by-one path (renaming
     * "UUID" to "T2" first would collide with the not-yet-renamed "T2").
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
        // Content moves with the name: UUID's original single-field shape is now under "T2", and vice versa.
        assertEquals(1, renamed.getRecordType("T2").getDescriptor().getFields().size());
        assertEquals(3, renamed.getRecordType("UUID").getDescriptor().getFields().size());
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
        RecordMetaData.build(originalProto); // ensure original metadata is valid
        crossRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * The rename must reject a renamer that maps a record type to the name of a distinct, un-renamed existing type.
     */
    @Test
    void batchedRejectsRenameToExistingType() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? "T2" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * The rename must reject a renamer that maps a record type to the name of an existing record type that is
     * registered in {@code MetaData.record_types} but has no corresponding message type in {@code MetaData.records}
     * (as would be the case for a record type whose message is defined in a dependency file, i.e., "imported").
     */
    @Test
    void batchedRejectsRenameToImportedType() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder().setName("Imported").build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? "Imported" : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * An imported record type, i.e., one registered in {@code MetaData.record_types} whose message type lives in a
     * dependency file rather than in {@code MetaData.records}, must be left untouched by the rename, and the renamer
     * must not even be applied to it. This is a documented divergence from
     * {@link MetaDataProtoEditor#renameRecordType}, which rejects the same rename outright, so it is asserted
     * separately for each path.
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
     * Renaming a top-level record type must not be blocked by an unrelated nested type that merely shares its simple
     * name. In the fixture, the top-level {@code T1} is renamed while an unnested constituent references {@code T2}'s
     * own nested type, also named {@code T1}; only a fully-qualified comparison tells the two apart.
     */
    @Test
    void shadowedNestedTypeNameDoesNotBlockRename() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("UnnestedShadowedName.json").build();
        RecordMetaData.build(originalProto); // ensure original metadata is valid
        final RecordMetaData renamed = runRename(originalProto,
                name -> name.equals("T1") ? simpleRename("T1") : name,
                name -> name.equals(simpleRename("T1")) ? "T1" : name);
        assertEquals(Set.of(simpleRename("T1"), "T2"), renamed.getRecordTypes().keySet());
        // The shadowing nested type keeps its own name, and the constituent still points at it rather than at the
        // renamed top-level type. (Constituent type names are reported as simple names, so "T1" here is T2's nested
        // type; the descriptor's full name below distinguishes it from the renamed top-level one.)
        assertEquals(Map.of("parent", "T2", "child", "T1"), constituentTypeNames(renamed));
        assertEquals("T2.T1", renamed.getSyntheticRecordType("__3_syntheticType_1").getConstituents().stream()
                .filter(constituent -> constituent.getName().equals("child"))
                .findFirst().orElseThrow().getRecordType().getDescriptor().getFullName());
    }

    /**
     * The rename must reject a renamer whose canonical union field name would collide with an existing,
     * non-canonically-named union field of another (un-renamed) type. Unlike the other exception tests here, this
     * one is batched-only: renaming one type at a time via {@link MetaDataProtoEditor#renameRecordType} silently
     * leaves the colliding field under its old name instead of throwing (see {@link #conflictingName}).
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
                .startsWith("Cannot rename union field to ")
                .endsWith("as a field of that name already exists");
    }

    /**
     * The rename must reject any renaming when the metadata has {@code user_defined_functions}, since a
     * user-defined function may be a string that references record types by name in ways that renaming cannot
     * safely account for.
     */
    @Test
    void batchedRejectsUserDefinedFunctions() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("TwoBoringTypes.json");
        builder.addUserDefinedFunctions(RecordMetaDataProto.PUserDefinedFunction.newBuilder().build());
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * The rename must reject a renamer that maps a non-union record type to the default union name.
     */
    @Test
    void batchedRejectsRenameToDefaultUnionName() throws IOException {
        final RecordMetaDataProto.MetaData originalProto = loadMetaData("TwoBoringTypes.json").build();
        crossRenameRecordTypesIsRejected(
                originalProto,
                name -> name.equals("T1") ? RecordMetaDataBuilder.DEFAULT_UNION_NAME : name,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * The rename must reject any renaming when the union message type itself declares a nested type.
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
        crossRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * A record type whose name contains a {@code '.'} cannot correspond to any message type in
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
     * The rename must reject any renaming when {@code MetaData.records} has no union message type at all.
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
        crossRenameRecordTypesIsRejected(
                originalProto,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
    }

    /**
     * The rename must reject any renaming when an unnested record type's non-parent constituent names a type that
     * cannot be resolved in the file descriptor.
     */
    @Test
    void batchedRejectsMissingNestedConstituentDescriptor() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData("UnnestedInternal.json");
        RecordMetaData.build(builder.build()); // ensure original metadata is valid
        builder.getUnnestedRecordTypesBuilder(0).getNestedConstituentsBuilder(1).setTypeName("DoesNotExist");
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        crossRenameRecordTypesIsRejected(
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
        // In the future we may want to make this work, but for now, I just want to assert that it either succeeds
        // and produces a valid metadata, or fails with a clear exception. Currently, it is order dependent.
        // Note also that this is exactly the kind of scenario where one-by-one renaming is *not* expected to match
        // the batched renaming; so we use `runRenameBatchedOnly()` here rather than `runRename()`.
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
        if (t1Conflicts) {
            assertEquals(Set.of("T1", prefix + "T1"), withConflict.getRecordTypes().keySet());
        } else {
            assertEquals(Set.of("T2", prefix + "T2"), withConflict.getRecordTypes().keySet());
        }
        try {
            runRenameBatchedOnly(withConflict.toProto(),
                    oldName -> prefix + oldName,
                    newName -> newName.substring(prefix.length()));
        } catch (MetaDataException e) {
            Assertions.assertThat(e.getMessage())
                    .satisfiesAnyOf(
                            message -> Assertions.assertThat(message).startsWith("Cannot rename record type to ").endsWith("as it already exists"),
                            message -> Assertions.assertThat(message).startsWith("Cannot rename union field to ").endsWith("as a field of that name already exists"));
        }
    }

    /**
     * A renamer that returns every type's own name unchanged must be a true no-op: the metadata must come out
     * byte-for-byte identical to how it went in, confirming that the early-return path for an empty rename set
     * never mutates anything.
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
     * sense of how it scales. Not a strict benchmark: there is no assertion on timing, only a log line. Run via the
     * {@code performanceTest} Gradle task.
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
     * Perform the rename using (only) the batched {@link MetaDataProtoEditor#renameRecordTypes} method, without any
     * cross-checking. Use this for renamings whose validity is sensitive to iteration order (see
     * {@link #conflictingName}), since the one-by-one path is not expected to match those.
     *
     * @see #runRename(RecordMetaDataProto.MetaData, UnaryOperator, Function)
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
     * update the {@link MetaDataProtoEditor}.
     */
    @Test
    void validateMetaDataCoverage() {
        assertEquals(Set.of(
                        "split_long_records", "version", "former_indexes", "record_count_key",
                        "store_record_versions", "dependencies", "subspace_key_counter", "uses_subspace_key_counter",
                        "stored_queries",
                        // the below reference record types
                        "records", "indexes", "record_types", "joined_record_types", "unnested_record_types",
                        "user_defined_functions", "views"),
                RecordMetaDataProto.MetaData.getDescriptor().getFields().stream()
                        .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toSet()));
    }
}
