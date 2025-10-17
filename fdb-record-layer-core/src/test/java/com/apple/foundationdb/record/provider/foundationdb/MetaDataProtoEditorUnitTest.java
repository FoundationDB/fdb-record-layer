/*
 * MetaDataProtoEditorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor.FieldTypeMatch;
import com.apple.test.BooleanSource;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the meta-data proto editor. There are more tests for this class (in action) in the {@link FDBMetaDataStoreTest}
 * class. Those tests are more end-to-end, and they are for doing things like testing that when the meta-data
 * are read from the database, edited, and written back, everything works. These tests focus on just the editor
 * itself.
 */
public class MetaDataProtoEditorUnitTest {

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
                        "UniversalIndex.json",
                        "UnnestedExternalType.json",
                        "UnnestedInternal.json",
                        "Joined.json"
                ).map(filename -> Arguments.of(filename, (Consumer<RecordMetaData>) renamed -> { })),
                Stream.of(
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void simplePrefix(String name, Consumer<RecordMetaData> extraAssertions) throws IOException {
        final RecordMetaData renamed = runRename(name);
        extraAssertions.accept(renamed);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "UnnestedRenamed.json",
            "UnnestedRenamedNested.json",
    })
    void unsupported(String name) throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData(name);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        RecordMetaData.build(originalProto); // ensure original metadata is valid
        assertThrows(MetaDataException.class, () -> MetaDataProtoEditor.renameRecordTypes(builder,
                MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(builder.build(), Map.of())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("renamableFiles")
    void doubleRename(String name) throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData(name);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final RecordMetaData originalMetaData = RecordMetaData.build(originalProto);
        MetaDataProtoEditor.renameRecordTypes(builder, MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));

        final RecordMetaDataProto.MetaData firstRename = builder.build();
        basicRenameAsserts(firstRename, originalMetaData,
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        // again
        MetaDataProtoEditor.renameRecordTypes(builder, MetaDataProtoEditorUnitTest::simpleRename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));

        final RecordMetaDataProto.MetaData secondRename = builder.build();
        basicRenameAsserts(secondRename, RecordMetaData.build(firstRename),
                MetaDataProtoEditorUnitTest::simpleRename,
                MetaDataProtoEditorUnitTest::simpleRenameUndo);

        final RecordMetaDataProto.MetaData.Builder restartBuilder = originalProto.toBuilder();
        MetaDataProtoEditor.renameRecordTypes(restartBuilder,
                oldName -> simpleRename(simpleRename(oldName)),
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));
        assertEquals(builder.build(), secondRename);

    }

    @ParameterizedTest
    @BooleanSource("t1Conflicts")
    void conflictingName(boolean t1Conflicts) throws IOException {
        // In the future we may want to make this work, but for now, I just want to assert that it either succeeds
        // and produces a valid metadata, or fails with a clear exception. Currently, it is order dependent.
        final String prefix = "__Q_";
        final RecordMetaData withConflict = runRename(loadMetaData("TwoBoringTypes.json").build(),
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
            runRename(withConflict.toProto(),
                    oldName -> prefix + oldName,
                    newName -> newName.substring(prefix.length()));
        } catch (MetaDataException e) {
            Assertions.assertThat(e)
                    .hasMessageStartingWith("Cannot rename record type to ")
                    .hasMessageEndingWith("as it already exists");
        }
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

    @Nonnull
    private RecordMetaData runRename(final RecordMetaDataProto.MetaData originalProto,
                                     final Function<String, String> rename,
                                     final Function<String, String> undoRename) {
        final RecordMetaDataProto.MetaData.Builder builder = originalProto.toBuilder();
        final RecordMetaData originalMetaData = RecordMetaData.build(originalProto);
        MetaDataProtoEditor.renameRecordTypes(builder, rename,
                RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));

        final RecordMetaDataProto.MetaData build = builder.build();
        return basicRenameAsserts(build, originalMetaData,
                rename,
                undoRename);
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
                        // the below reference record types
                        "records", "indexes", "record_types", "joined_record_types", "unnested_record_types",
                        "user_defined_functions", "views"),
                RecordMetaDataProto.MetaData.getDescriptor().getFields().stream()
                        .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toSet()));
    }
}
