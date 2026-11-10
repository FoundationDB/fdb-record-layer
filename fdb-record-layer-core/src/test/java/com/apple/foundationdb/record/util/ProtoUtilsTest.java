/*
 * ProtoUtilsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecords5Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsNameClashProto;
import com.apple.foundationdb.record.TestRecordsRepeatedImportProto;
import com.apple.foundationdb.record.TestRecordsTupleFieldsProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.TypeTestProto;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditorUnitTest;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests of the {@link ProtoUtils} class.
 */
class ProtoUtilsTest {

    static Stream<Arguments> protobufCompliantNameTestArguments() {
        return Stream.of(
                Arguments.of("__", "__"),
                Arguments.of("_", "_"),
                Arguments.of("$", null),
                Arguments.of(".", null),
                Arguments.of("__hello", "__hello"),
                Arguments.of("__$hello", "____1hello"),
                Arguments.of("__$hello", "____1hello"),
                Arguments.of("__.hello", "____2hello"),
                Arguments.of("____hello", "____0hello"),
                Arguments.of("__$h$e$l$l$o", "____1h__1e__1l__1l__1o"),
                Arguments.of("__0", null),
                Arguments.of("__0hello", null),
                Arguments.of("__1", null),
                Arguments.of("__1hello", null),
                Arguments.of("__2", null),
                Arguments.of("__2hello", null),
                Arguments.of("__4", "__4"),
                Arguments.of("__$$$$", "____1__1__1__1"),
                Arguments.of("______", "____0__0"),
                Arguments.of("__4hello", "__4hello"),
                Arguments.of("$", null),
                Arguments.of("$hello", null),
                Arguments.of(".", null),
                Arguments.of(".hello", null),
                Arguments.of("h__e__l__l__o", "h__0e__0l__0l__0o"),
                Arguments.of("h.e.l.l.o", "h__2e__2l__2l__2o"),
                Arguments.of("h$e$l$l$o", "h__1e__1l__1l__1o"),
                Arguments.of("1hello", null),
                Arguments.of("डेटाबेस", null)
        );
    }

    @ParameterizedTest
    @MethodSource("protobufCompliantNameTestArguments")
    void protobufCompliantNameTest(@Nonnull String userIdentifier, @Nullable String protobufIdentifier) {
        if (protobufIdentifier != null) {
            final String actual = ProtoUtils.toProtoBufCompliantName(userIdentifier);
            assertThat(actual)
                    .isEqualTo(protobufIdentifier);
            final String reTranslated = ProtoUtils.toUserIdentifier(actual);
            assertThat(reTranslated)
                    .isEqualTo(userIdentifier);
        } else {
            assertThatThrownBy(() -> ProtoUtils.toProtoBufCompliantName(userIdentifier))
                    .isInstanceOf(ProtoUtils.InvalidNameException.class);
        }
    }

    @Test
    void uniqueTypeName() {
        final String name1 = ProtoUtils.uniqueTypeName();
        final String name2 = ProtoUtils.uniqueTypeName();
        assertThat(name1)
                .isNotEqualTo(name2);
        assertThat(ProtoUtils.isUniqueTypeName(name1)).isTrue();
        assertThat(ProtoUtils.isUniqueTypeName(name2)).isTrue();
        assertThatCode(() -> ProtoUtils.checkValidProtoBufCompliantName(name1))
                .doesNotThrowAnyException();
        assertThatCode(() -> ProtoUtils.checkValidProtoBufCompliantName(name2))
                .doesNotThrowAnyException();
    }

    /**
     * Tests that only names generated by {@link ProtoUtils#uniqueTypeName()} are recognized as generated type names.
     */
    @Test
    void isUniqueTypeName() {
        assertThat(ProtoUtils.isUniqueTypeName(ProtoUtils.uniqueTypeName())).isTrue();
        assertThat(ProtoUtils.isUniqueTypeName(ProtoUtils.uniqueName("blah"))).isFalse();
        assertThat(ProtoUtils.isUniqueTypeName("T")).isFalse();
        assertThat(ProtoUtils.isUniqueTypeName("")).isFalse();
    }

    @Test
    void uniqueOtherName() {
        final String name1 = ProtoUtils.uniqueName("blah");
        final String name2 = ProtoUtils.uniqueName("blah");
        assertThat(name1)
                .startsWith("blah")
                .isNotEqualTo(name2);
        assertThat(name2)
                .startsWith("blah");
        assertThatCode(() -> ProtoUtils.checkValidProtoBufCompliantName(name1))
                .doesNotThrowAnyException();
        assertThatCode(() -> ProtoUtils.checkValidProtoBufCompliantName(name2))
                .doesNotThrowAnyException();
    }

    @Nonnull
    private static Stream<Arguments> basicFileDescriptors() {
        return Stream.of(
                Arguments.argumentSet(TestRecords4Proto.getDescriptor().getName(), TestRecords4Proto.getDescriptor()),
                Arguments.argumentSet(TestRecords4WrapperProto.getDescriptor().getName(), TestRecords4WrapperProto.getDescriptor()),
                Arguments.argumentSet(TestRecords5Proto.getDescriptor().getName(), TestRecords5Proto.getDescriptor()),
                Arguments.argumentSet(TestRecordsTupleFieldsProto.getDescriptor().getName(), TestRecordsTupleFieldsProto.getDescriptor()),
                Arguments.argumentSet(TestRecordsWithUnionProto.getDescriptor().getName(), TestRecordsWithUnionProto.getDescriptor()));
    }

    @ParameterizedTest
    @MethodSource("basicFileDescriptors")
    void getTypeDescriptorsInFileForBasicFiles(@Nonnull final Descriptors.FileDescriptor fileDescriptor) {
        final var expectedDescriptors = ImmutableSet.copyOf(fileDescriptor.getMessageTypes());

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(RecordMetaDataOptionsProto.getDescriptor(), TupleFieldsProto.getDescriptor()),
                ImmutableSet.of());

        assertThat(actualDescriptors).containsExactlyInAnyOrderElementsOf(expectedDescriptors);
    }

    @Test
    void getTypeDescriptorInFileExcludesDependenciesCorrectly() {
        final var fileDescriptor = TestRecordsRepeatedImportProto.getDescriptor();
        final var expectedDescriptors = ImmutableSet.of(
                TestRecordsImportProto.RecordTypeUnion.getDescriptor(),
                TestRecordsRepeatedImportProto.RecordTypeUnion.getDescriptor());

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(
                        RecordMetaDataOptionsProto.getDescriptor(),
                        TestRecords1Proto.getDescriptor(),
                        TestRecords2Proto.getDescriptor()),
                ImmutableSet.of());

        assertThat(actualDescriptors).isEqualTo(expectedDescriptors);
    }

    @Test
    void getTypeDescriptorInFileExcludesDescriptorsCorrectly() {
        final var fileDescriptor = TestRecordsDoubleNestedProto.getDescriptor();
        final var expectedDescriptors = ImmutableSet.of(
                TestRecordsDoubleNestedProto.OuterRecord.getDescriptor(),
                TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(),
                TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor());

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(RecordMetaDataOptionsProto.getDescriptor()),
                ImmutableSet.of(
                        TestRecordsDoubleNestedProto.OtherRecord.getDescriptor(),
                        TestRecordsDoubleNestedProto.RecordTypeUnion.getDescriptor()));

        assertThat(actualDescriptors).isEqualTo(expectedDescriptors);
    }

    @Test
    void getTypeDescriptorsInFileWithNestedMessagesAndDifferentTypes() {
        final var fileDescriptor = TypeTestProto.getDescriptor();
        final var expectedDescriptors = ImmutableSet.<Descriptors.GenericDescriptor>builder()
                .addAll(TypeTestProto.getDescriptor().getMessageTypes())
                .addAll(TypeTestProto.Type1.getDescriptor().getNestedTypes())
                .addAll(TypeTestProto.PrimitiveFields.getDescriptor().getNestedTypes())
                .addAll(TypeTestProto.Type2.getDescriptor().getNestedTypes())
                .addAll(TypeTestProto.Type2.getDescriptor().getEnumTypes())
                .build();

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(RecordMetaDataOptionsProto.getDescriptor()),
                ImmutableSet.of());

        assertThat(actualDescriptors).containsAll(expectedDescriptors);
    }

    @Test
    void getTypeDescriptorsInFileWithDependencies() {
        final var fileDescriptor = TestRecordsNameClashProto.getDescriptor();
        final var expectedDescriptors = ImmutableSet.<Descriptors.GenericDescriptor>builder()
                .addAll(TestRecordsNameClashProto.getDescriptor().getMessageTypes())
                .addAll(TestRecords1Proto.getDescriptor().getMessageTypes())
                .build();

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(RecordMetaDataOptionsProto.getDescriptor()),
                ImmutableSet.of());

        assertThat(actualDescriptors)
                .containsExactlyInAnyOrderElementsOf(expectedDescriptors)
                .contains(
                        TestRecordsNameClashProto.MySimpleRecord.getDescriptor(),
                        TestRecords1Proto.MySimpleRecord.getDescriptor());
    }

    @Test
    void getTypeDescriptorInFileWithDuplicateMessage() throws IOException, Descriptors.DescriptorValidationException {
        // This needs to use a JSON metadata as we can't compile a protobuf metadata having message with duplicate
        // names declared in the same package.
        final var metadata = MetaDataProtoEditorUnitTest.loadMetaData("NestedWithDependency.json").build();
        assertThat(metadata.getDependenciesList()).hasSize(2);
        final var additionalDependency1Proto = metadata.getDependencies(0);
        final var additionalDependency2Proto = metadata.getDependencies(1);
        final var additionalDependency2 =  Descriptors.FileDescriptor.buildFrom(
                additionalDependency2Proto, new Descriptors.FileDescriptor[0]);
        final var additionalDependency1 = Descriptors.FileDescriptor.buildFrom(
                additionalDependency1Proto, new Descriptors.FileDescriptor[]{additionalDependency2});
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(metadata.getRecords(),
                new Descriptors.FileDescriptor[] {
                        RecordMetaDataOptionsProto.getDescriptor().getFile(), additionalDependency1, additionalDependency2});

        final var recordTypeUnionDescriptor = fileDescriptor.findMessageTypeByName("RecordTypeUnion");
        final var t1Descriptor = fileDescriptor.findMessageTypeByName("T1");
        final var t2Descriptor = fileDescriptor.findMessageTypeByName("T2");

        // There are two message types with the full name 'com.apple.foundationdb.test.additionaldependency.TypeFromDependency',
        // one in each dependency file. Both should exist within the returned set.
        final var typeFromDependency1Descriptor = additionalDependency1.findMessageTypeByName("TypeFromDependency");
        final var typeFromDependency2Descriptor = additionalDependency2.findMessageTypeByName("TypeFromDependency");
        final var colorEnumDescriptor = additionalDependency1.findEnumTypeByName("Color");
        assertThat(typeFromDependency1Descriptor).isNotNull();
        assertThat(typeFromDependency2Descriptor).isNotNull();
        assertThat(colorEnumDescriptor).isNotNull();

        final var actualDescriptors = ProtoUtils.getTypeDescriptorsInFile(fileDescriptor,
                ImmutableSet.of(RecordMetaDataOptionsProto.getDescriptor()),
                ImmutableSet.of());

        assertThat(actualDescriptors).containsExactlyInAnyOrder(
                recordTypeUnionDescriptor,
                typeFromDependency1Descriptor,
                typeFromDependency2Descriptor,
                colorEnumDescriptor,
                t1Descriptor,
                t2Descriptor);
    }
}
