/*
 * MetaDataEvolutionTest.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsIdenticalTypesProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.evolution.TestHeaderAsGroupProto;
import com.apple.foundationdb.record.evolution.TestMergedNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestNewRecordTypeProto;
import com.apple.foundationdb.record.evolution.TestSelfReferenceProto;
import com.apple.foundationdb.record.evolution.TestSelfReferenceUnspooledProto;
import com.apple.foundationdb.record.evolution.TestSplitNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestUnmergedNestedTypesProto;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.PrefixTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the {@link MetaDataEvolutionValidator} class. This mostly consists of trying to perform illegal updates
 * to the meta-data and then verifying that the update fails. Some of the tests may try doing things that
 * <i>seem</i> like they should be illegal but are actually fine.
 */
class MetaDataEvolutionValidatorTest {
    @Nonnull
    private final MetaDataEvolutionValidator validator = MetaDataEvolutionValidator.getDefaultInstance();

    static void assertInvalid(@Nonnull String errMsg, @Nonnull MetaDataEvolutionValidator validator,
                              @Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
        MetaDataException err = assertThrows(MetaDataException.class, () -> validator.validate(oldMetaData, newMetaData));
        assertThat(err.getMessage(), containsString(errMsg));
    }

    static void assertInvalid(@Nonnull String errMsg, @Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
        assertInvalid(errMsg, MetaDataEvolutionValidator.getDefaultInstance(), oldMetaData, newMetaData);
    }

    static void assertInvalid(@Nonnull String errMsg, @Nonnull MetaDataEvolutionValidator validator,
                              @Nonnull Descriptor oldUnionDescriptor, @Nonnull Descriptor newUnionDescriptor) {
        MetaDataException err = assertThrows(MetaDataException.class, () -> validator.validateUnion(oldUnionDescriptor, newUnionDescriptor));
        assertThat(err.getMessage(), containsString(errMsg));
    }

    static void assertInvalid(@Nonnull String errMsg, @Nonnull Descriptor oldUnionDescriptor, @Nonnull Descriptor newUnionDescriptor) {
        assertInvalid(errMsg, MetaDataEvolutionValidator.getDefaultInstance(), oldUnionDescriptor, newUnionDescriptor);
    }

    static void assertInvalid(@Nonnull String errMsg, @Nonnull FileDescriptor oldFileDescriptor, @Nonnull FileDescriptor newFileDescriptor) {
        assertInvalid(errMsg, oldFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME), newFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
    }

    private static class FieldRenameChecker {
        @Nonnull
        private final MetaDataEvolutionValidator baseValidator;
        @Nonnull
        private final MetaDataEvolutionValidator noRenamesValidator;
        @Nonnull
        private final MetaDataEvolutionValidator deprecatedOnlyValidator;
        @Nonnull
        private final MetaDataEvolutionValidator anyRenameValidator;
        @Nonnull
        private final MetaDataEvolutionValidator allRenamesValidator;

        public FieldRenameChecker(MetaDataEvolutionValidator baseValidator) {
            this.baseValidator = baseValidator;
            final MetaDataEvolutionValidator.Builder builder = baseValidator.asBuilder();

            noRenamesValidator = builder
                    .setAllowDeprecatedFieldRenames(false)
                    .setAllowDeprecatedFieldRenames(false)
                    .build();
            assertFalse(noRenamesValidator.allowsAnyFieldRenames());
            assertFalse(noRenamesValidator.allowsDeprecatedFieldRenames());
            assertFalse(noRenamesValidator.allowsFieldRenames());

            deprecatedOnlyValidator = builder
                    .setAllowDeprecatedFieldRenames(true)
                    .build();
            assertTrue(deprecatedOnlyValidator.allowsAnyFieldRenames());
            assertTrue(deprecatedOnlyValidator.allowsDeprecatedFieldRenames());
            assertFalse(deprecatedOnlyValidator.allowsFieldRenames());

            anyRenameValidator = builder
                    .setAllowDeprecatedFieldRenames(false)
                    .setAllowFieldRenames(true)
                    .build();
            assertTrue(anyRenameValidator.allowsAnyFieldRenames());
            assertFalse(anyRenameValidator.allowsDeprecatedFieldRenames());
            assertTrue(anyRenameValidator.allowsFieldRenames());

            // This should behave the same as anyRenameValidator, but it is included for completeness
            allRenamesValidator = builder
                    .setAllowDeprecatedFieldRenames(true)
                    .build();
            assertTrue(allRenamesValidator.allowsAnyFieldRenames());
            assertTrue(allRenamesValidator.allowsDeprecatedFieldRenames());
            assertTrue(allRenamesValidator.allowsFieldRenames());
        }

        @Nonnull
        public MetaDataEvolutionValidator getBaseValidator() {
            return baseValidator;
        }

        public void assertInvalidRenaming(@Nonnull String errMsg, boolean deprecatedOnly, @Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
            assertInvalid("field renamed", noRenamesValidator, oldMetaData, newMetaData);
            assertInvalid(deprecatedOnly ? errMsg : "field renamed", deprecatedOnlyValidator, oldMetaData, newMetaData);
            assertInvalid(errMsg, anyRenameValidator, oldMetaData, newMetaData);
            assertInvalid(errMsg, allRenamesValidator, oldMetaData, newMetaData);
        }

        public void assertValidRenaming(boolean deprecatedOnly, @Nonnull RecordMetaData oldMetaData, @Nonnull RecordMetaData newMetaData) {
            assertInvalid("field renamed", noRenamesValidator, oldMetaData, newMetaData);
            if (deprecatedOnly) {
                deprecatedOnlyValidator.validate(oldMetaData, newMetaData);
            } else {
                assertInvalid("field renamed", deprecatedOnlyValidator, oldMetaData, newMetaData);
            }
            anyRenameValidator.validate(oldMetaData, newMetaData);
            allRenamesValidator.validate(oldMetaData, newMetaData);
        }

        public void assertValidRenaming(boolean deprecatedOnly, @Nonnull FileDescriptor oldFileDescriptor, @Nonnull FileDescriptor newFileDescriptor) {
            assertValidRenaming(deprecatedOnly, oldFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME), newFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        }

        public void assertValidRenaming(boolean deprecatedOnly, @Nonnull Descriptor oldUnionDescriptor, @Nonnull Descriptor newUnionDescriptor) {
            assertInvalid("field renamed", noRenamesValidator, oldUnionDescriptor, newUnionDescriptor);
            if (deprecatedOnly) {
                deprecatedOnlyValidator.validateUnion(oldUnionDescriptor, newUnionDescriptor);
            } else {
                assertInvalid("field renamed", deprecatedOnlyValidator, oldUnionDescriptor, newUnionDescriptor);
            }
            anyRenameValidator.validateUnion(oldUnionDescriptor, newUnionDescriptor);
            allRenamesValidator.validateUnion(oldUnionDescriptor, newUnionDescriptor);
        }
    }

    private final FieldRenameChecker fieldRenameChecker = new FieldRenameChecker(validator);

    @Test
    void doNotChangeVersion() {
        // Check if a naive removal of the index without updating the version is checked
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        assertInvalid("new meta-data does not have newer version", metaData1, metaData1);
        RecordMetaDataBuilder metaData2Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData2Builder.removeIndex("MySimpleRecord$str_value_indexed");
        assertFalse(validator.allowsNoVersionChange());
        validator.validate(metaData1, metaData2Builder.getRecordMetaData());
        metaData2Builder.setVersion(metaData1.getVersion());
        assertInvalid("new meta-data does not have newer version", metaData1, metaData2Builder.build(false));

        // If the validator allows not changing the version, it should make sure all of the changes are compatible
        MetaDataEvolutionValidator validatorAcceptingSameVersion = MetaDataEvolutionValidator.newBuilder()
                .setAllowNoVersionChange(true)
                .build();
        assertTrue(validatorAcceptingSameVersion.allowsNoVersionChange());
        validatorAcceptingSameVersion.validate(metaData1, metaData1);

        // Confirm with the laxer validator that the removed index is noticed
        metaData2Builder.setVersion(metaData1.getVersion() + 1);
        RecordMetaDataBuilder metaData3Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData3Builder.setVersion(metaData1.getVersion() + 1);
        RecordMetaData metaData3 = metaData3Builder.getRecordMetaData();
        RecordMetaData metaData4 = metaData2Builder.getRecordMetaData();
        assertInvalid("new meta-data does not have newer version", metaData3, metaData4);
        assertInvalid("new former index has removed version that is not newer than the old meta-data version",
                validatorAcceptingSameVersion, metaData3, metaData4);
    }

    // Schema evolution tests

    @Nonnull
    static RecordMetaData updateMetaData(@Nonnull RecordMetaData metaData, @Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> updater) {
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaData.toProto().toBuilder();
        metaDataProtoBuilder.setVersion(metaData.getVersion() + 1);
        updater.accept(metaDataProtoBuilder);
        return RecordMetaData.build(metaDataProtoBuilder.build());
    }

    @Nonnull
    static RecordMetaData replaceRecordsDescriptor(@Nonnull RecordMetaData metaData, @Nonnull FileDescriptor newDescriptor,
                                                    @Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> metaDataMutation) {
        return updateMetaData(metaData, protoBuilder -> {
            protoBuilder.setRecords(newDescriptor.toProto());
            metaDataMutation.accept(protoBuilder);
        });
    }

    @Nonnull
    static RecordMetaData replaceRecordsDescriptor(@Nonnull RecordMetaData metaData, @Nonnull FileDescriptor newDescriptor) {
        return replaceRecordsDescriptor(metaData, newDescriptor, ignore -> { });
    }

    @Nonnull
    static FileDescriptor mutateFile(@Nonnull FileDescriptor originalFile, @Nonnull Consumer<DescriptorProtos.FileDescriptorProto.Builder> fileMutation) {
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = originalFile.toProto().toBuilder();
        fileMutation.accept(fileBuilder);
        try {
            return FileDescriptor.buildFrom(fileBuilder.build(), new FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RecordCoreException("unable to build file descriptor", e);
        }
    }

    @Nonnull
    static FileDescriptor mutateFile(@Nonnull Consumer<DescriptorProtos.FileDescriptorProto.Builder> fileMutation) {
        return mutateFile(TestRecords1Proto.getDescriptor(), fileMutation);
    }

    @Nonnull
    static FileDescriptor mutateMessageType(@Nonnull String messageName, @Nonnull FileDescriptor originalFile, @Nonnull Consumer<DescriptorProtos.DescriptorProto.Builder> typeMutation) {
        return mutateFile(originalFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(messageName)) {
                        typeMutation.accept(message);
                    }
                })
        );
    }

    @Nonnull
    static FileDescriptor mutateMessageType(@Nonnull String messageName, @Nonnull Consumer<DescriptorProtos.DescriptorProto.Builder> typeMutation) {
        return mutateMessageType(messageName, TestRecords1Proto.getDescriptor(), typeMutation);
    }

    @Nonnull
    static FileDescriptor mutateField(@Nonnull String messageName, @Nonnull String fieldName, @Nonnull FileDescriptor originalFile,
                                      @Nonnull Consumer<DescriptorProtos.FieldDescriptorProto.Builder> fieldMutation) {
        return mutateMessageType(messageName, originalFile, message ->
                message.getFieldBuilderList().forEach(field -> {
                    if (field.getName().equals(fieldName)) {
                        fieldMutation.accept(field);
                    }
                })
        );
    }

    @Nonnull
    static FileDescriptor mutateField(@Nonnull String messageName, @Nonnull String fieldName, @Nonnull Consumer<DescriptorProtos.FieldDescriptorProto.Builder> fieldMutation) {
        return mutateField(messageName, fieldName, TestRecords1Proto.getDescriptor(), fieldMutation);
    }

    @Nonnull
    static DescriptorProtos.FieldDescriptorProto.Builder addField(@Nonnull DescriptorProtos.DescriptorProto.Builder message) {
        int maxFieldNumber = message.getFieldBuilderList().stream()
                .mapToInt(DescriptorProtos.FieldDescriptorProto.Builder::getNumber)
                .max()
                .orElse(0);
        return message.addFieldBuilder()
                .setNumber(maxFieldNumber + 1);
    }

    static void deprecateField(@Nonnull DescriptorProtos.FieldDescriptorProto.Builder field) {
        field.getOptionsBuilder().setDeprecated(true);
    }

    @Test
    void changeSplitLongRecords() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        assertThat(metaData1.isSplitLongRecords(), is(false));
        RecordMetaData metaData2 = RecordMetaData.build(metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1)
                .setSplitLongRecords(true)
                .build()
        );
        assertFalse(validator.allowsUnsplitToSplit());
        assertInvalid("new meta-data splits long records", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowUnsplitToSplit(true)
                .build();
        assertTrue(laxerValidator.allowsUnsplitToSplit());
        laxerValidator.validate(metaData1, metaData2);

        RecordMetaData metaData3 = RecordMetaData.build(metaData2.toProto().toBuilder()
                .setVersion(metaData2.getVersion() + 1)
                .setSplitLongRecords(false)
                .build()
        );
        assertInvalid("new meta-data no longer splits long records", metaData2, metaData3);
        assertInvalid("new meta-data no longer splits long records", laxerValidator, metaData2, metaData3);
    }

    @Test
    void changeStoreRecordVersions() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = RecordMetaData.build(metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1)
                .setStoreRecordVersions(!metaData1.isStoreRecordVersions())
                .build()
        );
        assertNotEquals(metaData1.isStoreRecordVersions(), metaData2.isStoreRecordVersions());
        validator.validate(metaData1, metaData2);

        RecordMetaData metaData3 = RecordMetaData.build(metaData2.toProto().toBuilder()
                .setVersion(metaData2.getVersion() + 1)
                .setStoreRecordVersions(!metaData2.isStoreRecordVersions())
                .build()
        );
        assertNotEquals(metaData2.isStoreRecordVersions(), metaData3.isStoreRecordVersions());
        validator.validate(metaData2, metaData3);
    }

    // Protobuf evolution tests

    @Test
    void swapUnionFields() {
        FileDescriptor updatedDescriptor = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, message ->
                message.getFieldBuilderList().forEach(field -> {
                    if (field.getNumber() == 1) {
                        field.setNumber(2);
                    } else {
                        field.setNumber(1);
                    }
                })
        );
        // The two record types do not have the same form, so swapping them should fail.
        // However, the exact way they fail isn't super important.
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedDescriptor);
        assertInvalid("", metaData1, metaData2);
    }

    @Test
    void swapUnionFieldsWithIdenticalTypes() {
        // Swap the positions for RecordOne and RecordTwo in the union descriptor. As these have identical definitions,
        // they could actually be swapped. Though perhaps they shouldn't be, and disallowing type renames will address
        // this kind of tom foolery
        FileDescriptor updatedFileDescriptor = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, TestRecordsIdenticalTypesProto.getDescriptor(), message ->
                message.getFieldBuilderList().forEach(field -> {
                    if (field.getNumber() == 1) {
                        field.setNumber(2);
                    } else {
                        field.setNumber(1);
                    }
                })
        );
        validator.validateUnion(TestRecordsIdenticalTypesProto.RecordTypeUnion.getDescriptor(), updatedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        final MetaDataEvolutionValidator stricterValidator = MetaDataEvolutionValidator.newBuilder()
                .setDisallowTypeRenames(true)
                .build();
        assertTrue(stricterValidator.disallowsTypeRenames());
        stricterValidator.validateUnion(TestRecordsIdenticalTypesProto.RecordTypeUnion.getDescriptor(), updatedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));

        final RecordMetaData metaData1 = RecordMetaData.build(TestRecordsIdenticalTypesProto.getDescriptor());

        // Swap the types. The indexes are referencing the old index names, which means they are now pointing to data
        // of the incorrect type. This is what results in the error message. Note the more straightforward error message
        // from the stricter validator
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFileDescriptor);
        assertInvalid("new index removes record type", metaData1, metaData2);
        assertInvalid("record type name changed", stricterValidator, metaData1, metaData2);

        // Update the names in the index definitions. The default validator now passes, though the stricter
        // validator fails
        final RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFileDescriptor, metaDataBuilder -> {
            metaDataBuilder.getIndexesBuilderList().forEach(index -> {
                if (index.getRecordTypeList().equals(List.of("RecordOne"))) {
                    index.clearRecordType();
                    index.addRecordType("RecordTwo");
                } else if (index.getRecordTypeList().equals(List.of("RecordTwo"))) {
                    index.clearRecordType();
                    index.addRecordType("RecordOne");
                }
            });
        });
        validator.validate(metaData1, metaData3);
        assertInvalid("record type name changed", stricterValidator, metaData1, metaData3);
    }

    @Test
    void typeChangeCreatesAmbiguousCorrespondence() {
        final FileDescriptor fileWithAdditionalUnionField = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, TestRecordsIdenticalTypesProto.getDescriptor(), message ->
                // Add a second field in the union descriptor pointing to RecordOne. This is fine
                addField(message)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("RecordOne")
                        .setName("other_union_field")
        );
        final RecordMetaData metaData1 = RecordMetaData.build(TestRecordsIdenticalTypesProto.getDescriptor());
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, fileWithAdditionalUnionField);
        validator.validate(metaData1, metaData2);

        // Change the type of the new union field so it now points to RecordTwo
        final FileDescriptor fileWithModifiedNewUnionField = mutateField(RecordMetaDataBuilder.DEFAULT_UNION_NAME, "other_union_field", fileWithAdditionalUnionField,
                field -> field.setTypeName("RecordTwo"));
        final RecordMetaData metaData3 = replaceRecordsDescriptor(metaData2, fileWithModifiedNewUnionField);
        validator.validate(metaData1, metaData3); // it actually would be fine to go straight from 1 to 3
        // Going from 2 to 3 is a problem. That's because when the field numbers are consulted between union
        // descriptor fields, we first establish that the old RecordOne corresponds to the new RecordOne (as
        // field 1 is a RecordOne in both). Likewise, looking at field 2 establishes that RecordTwo corresponds
        // to RecordTwo. But then the third field causes trouble: version 2 is of type RecordOne and version 3
        // is of type RecordTwo. So the old RecordOne must be both a new RecordOne and a new RecordTwo.
        assertInvalid("record type corresponds to multiple types in new meta-data", metaData2.getUnionDescriptor(), metaData3.getUnionDescriptor());
        assertInvalid("record type corresponds to multiple types in new meta-data", metaData2, metaData3);
    }

    /**
     * Validate that changes to the union descriptor that equate two previously differentiated
     * types--even if they have the same form--are disallowed. In theory, this could be allowed if
     * the machinery to handle indexes were sophisticated enough. It would require that every index
     * either be dropped or that all indexes were previously defined on both records and are now
     * defined on the combined record. It also requires that any indexes on record type be dropped.
     */
    @Test
    void mergeTypes() {
        // Build a descriptor with two copies of MyOtherRecord (essentially).
        FileDescriptor updatedDescriptor = mutateFile(fileBuilder -> {
            DescriptorProtos.DescriptorProto newMessageType = fileBuilder.getMessageTypeList().stream()
                    .filter(message -> message.getName().equals("MyOtherRecord"))
                    .findFirst()
                    .get()
                    .toBuilder()
                    .setName("MyOtherOtherRecord")
                    .build();
            fileBuilder.addMessageType(newMessageType);
            fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    addField(message)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord");
                }
            });
        });

        assertThat(updatedDescriptor.getMessageTypes().stream().map(Descriptor::getName).collect(Collectors.toSet()),
                containsInAnyOrder("MySimpleRecord", "MyOtherRecord", "MyOtherOtherRecord", RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        RecordMetaData metaData1 = RecordMetaData.build(updatedDescriptor);
        assertThat(metaData1.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord", "MyOtherOtherRecord"));

        FileDescriptor secondDescriptor = mutateFile(updatedDescriptor, fileBuilder -> {
            DescriptorProtos.DescriptorProto.Builder myOtherOtherDescriptor = fileBuilder.getMessageTypeBuilderList().stream()
                    .filter(message -> message.getName().equals("MyOtherOtherRecord"))
                    .findFirst()
                    .get();
            int index = fileBuilder.getMessageTypeBuilderList().indexOf(myOtherOtherDescriptor);
            fileBuilder.removeMessageType(index);
            fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    message.getFieldBuilderList().forEach(field -> {
                        if (field.getTypeName().equals("MyOtherOtherRecord")) {
                            field.setTypeName("MyOtherRecord");
                        }
                    });
                }
            });
        });

        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, secondDescriptor, metaDataProtoBuilder -> {
            RecordMetaDataProto.RecordType.Builder myOtherOtherRecord = metaDataProtoBuilder.getRecordTypesBuilderList().stream()
                    .filter(recordType -> recordType.getName().equals("MyOtherOtherRecord"))
                    .findFirst()
                    .get();
            int index = metaDataProtoBuilder.getRecordTypesBuilderList().indexOf(myOtherOtherRecord);
            metaDataProtoBuilder.removeRecordTypes(index);
        });
        assertThat(metaData2.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord"));
        assertInvalid("record type corresponds to multiple types in old meta-data", metaData1, metaData2);
    }

    /**
     * Validate that changes to the union descriptor that differentiate two previously equivalent
     * types--even if they have the same form--are disallowed. In theory, this could be allowed if
     * the machinery to handle indexes were sophisticated enough. It would require that every index
     * either be dropped or that all indexes that were previously defined on the combined record type
     * are now defined on both types. It also requires that any indexes on record type be dropped.
     */
    @Test
    void splitTypes() {
        // Add a second "MyOtherRecord" to the union descriptor
        FileDescriptor updatedDescriptor = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        addField(message)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("MyOtherRecord")
                                .setName("_MyOtherOtherRecord");
                    }
                })
        );
        RecordMetaData metaData1 = RecordMetaData.build(updatedDescriptor);
        assertThat(metaData1.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord"));

        FileDescriptor secondDescriptor = mutateFile(updatedDescriptor, fileBuilder -> {
            DescriptorProtos.DescriptorProto newMessageType = fileBuilder.getMessageTypeList().stream()
                    .filter(message -> message.getName().equals("MyOtherRecord"))
                    .findFirst()
                    .get()
                    .toBuilder()
                    .setName("MyOtherOtherRecord")
                    .build();
            fileBuilder.addMessageType(newMessageType);
            fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    message.getFieldBuilderList().forEach(field -> {
                        if (field.getName().equals("_MyOtherOtherRecord")) {
                            field.setTypeName("MyOtherOtherRecord");
                        }
                    });
                }
            });
        });
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, secondDescriptor, metaDataProtoBuilder ->
                metaDataProtoBuilder.addRecordTypes(RecordMetaDataProto.RecordType.newBuilder()
                        .setName("MyOtherOtherRecord")
                        .setPrimaryKey(Key.Expressions.field("rec_no").toKeyExpression())
                )
        );
        assertThat(metaData2.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord", "MyOtherOtherRecord"));
        assertInvalid("record type corresponds to multiple types in new meta-data", metaData1, metaData2);
    }

    @Test
    void changeRecordTypeName() {
        assertFalse(validator.disallowsTypeRenames());
        final MetaDataEvolutionValidator renameDisallowingValidator = MetaDataEvolutionValidator.newBuilder()
                .setDisallowTypeRenames(true)
                .build();
        assertTrue(renameDisallowingValidator.disallowsTypeRenames());

        // Update the record type name, but don't update any references in indexes
        FileDescriptor updatedFile = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                    if (messageType.getName().equals("MyOtherRecord")) {
                        messageType.setName("MyOtherOtherRecord");
                    } else if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        messageType.getFieldBuilderList().forEach(field -> {
                            if (field.getName().equals("_MyOtherRecord")) {
                                field.setName("_MyOtherOtherRecord");
                                field.setTypeName("MyOtherOtherRecord");
                            }
                        });
                    }
                })
        );
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MyOtherRecord", "num_value_3_indexed");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        MetaDataException e = assertThrows(MetaDataException.class, () -> replaceRecordsDescriptor(metaData1, updatedFile));
        assertThat(e.getMessage(), containsString("Unknown record type MyOtherRecord"));
        validator.validateUnion(metaData1.getUnionDescriptor(), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));

        // Changes the record type definition but not the indexes
        e = assertThrows(MetaDataException.class, () -> replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder ->
                protoBuilder.getRecordTypesBuilderList().forEach(recordType -> {
                    if (recordType.getName().equals("MyOtherRecord")) {
                        recordType.setName("MyOtherOtherRecord");
                    }
                })
        ));
        assertThat(e.getMessage(), containsString("Unknown record type MyOtherRecord"));

        // This should be allowed because it replaces all index definitions with the new record type as well
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder -> {
            protoBuilder.getRecordTypesBuilderList().forEach(recordType -> {
                if (recordType.getName().equals("MyOtherRecord")) {
                    recordType.setName("MyOtherOtherRecord");
                }
            });
            protoBuilder.getIndexesBuilderList().forEach(index -> {
                List<String> recordTypes = new ArrayList<>(index.getRecordTypeList());
                recordTypes.replaceAll(recordType -> recordType.equals("MyOtherRecord") ? "MyOtherOtherRecord" : recordType);
                index.clearRecordType();
                index.addAllRecordType(recordTypes);
            });
        });
        assertEquals(Collections.singletonList(metaData3.getRecordType("MyOtherOtherRecord")),
                metaData3.recordTypesForIndex(metaData3.getIndex("MyOtherRecord$num_value_3_indexed")));
        validator.validate(metaData1, metaData3);
        assertInvalid("record type name changed", renameDisallowingValidator, metaData1, metaData3);

        // Validate that calling update records with the new file descriptor produces a valid evolution
        RecordMetaDataBuilder metaDataBuilder4 = RecordMetaData.newBuilder().setRecords(metaData1.toProto());
        metaDataBuilder4.updateRecords(updatedFile);
        RecordMetaData metaData4 = metaDataBuilder4.getRecordMetaData();
        assertEquals(Collections.singletonList(metaData4.getRecordType("MyOtherOtherRecord")),
                metaData4.recordTypesForIndex(metaData4.getIndex("MyOtherRecord$num_value_3_indexed")));
        validator.validate(metaData1, metaData4);
        assertInvalid("record type name changed", renameDisallowingValidator, metaData1, metaData4);
    }

    @Test
    void swapRecordTypes() {
        FileDescriptor updatedFile = mutateFile(fileBuilder -> {
            // Update the field of the union descriptor.
            fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    messageType.getFieldBuilderList().forEach(field -> {
                        if (field.getName().equals("_MyOtherRecord")) {
                            field.setTypeName("MySimpleRecord");
                        }
                        if (field.getName().equals("_MySimpleRecord")) {
                            field.setTypeName("MyOtherRecord");
                        }
                    });
                }
            });
        });
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MyOtherRecord", "num_value_3_indexed");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        // Swap is noticed as the two records are not of compatible forms
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("", metaData1, metaData2);
    }

    @Test
    void swapIsomorphicRecordTypesWithIndexes() {
        FileDescriptor updatedFile = mutateFile(fileBuilder -> {
            DescriptorProtos.DescriptorProto newMessageType = fileBuilder.getMessageTypeList().stream()
                    .filter(messageType -> messageType.getName().equals("MyOtherRecord"))
                    .findFirst()
                    .get()
                    .toBuilder()
                    .setName("MyOtherOtherRecord")
                    .build();
            fileBuilder.addMessageType(newMessageType);
            fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    addField(messageType)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord");
                }
            });
        });
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(updatedFile);
        metaDataBuilder.addIndex("MyOtherRecord", "num_value_3_indexed");
        metaDataBuilder.addIndex("MyOtherOtherRecord", "num_value_3_indexed");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord", "MyOtherOtherRecord"));
        assertEquals(Collections.singletonList(metaData1.getRecordType("MyOtherRecord")),
                metaData1.recordTypesForIndex(metaData1.getIndex("MyOtherRecord$num_value_3_indexed")));
        assertEquals(Collections.singletonList(metaData1.getRecordType("MyOtherOtherRecord")),
                metaData1.recordTypesForIndex(metaData1.getIndex("MyOtherOtherRecord$num_value_3_indexed")));

        // Swap the two record types in the union descriptor.
        FileDescriptor secondFile = mutateFile(updatedFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                    if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        messageType.getFieldBuilderList().forEach(field -> {
                            if (field.getName().equals("_MyOtherRecord")) {
                                field.setTypeName("MyOtherOtherRecord");
                            }
                            if (field.getName().equals("_MyOtherOtherRecord")) {
                                field.setTypeName("MyOtherRecord");
                            }
                        });
                    }
                })
        );
        // Doesn't update the record types for the index which effectively swaps the definitions
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, secondFile);
        assertThat(metaData2.getRecordTypes().keySet(), containsInAnyOrder("MySimpleRecord", "MyOtherRecord", "MyOtherOtherRecord"));
        assertEquals(Collections.singletonList(metaData2.getRecordType("MyOtherRecord")),
                metaData2.recordTypesForIndex(metaData2.getIndex("MyOtherRecord$num_value_3_indexed")));
        assertEquals(Collections.singletonList(metaData2.getRecordType("MyOtherOtherRecord")),
                metaData2.recordTypesForIndex(metaData2.getIndex("MyOtherOtherRecord$num_value_3_indexed")));
        assertInvalid("new index removes record type", metaData1, metaData2);

        // Replace the record types in the indexes with the new names
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData2, secondFile, metaDataProtoBuilder ->
                metaDataProtoBuilder.getIndexesBuilderList().forEach(index -> {
                    List<String> recordTypes = new ArrayList<>(index.getRecordTypeList());
                    recordTypes.replaceAll(recordType -> {
                        if (recordType.equals("MyOtherRecord")) {
                            return "MyOtherOtherRecord";
                        } else if (recordType.equals("MyOtherOtherRecord")) {
                            return "MyOtherRecord";
                        } else {
                            return recordType;
                        }
                    });
                    index.clearRecordType();
                    index.addAllRecordType(recordTypes);
                })
        );
        assertEquals(Collections.singletonList(metaData3.getRecordType("MyOtherOtherRecord")),
                metaData3.recordTypesForIndex(metaData3.getIndex("MyOtherRecord$num_value_3_indexed")));
        assertEquals(Collections.singletonList(metaData3.getRecordType("MyOtherRecord")),
                metaData3.recordTypesForIndex(metaData3.getIndex("MyOtherOtherRecord$num_value_3_indexed")));
        validator.validate(metaData1, metaData3);

        // Verify that using "update records" updates the index definitions
        RecordMetaDataBuilder metaDataBuilder4 = RecordMetaData.newBuilder().setRecords(metaData1.toProto());
        metaDataBuilder4.updateRecords(secondFile);
        RecordMetaData metaData4 = metaDataBuilder4.getRecordMetaData();
        assertEquals(Collections.singletonList(metaData4.getRecordType("MyOtherOtherRecord")),
                metaData4.recordTypesForIndex(metaData4.getIndex("MyOtherRecord$num_value_3_indexed")));
        assertEquals(Collections.singletonList(metaData4.getRecordType("MyOtherRecord")),
                metaData4.recordTypesForIndex(metaData4.getIndex("MyOtherOtherRecord$num_value_3_indexed")));
        validator.validate(metaData1, metaData4);
    }

    @Test
    void swapIsomorphicRecordTypesWithExplicitKeys() {
        FileDescriptor updatedFile = mutateFile(fileBuilder -> {
            DescriptorProtos.DescriptorProto newMessageType = fileBuilder.getMessageTypeList().stream()
                    .filter(messageType -> messageType.getName().equals("MyOtherRecord"))
                    .findFirst()
                    .get()
                    .toBuilder()
                    .setName("MyOtherOtherRecord")
                    .build();
            fileBuilder.addMessageType(newMessageType);
            fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    addField(messageType)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord");
                }
            });
        });
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(updatedFile);
        metaDataBuilder.getRecordType("MyOtherRecord").setRecordTypeKey("other");
        metaDataBuilder.getRecordType("MyOtherOtherRecord").setRecordTypeKey("other_other");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertEquals("other", metaData1.getRecordType("MyOtherRecord").getRecordTypeKey());
        assertEquals("other_other", metaData1.getRecordType("MyOtherOtherRecord").getRecordTypeKey());

        // Swap the definitions in the union descriptor
        FileDescriptor secondFile = mutateFile(updatedFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
                    if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        messageType.getFieldBuilderList().forEach(field -> {
                            if (field.getName().equals("_MyOtherRecord")) {
                                field.setTypeName("MyOtherOtherRecord");
                            } else if (field.getName().equals("_MyOtherOtherRecord")) {
                                field.setTypeName("MyOtherRecord");
                            }
                        });
                    }
                })
        );
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, secondFile);
        assertEquals("other", metaData2.getRecordType("MyOtherRecord").getRecordTypeKey());
        assertEquals("other_other", metaData2.getRecordType("MyOtherOtherRecord").getRecordTypeKey());
        assertInvalid("record type key changed", metaData1, metaData2);

        // Swap the definitions in the record type descriptor list
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, secondFile, metaDataProtoBuilder ->
                metaDataProtoBuilder.getRecordTypesBuilderList().forEach(recordType -> {
                    if (recordType.getName().equals("MyOtherRecord")) {
                        recordType.setName("MyOtherOtherRecord");
                    } else if (recordType.getName().equals("MyOtherOtherRecord")) {
                        recordType.setName("MyOtherRecord");
                    }
                })
        );
        assertEquals("other", metaData3.getRecordType("MyOtherOtherRecord").getRecordTypeKey());
        assertEquals("other_other", metaData3.getRecordType("MyOtherRecord").getRecordTypeKey());
        validator.validate(metaData1, metaData3);

        // Verify that using "update records" updates the record type keys
        RecordMetaDataBuilder metaDataBuilder4 = RecordMetaData.newBuilder().setRecords(metaData1.toProto());
        metaDataBuilder4.updateRecords(secondFile);
        RecordMetaData metaData4 = metaDataBuilder4.getRecordMetaData();
        assertEquals("other", metaData4.getRecordType("MyOtherOtherRecord").getRecordTypeKey());
        assertEquals("other_other", metaData4.getRecordType("MyOtherRecord").getRecordTypeKey());
        validator.validate(metaData1, metaData4);
    }

    @Test
    void dropField() {
        FileDescriptor updatedFile = mutateMessageType("MySimpleRecord", message -> {
            int fieldNumValue2Index = 0;
            while (!message.getField(fieldNumValue2Index).getName().equals("num_value_2")) {
                fieldNumValue2Index++;
            }
            message.removeField(fieldNumValue2Index);
        });
        assertInvalid("field removed from message descriptor", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field removed from message descriptor", metaData1, metaData2);
    }

    @Test
    void renameField() {
        FileDescriptor updatedFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setName("num_value_too"));
        fieldRenameChecker.assertValidRenaming(false, TestRecords1Proto.getDescriptor(), updatedFile);

        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        fieldRenameChecker.assertValidRenaming(false, metaData1, metaData2);
    }

    @Test
    void renameDeprecatedField() {
        FileDescriptor deprecatedFile = mutateField("MySimpleRecord", "num_value_2",
                MetaDataEvolutionValidatorTest::deprecateField);
        FileDescriptor renamedFile = mutateField("MySimpleRecord", "num_value_2", deprecatedFile,
                field -> field.setName("num_value_too"));
        fieldRenameChecker.assertValidRenaming(true, deprecatedFile, renamedFile);

        RecordMetaData metaData1 = RecordMetaData.build(deprecatedFile);
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, renamedFile);
        fieldRenameChecker.assertValidRenaming(true, metaData1, metaData2);
    }

    @Test
    void renameFieldWhenMarkingDeprecated() {
        FileDescriptor updatedFile = mutateField("MySimpleRecord", "num_value_2", field -> {
            deprecateField(field);
            field.setName("num_value_too");
        });
        fieldRenameChecker.assertValidRenaming(true, TestRecords1Proto.getDescriptor(), updatedFile);

        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        fieldRenameChecker.assertValidRenaming(true, metaData1, metaData2);
    }

    @Test
    void renameFieldWhenUndeprecating() {
        // Change the field name at the same time we mark it as not deprecated
        FileDescriptor deprecatedFile = mutateField("MySimpleRecord", "num_value_2",
                MetaDataEvolutionValidatorTest::deprecateField);
        FileDescriptor renamedFile = mutateField("MySimpleRecord", "num_value_2", deprecatedFile,
                field -> field.clearOptions().setName("num_value_too"));
        RecordMetaData metaData1 = RecordMetaData.build(deprecatedFile);
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, renamedFile);

        // Under default settings, this is still not allowed (because we ban undeprecating fields)
        assertFalse(fieldRenameChecker.getBaseValidator().allowsUndeprecatingFields());
        fieldRenameChecker.assertInvalidRenaming("field is no longer deprecated", true, metaData1, metaData2);

        // If un-deprecating fields is okay, then we get a valid renaming
        final FieldRenameChecker laxerFieldRenameChecker = new FieldRenameChecker(fieldRenameChecker.getBaseValidator().asBuilder()
                .setAllowUndeprecatingFields(true)
                .build());
        assertTrue(laxerFieldRenameChecker.getBaseValidator().allowsUndeprecatingFields());
        laxerFieldRenameChecker.assertValidRenaming(true, metaData1, metaData2);
    }

    @Test
    void renameMixOfDeprecatedAndUndeprecatedFields() {
        FileDescriptor deprecatedFile = mutateField("RestaurantTag", "weight", TestRecords4Proto.getDescriptor(),
                MetaDataEvolutionValidatorTest::deprecateField);
        deprecatedFile = mutateField("ReviewerStats", "hometown", deprecatedFile,
                MetaDataEvolutionValidatorTest::deprecateField);
        assertTrue(deprecatedFile.findMessageTypeByName("RestaurantTag").findFieldByName("weight").getOptions().getDeprecated());
        assertTrue(deprecatedFile.findMessageTypeByName("ReviewerStats").findFieldByName("hometown").getOptions().getDeprecated());
        final RecordMetaData metaData1 = RecordMetaData.build(deprecatedFile);

        // Rename one deprecated field
        FileDescriptor renamedFile = mutateField("ReviewerStats", "hometown", deprecatedFile,
                field -> field.setName("origin"));
        assertTrue(renamedFile.findMessageTypeByName("ReviewerStats").findFieldByName("origin").getOptions().getDeprecated());
        fieldRenameChecker.assertValidRenaming(true, deprecatedFile.findMessageTypeByName("UnionDescriptor"), renamedFile.findMessageTypeByName("UnionDescriptor"));
        fieldRenameChecker.assertValidRenaming(true, metaData1, replaceRecordsDescriptor(metaData1, renamedFile));

        // Rename another deprecated field
        renamedFile = mutateField("RestaurantTag", "weight", renamedFile,
                field -> field.setName("weighting"));
        assertTrue(renamedFile.findMessageTypeByName("RestaurantTag").findFieldByName("weighting").getOptions().getDeprecated());
        fieldRenameChecker.assertValidRenaming(true, deprecatedFile.findMessageTypeByName("UnionDescriptor"), renamedFile.findMessageTypeByName("UnionDescriptor"));
        fieldRenameChecker.assertValidRenaming(true, metaData1, replaceRecordsDescriptor(metaData1, renamedFile));

        // Rename a non deprecated field. Now, the fieldRenameChecker should only consider this valid if it allows
        // all field renames
        renamedFile = mutateField("RestaurantRecord", "reviews", renamedFile,
                field -> field.setName("review_list"));
        assertFalse(renamedFile.findMessageTypeByName("RestaurantRecord").findFieldByName("review_list").getOptions().getDeprecated());
        fieldRenameChecker.assertValidRenaming(false, deprecatedFile.findMessageTypeByName("UnionDescriptor"), renamedFile.findMessageTypeByName("UnionDescriptor"));
        fieldRenameChecker.assertValidRenaming(false, metaData1, replaceRecordsDescriptor(metaData1, renamedFile));
    }

    @Nonnull
    static Stream<Named<Boolean>> deprecatedArgs() {
        return ParameterizedTestUtils.booleans("deprecated");
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void renameFieldWithIndex(boolean deprecated) {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FileDescriptor updatedFile = mutateMessageType("MySimpleRecord", simpleRecordType -> {
            simpleRecordType.getFieldBuilderList().stream()
                    .filter(field -> field.getName().equals("str_value_indexed"))
                    .forEach(field -> {
                        if (deprecated) {
                            deprecateField(field);
                        }
                        field.setName("str_value_indexed_old");
                    });

            // Add a new field also called str_value_indexed. This is necessary as the validation logic invoked
            // when building the meta-data will fail if there's an index on a field that doesn't exist
            addField(simpleRecordType)
                    .setName("str_value_indexed")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);

        // This is rejected even if we allow field renames as the index expression has not been updated
        fieldRenameChecker.assertInvalidRenaming("index key expression does not match required", deprecated, metaData1, metaData2);

        // This updates both the field name and its indexes which means that this is actually okay.
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder ->
                protoBuilder.getIndexesBuilderList().forEach(index -> {
                    if (index.getName().equals("MySimpleRecord$str_value_indexed")) {
                        index.setRootExpression(Key.Expressions.field("str_value_indexed_old").toKeyExpression());
                    }
                })
        );
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData3);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void renameFieldInUniversalIndex(boolean deprecated) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(new Index("all$num_value_2", "num_value_2"));
        RecordMetaData metaData1 = metaDataBuilder.build();

        FileDescriptor updatedFile = mutateMessageType("MySimpleRecord", simpleRecordType -> {
            simpleRecordType.getFieldBuilderList().stream()
                    .filter(field -> field.getName().equals("num_value_2"))
                    .forEach(field -> {
                        if (deprecated) {
                            deprecateField(field);
                        }
                        field.setName("num_value_2__old");
                    });

            addField(simpleRecordType)
                    .setName("num_value_2")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);

        // Still not allowed as the multi-type index requires the new key expression num_value_2__old on one record
        // type but num_value_2 on another
        fieldRenameChecker.assertInvalidRenaming("field renames result in inconsistent index definition for multi-type index", deprecated, metaData1, metaData2);

        // Update the other types num_value_2 so now all types rename num_value_2 the same way
        updatedFile = mutateMessageType("MyOtherRecord", updatedFile, otherRecordType -> {
            otherRecordType.getFieldBuilderList().stream()
                    .filter(field -> field.getName().equals("num_value_2"))
                    .forEach(field -> {
                        if (deprecated) {
                            deprecateField(field);
                        }
                        field.setName("num_value_2__old");
                    });

            addField(otherRecordType)
                    .setName("num_value_2")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile);
        // Still not allowed as the index hasn't been updated
        fieldRenameChecker.assertInvalidRenaming("index key expression does not match required", deprecated, metaData1, metaData3);

        RecordMetaData metaData4 = mutateIndex(metaData3, "all$num_value_2",
                indexProto -> indexProto.setRootExpression(Key.Expressions.field("num_value_2__old").toKeyExpression()));
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData4);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void renameFieldInPrimaryKey(boolean deprecated) {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FileDescriptor updatedFile = mutateMessageType("MySimpleRecord", simpleRecordType -> {
            simpleRecordType.getFieldBuilderList().stream()
                    .filter(field -> field.getName().equals("rec_no"))
                    .forEach(field -> {
                        if (deprecated) {
                            deprecateField(field);
                        }
                        field.setName("old_rec_no");
                    });

            // Add a new field also called rec_no so that we pass meta-data validation
            addField(simpleRecordType)
                    .setName("rec_no")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);

        // This is rejected even if we allow field renames as the primary key has not been updated
        fieldRenameChecker.assertInvalidRenaming("record type primary key does not match required", deprecated, metaData1, metaData2);

        // Now update the primary key to match the new record name
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder ->
                protoBuilder.getRecordTypesBuilderList().forEach(recordType -> {
                    if (recordType.getName().equals("MySimpleRecord")) {
                        recordType.setPrimaryKey(Key.Expressions.field("old_rec_no").toKeyExpression());
                    }
                })
        );
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData3);
    }

    @Test
    void deprecateField() {
        FileDescriptor deprecatedFile = mutateField("MySimpleRecord", "str_value_indexed",
                MetaDataEvolutionValidatorTest::deprecateField);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        // The str_value_indexed field is used in indexes. We may want to ban those, which we should do in the
        // MetaDataValidator (not the evolution validator). If we do, this may fail, and we should change this
        // test to use a non-indexed field
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, deprecatedFile);
        // Deprecating fields is okay
        validator.validate(metaData1, metaData2);
    }

    @Test
    void undeprecateField() {
        FileDescriptor deprecatedFile = mutateField("MySimpleRecord", "num_value_3_indexed",
                MetaDataEvolutionValidatorTest::deprecateField);
        RecordMetaData metaData1 = RecordMetaData.build(deprecatedFile);
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestRecords1Proto.getDescriptor());
        assertFalse(validator.allowsUndeprecatingFields());
        assertInvalid("field is no longer deprecated", metaData1, metaData2);

        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowUndeprecatingFields(true)
                .build();
        assertTrue(laxerValidator.allowsUndeprecatingFields());
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    void fieldTypeChanged() {
        FileDescriptor updatedFile = mutateField("MySimpleRecord", "str_value_indexed",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES));
        assertInvalid("field type changed", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field type changed", metaData1, metaData2);

        // Allow int32 -> int64 but not int64 -> int32
        assertEquals(FieldDescriptor.Type.INT32, TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("num_value_2").getType());
        updatedFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64));
        assertEquals(FieldDescriptor.Type.INT64, updatedFile.findMessageTypeByName("MySimpleRecord").findFieldByName("num_value_2").getType());
        validator.validateUnion(TestRecords1Proto.RecordTypeUnion.getDescriptor(), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        assertInvalid("field type changed", updatedFile, TestRecords1Proto.getDescriptor());
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile);
        validator.validate(metaData1, metaData3);

        // Allow sint32 -> sint64 but not sint32 -> sint64
        FileDescriptor intermediateFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32));
        updatedFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64));
        validator.validateUnion(intermediateFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        assertInvalid("field type changed", updatedFile, intermediateFile);

        // Do not allow sfixed32 -> sfixed64
        intermediateFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32));
        updatedFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64));
        assertInvalid("field type changed", intermediateFile, updatedFile);
    }

    @Test
    void fieldChangedFromMessageToGroup() {
        // The message and group types here have the same form, but messages and groups are serialized differently
        assertInvalid("field type changed", TestRecordsWithHeaderProto.getDescriptor(), TestHeaderAsGroupProto.getDescriptor());
        assertInvalid("field type changed", TestHeaderAsGroupProto.getDescriptor(), TestRecordsWithHeaderProto.getDescriptor());

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestHeaderAsGroupProto.getDescriptor());
        assertInvalid("field type changed", metaData1, metaData2);
    }

    @Test
    void enumFieldChanged() {
        // Add an enum field
        FileDescriptor updatedFile = mutateFile(TestRecordsEnumProto.getDescriptor(), fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("MyShapeRecord")) {
                        message.getEnumTypeBuilderList().forEach(enumType -> {
                            if (enumType.getName().equals("Size")) {
                                enumType.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                        .setName("X_LARGE")
                                        .setNumber(TestRecordsEnumProto.MyShapeRecord.Size.getDescriptor().getValues().stream()
                                                           .mapToInt(Descriptors.EnumValueDescriptor::getNumber).max().getAsInt() + 1));
                            }
                        });
                    }
                })
        );
        validator.validateUnion(TestRecordsEnumProto.RecordTypeUnion.getDescriptor(), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        RecordMetaData metaData1 = RecordMetaData.build(TestRecordsEnumProto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        validator.validate(metaData1, metaData2);

        // Dropping a value is not allowed
        assertInvalid("enum removes value", updatedFile, TestRecordsEnumProto.getDescriptor());
        RecordMetaData metaData3 = RecordMetaData.build(updatedFile);
        RecordMetaData metaData4 = replaceRecordsDescriptor(metaData3, TestRecordsEnumProto.getDescriptor());
        assertInvalid("enum removes value", metaData3, metaData4);

        // Changing the value name is okay
        updatedFile = mutateFile(TestRecordsEnumProto.getDescriptor(), fileBuilder ->
                fileBuilder.getEnumTypeBuilderList().forEach(enumType -> {
                    if (enumType.getName().equals("Size")) {
                        enumType.getValueBuilder(0).setName("PETIT");
                    }
                })
        );
        validator.validateUnion(TestRecordsEnumProto.RecordTypeUnion.getDescriptor(), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        RecordMetaData metaData5 = replaceRecordsDescriptor(metaData1, updatedFile);
        validator.validate(metaData1, metaData5);
    }

    @Test
    void selfReferenceChanged() {
        // This is largely to test that messages which include themselves as nested types don't cause the validator to blow up
        final Descriptor selfReferenceUnion = TestSelfReferenceProto.RecordTypeUnion.getDescriptor();
        final Descriptor unspooledUnion = TestSelfReferenceUnspooledProto.RecordTypeUnion.getDescriptor();
        validator.validateUnion(selfReferenceUnion, unspooledUnion);

        // Try the other way. Note that one of the fields in the unspooled LinkedListRecord is deprecated, so we need
        // to allow undeprecation in order to catch the field removal error
        assertFalse(validator.allowsUndeprecatingFields());
        assertInvalid("field is no longer deprecated", unspooledUnion, selfReferenceUnion);
        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowUndeprecatingFields(true)
                .build();
        assertTrue(laxerValidator.allowsUndeprecatingFields());
        assertInvalid("field removed", laxerValidator, unspooledUnion, selfReferenceUnion);

        FileDescriptor updatedUnspooledFile = mutateMessageType("Node", TestSelfReferenceUnspooledProto.getDescriptor(),
                message -> message.removeField(0));
        assertNull(updatedUnspooledFile.findMessageTypeByName("Node").findFieldByName("rec_no"));
        assertInvalid("field removed", TestSelfReferenceUnspooledProto.getDescriptor(), updatedUnspooledFile);
    }

    @Test
    void nestedTypeChangesName() {
        FileDescriptor updatedFile = mutateFile(TestRecordsWithHeaderProto.getDescriptor(), fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("HeaderRecord")) {
                        message.setName("Header");
                    } else if (message.getName().equals("MyRecord")) {
                        message.getFieldBuilderList().forEach(field -> {
                            if (field.getName().equals("header")) {
                                field.setTypeName("." + fileBuilder.getPackage() + ".Header");
                            }
                        });
                    }
                })
        );
        assertThat(updatedFile.getMessageTypes().stream().map(Descriptor::getName).collect(Collectors.toList()), containsInAnyOrder("MyRecord", RecordMetaDataBuilder.DEFAULT_UNION_NAME, "Header"));
        validator.validateUnion(TestRecordsWithHeaderProto.RecordTypeUnion.getDescriptor(), updatedFile.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        validator.validate(metaData1, metaData2);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void nestedTypeChangesFieldName(boolean deprecated) {
        FileDescriptor updatedFile = mutateField("HeaderRecord", "num", TestRecordsWithHeaderProto.getDescriptor(), field -> {
            if (deprecated) {
                deprecateField(field);
            }
            field.setName("numb");
        });
        fieldRenameChecker.assertValidRenaming(deprecated, TestRecordsWithHeaderProto.getDescriptor(), updatedFile);

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData2);
    }

    @Test
    void nestedTypeChangesFieldType() {
        FileDescriptor updatedFile = mutateField("HeaderRecord", "num", TestRecordsWithHeaderProto.getDescriptor(),
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32));
        assertInvalid("field type changed", TestRecordsWithHeaderProto.getDescriptor(), updatedFile);
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field type changed", metaData1, metaData2);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void nestedTypesMerged(boolean deprecated) {
        validator.validateUnion(TestUnmergedNestedTypesProto.RecordTypeUnion.getDescriptor(), TestMergedNestedTypesProto.RecordTypeUnion.getDescriptor());

        FileDescriptor updatedMergedFile = mutateField("OneTrueNested", "b", TestMergedNestedTypesProto.getDescriptor(), field -> {
            if (deprecated) {
                deprecateField(field);
            }
            field.setName("c");
        });
        fieldRenameChecker.assertValidRenaming(deprecated, TestUnmergedNestedTypesProto.getDescriptor(), updatedMergedFile);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void nestedTypesMergedWithIndexesAndFieldRenames(boolean deprecated) {
        // Start with two fields in MyRecord, a and b, pointing to a NestedA and Nested B respectively
        // Then merge the types NestedA and NestedB together. In the merging, field 2 of NestedA is renamed
        // from a_prime to b, and field 2 of NestedB is renamed from b_prime to b. Validate that indexes
        // defined on those two fields need to match to pass validation
        FileDescriptor unmergedFile = mutateMessageType("NestedA", TestUnmergedNestedTypesProto.getDescriptor(),
                message -> addField(message)
                        .setName("a_prime")
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        unmergedFile = mutateField("NestedB", "b", unmergedFile,
                field -> field.setName("b_prime"));
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(unmergedFile);
        metaDataBuilder.addIndex("MyRecord", "MyRecord$a.b+b.b", Key.Expressions.concat(Key.Expressions.field("a").nest("a_prime"), Key.Expressions.field("b").nest("b_prime")));
        final RecordMetaData metaData1 = metaDataBuilder.build();

        FileDescriptor mergedFile = mutateMessageType("OneTrueNested", TestMergedNestedTypesProto.getDescriptor(), message -> {
            final DescriptorProtos.FieldDescriptorProto.Builder aPrime = addField(message)
                    .setName("a_prime")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            final DescriptorProtos.FieldDescriptorProto.Builder bPrime = addField(message)
                    .setName("b_prime")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            if (deprecated) {
                deprecateField(aPrime);
                deprecateField(bPrime);
                message.getFieldBuilderList().stream()
                        .filter(field -> field.getName().equals("a") || field.getName().equals("b"))
                        .forEach(MetaDataEvolutionValidatorTest::deprecateField);
            }
        });
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, mergedFile);

        // Even with field renames allowed, this should be rejected as the a.a_prime field has not been updated in the index
        fieldRenameChecker.assertInvalidRenaming("index key expression does not match required", deprecated, metaData1, metaData2);

        // Update the index so that it reflects the new field name for a.a_prime -> a.b
        final RecordMetaData metaData3 = mutateIndex(metaData2, "MyRecord$a.b+b.b", indexProto ->
                indexProto.setRootExpression(Key.Expressions.concat(Key.Expressions.field("a").nest("b"), Key.Expressions.field("b").nest("b")).toKeyExpression()));
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData3);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void nestedTypesSplit(boolean deprecated) {
        validator.validateUnion(TestMergedNestedTypesProto.RecordTypeUnion.getDescriptor(), TestSplitNestedTypesProto.RecordTypeUnion.getDescriptor());

        FileDescriptor fieldTypeChangedFile = mutateField("NestedB", "b", TestSplitNestedTypesProto.getDescriptor(),
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES));
        assertInvalid("field type changed", TestUnmergedNestedTypesProto.getDescriptor(), fieldTypeChangedFile);

        // Put different renames for different fields
        FileDescriptor updatedSplitFile = mutateField("NestedA", "b", TestSplitNestedTypesProto.getDescriptor(), field -> {
            if (deprecated) {
                deprecateField(field);
            }
            field.setName("b_1");
        });
        updatedSplitFile = mutateField("NestedB", "b", updatedSplitFile, field -> {
            if (deprecated) {
                deprecateField(field);
            }
            field.setName("b_2");
        });
        fieldRenameChecker.assertValidRenaming(deprecated, TestMergedNestedTypesProto.getDescriptor(), updatedSplitFile);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void nestedTypesSplitWithIndex(boolean deprecated) {
        // Start with two fields in MyRecord, a and b, both pointing to OneTrueNested with fields a and b
        // In the split file, a now points to a NestedA and b points to a NestedB
        // Rename the b field in NestedA to b_1 and the b field in NestedB to b_2 and validate that the indexes
        // need to match to pass validation
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestMergedNestedTypesProto.getDescriptor());
        metaDataBuilder.addIndex("MyRecord", "MyRecord$a.b+b.b", Key.Expressions.concat(Key.Expressions.field("a").nest("b"), Key.Expressions.field("b").nest("b")));
        final RecordMetaData metaData1 = metaDataBuilder.build();

        FileDescriptor splitFile = mutateMessageType("NestedA", TestSplitNestedTypesProto.getDescriptor(), message -> {
            message.getFieldBuilderList().forEach(field -> {
                if (field.getName().equals("b")) {
                    if (deprecated) {
                        deprecateField(field);
                    }
                    field.setName("b_1");
                }
            });
            addField(message)
                    .setName("b")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        splitFile = mutateMessageType("NestedB", splitFile, message -> {
            message.getFieldBuilderList().forEach(field -> {
                if (field.getName().equals("b")) {
                    if (deprecated) {
                        deprecateField(field);
                    }
                    field.setName("b_2");
                }
            });
            addField(message)
                    .setName("b")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        });
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, splitFile);

        // Even with field renames allowed, this should be rejected as the a.b and b.b fields have not been updated in the index
        fieldRenameChecker.assertInvalidRenaming("index key expression does not match required", deprecated, metaData1, metaData2);

        // Update the index so that it reflects the new field name for a.b -> a.b_1 and b.b -> b.b_2
        final RecordMetaData metaData3 = mutateIndex(metaData2, "MyRecord$a.b+b.b", indexProto ->
                indexProto.setRootExpression(Key.Expressions.concat(Key.Expressions.field("a").nest("b_1"), Key.Expressions.field("b").nest("b_2")).toKeyExpression()));
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData3);
    }

    @Test
    void fieldLabelChanged() {
        FileDescriptor oldFile = TestRecords1Proto.getDescriptor();
        List<DescriptorProtos.FieldDescriptorProto.Label> labels = Arrays.asList(
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED
        );
        for (int i = 0; i < labels.size(); i++) {
            final DescriptorProtos.FieldDescriptorProto.Label label = labels.get(i);
            final String errMsg;
            if (label == DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL) {
                errMsg = "field changed whether default values are stored if set explicitly";
            } else {
                final String labelText = label.name().substring(label.name().indexOf('_') + 1).toLowerCase(Locale.ROOT);
                errMsg = labelText + " field is no longer " + labelText;
            }
            final DescriptorProtos.FieldDescriptorProto.Label newLabel = labels.get((i + 1) % labels.size());
            FileDescriptor updatedFile = mutateField("MySimpleRecord", "str_value_indexed", oldFile,
                    field -> field.setLabel(newLabel));
            assertInvalid(errMsg, oldFile, updatedFile);

            oldFile = updatedFile;
        }
    }

    @Test
    void addRequiredField() {
        FileDescriptor updatedFile = mutateMessageType("MySimpleRecord", message ->
                addField(message)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                        .setName("new_int_field")
        );
        assertInvalid("required field added to record type", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("required field added to record type", metaData1, metaData2);
    }

    @Test
    void dropType() {
        FileDescriptor updatedFile = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME,
                message -> message.removeField(1));
        assertInvalid("record type removed from union", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        MetaDataException e = assertThrows(MetaDataException.class, () -> replaceRecordsDescriptor(metaData1, updatedFile));
        assertThat(e.getMessage(), containsString("Unknown record type MyOtherRecord"));

        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder -> {
            protoBuilder.removeRecordTypes(1);
            final List<RecordMetaDataProto.Index> indexes = protoBuilder.getIndexesList().stream()
                    .filter(index -> !index.getRecordTypeList().contains("MyOtherRecord"))
                    .collect(Collectors.toList());
            protoBuilder.clearIndexes().addAllIndexes(indexes);
        });
        assertInvalid("record type removed from union", metaData1, metaData2);
    }

    @Test
    void addNewPlaceInUnionDescriptor() {
        // Add a new field to the union descriptor that points to an existing record; leave the old one
        FileDescriptor updatedFile = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, message -> {
            message.getFieldBuilderList().forEach(field -> {
                if (field.getName().endsWith("MySimpleRecord")) {
                    field.setName("_MyOldSimpleRecordField");
                }
            });
            message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("MySimpleRecord")
                    .setName("_MySimpleRecord")
                    .setNumber(1066));
        });
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertEquals(1066, metaData2.getUnionFieldForRecordType(metaData2.getRecordType("MySimpleRecord")).getNumber());
        validator.validate(metaData1, metaData2);
        assertEquals(metaData1.getRecordType("MySimpleRecord").getRecordTypeKey(), metaData2.getRecordType("MySimpleRecord").getRecordTypeKey());

        // Add a new field that points to an existing record but put it in a lower position in the union which makes the record type key change
        updatedFile = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, updatedFile,
                message -> message.removeField(0));
        RecordMetaData metaData3 = RecordMetaData.build(updatedFile);
        updatedFile = mutateMessageType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, updatedFile, message ->
                message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("MySimpleRecord")
                        .setName("_MyOtherSimpleRecord")
                        .setNumber(800))
        );
        RecordMetaData metaData4 = replaceRecordsDescriptor(metaData3, updatedFile);
        RecordType recordType3 = metaData3.getRecordType("MySimpleRecord");
        assertEquals(1066, metaData3.getUnionFieldForRecordType(recordType3).getNumber());
        assertEquals(1066L, recordType3.getRecordTypeKey());
        RecordType recordType4 = metaData4.getRecordType("MySimpleRecord");
        assertEquals(1066, metaData4.getUnionFieldForRecordType(recordType4).getNumber());
        assertEquals(800L, recordType4.getRecordTypeKey());
        assertInvalid("record type key changed", metaData3, metaData4);
    }

    // Record types tests

    @Nonnull
    private RecordMetaData addNewRecordType(@Nonnull RecordMetaData metaData, @Nonnull Consumer<RecordMetaDataProto.RecordType.Builder> newRecordTypeHook) {
        RecordMetaDataProto.RecordType.Builder newRecordTypeBuilder = RecordMetaDataProto.RecordType.newBuilder()
                .setName("NewRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no").toKeyExpression())
                .setSinceVersion(metaData.getVersion() + 1);
        newRecordTypeHook.accept(newRecordTypeBuilder);
        return RecordMetaData.build(metaData.toProto().toBuilder()
                .setVersion(metaData.getVersion() + 1)
                .addDependencies(TestRecords1Proto.getDescriptor().toProto())
                .setRecords(TestNewRecordTypeProto.getDescriptor().toProto())
                .addRecordTypes(newRecordTypeBuilder)
                .build()
        );
    }

    @Nonnull
    private RecordMetaData addNewRecordType(@Nonnull RecordMetaData metaData) {
        return addNewRecordType(metaData, ignore -> { });
    }

    @Test
    void newTypeWithoutSinceVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = addNewRecordType(metaData1, RecordMetaDataProto.RecordType.Builder::clearSinceVersion);
        assertFalse(validator.allowsNoSinceVersion());
        assertInvalid("new record type is missing since version", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowNoSinceVersion(true)
                .build();
        assertTrue(laxerValidator.allowsNoSinceVersion());
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    void newTypeWithOlderSinceVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = addNewRecordType(metaData1, protoBuilder -> protoBuilder.setSinceVersion(metaData1.getVersion() - 1));
        assertInvalid("new record type has since version older than old meta-data", metaData1, metaData2);

        RecordMetaData metaData3 = addNewRecordType(metaData1, protoBuilder -> protoBuilder.setSinceVersion(metaData1.getVersion()));
        assertInvalid("new record type has since version older than old meta-data", metaData1, metaData3);
    }

    @Test
    void typeModifiesSinceVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData1.toProto().toBuilder();
        protoBuilder.setVersion(metaData1.getVersion() + 1);
        protoBuilder.getRecordTypesBuilderList().get(0).setSinceVersion(metaData1.getVersion() + 1);
        RecordMetaData metaData2 = RecordMetaData.build(protoBuilder.build());
        assertInvalid("record type since version changed", metaData1, metaData2);
    }

    @Test
    void recordTypeKeyChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1);
        protoBuilder.getRecordTypesBuilder(0)
                .setExplicitKey(RecordKeyExpressionProto.Value.newBuilder()
                    .setStringValue("new_key"));
        RecordMetaData metaData2 = RecordMetaData.build(protoBuilder.build());
        assertInvalid("record type key changed", metaData1, metaData2);
    }

    @Test
    void primaryKeyChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1);
        protoBuilder.getRecordTypesBuilder(0)
                .setPrimaryKey(Key.Expressions.field("num_value_2").toKeyExpression());

        RecordMetaData metaData2 = RecordMetaData.build(protoBuilder.build());
        assertInvalid("record type primary key changed", metaData1, metaData2);
    }

    // JoinedRecordType type tests

    private void addNumValue2Join(@Nonnull RecordMetaDataBuilder metaDataBuilder) {
        final JoinedRecordTypeBuilder joinBuilder = metaDataBuilder.addJoinedRecordType("join_nv2");
        joinBuilder.addConstituent("l", "MySimpleRecord");
        joinBuilder.addConstituent("r", "MyOtherRecord");
        joinBuilder.addJoin("l", Key.Expressions.field("num_value_2"), "r", Key.Expressions.field("num_value_2"));

        metaDataBuilder.addIndex("join_nv2", "joined$l.num_value_unique", Key.Expressions.field("l").nest("num_value_unique"));
    }

    private void addThreeWayNumValue2Join(@Nonnull RecordMetaDataBuilder metaDataBuilder) {
        final JoinedRecordTypeBuilder joinBuilder = metaDataBuilder.addJoinedRecordType("join_nv2");
        joinBuilder.addConstituent("s", "MySimpleRecord");
        joinBuilder.addConstituent("o", "MyOtherRecord");
        joinBuilder.addConstituent("a", "AnotherRecord");
        joinBuilder.addJoin("s", Key.Expressions.field("num_value_2"), "o", Key.Expressions.field("num_value_2"));
        joinBuilder.addJoin("s", Key.Expressions.field("num_value_2"), "a", Key.Expressions.field("num_value_2"));

        metaDataBuilder.addIndex("join_nv2", "joined$nv3", Key.Expressions.concat(
                Key.Expressions.field("s").nest("num_value_3_indexed"),
                Key.Expressions.field("o").nest("num_value_3_indexed"),
                Key.Expressions.field("a").nest("num_value_3_indexed"))
        );
    }

    private void addStrValueSelfJoin(@Nonnull RecordMetaDataBuilder metaDataBuilder) {
        final JoinedRecordTypeBuilder joinBuilder = metaDataBuilder.addJoinedRecordType("self_join_svi");
        joinBuilder.addConstituent("s1", "MySimpleRecord");
        joinBuilder.addConstituent("s2", "MySimpleRecord");
        joinBuilder.addJoin("s1", Key.Expressions.field("str_value_indexed"), "s2", Key.Expressions.field("str_value_indexed"));

        metaDataBuilder.addIndex("self_join_svi", "self_join_svi$num_value_unique", Key.Expressions.concat(Key.Expressions.field("s1").nest("num_value_unique"), Key.Expressions.field("s2").nest("num_value_unique")));
    }

    @Nonnull
    private RecordMetaData createSimpleMetaData(@Nonnull FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private RecordMetaData createEvolvedMetaData(@Nonnull FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1EvolvedProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private RecordMetaData mutateJoinedRecordType(@Nonnull RecordMetaData metaData, @Nonnull String typeName, @Nonnull Consumer<RecordMetaDataProto.JoinedRecordType.Builder> typeMutator) {
        return updateMetaData(metaData, metaDataProtoBuilder -> {
            for (RecordMetaDataProto.JoinedRecordType.Builder typeBuilder : metaDataProtoBuilder.getJoinedRecordTypesBuilderList()) {
                if (typeBuilder.getName().equals(typeName)) {
                    typeMutator.accept(typeBuilder);
                }
            }
        });
    }

    @Test
    void addJoinedType() {
        final RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.setVersion(metaData1.getVersion() + 1);
        addNumValue2Join(metaDataBuilder);
        RecordMetaData metaData2 = metaDataBuilder.build();

        validator.validate(metaData1, metaData2);
    }

    @Test
    void dropJoinedType() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.setVersion(metaData1.getVersion() + 1);
        metaDataBuilder.addFormerIndex(new FormerIndex("joined$l.num_value_unique", metaData1.getVersion(), metaData1.getVersion() + 1, "joined$l.num_value_unique"));
        RecordMetaData metaData2 = metaDataBuilder.build();

        validator.validate(metaData1, metaData2);
    }

    @Test
    void swapJoinTypeDefinitions() {
        final RecordMetaData metaData1 = createSimpleMetaData(metaDataBuilder -> {
            addNumValue2Join(metaDataBuilder);
            addStrValueSelfJoin(metaDataBuilder);
        });

        final RecordMetaData metaData2 = updateMetaData(metaData1, metaDataBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Builder type1 = metaDataBuilder.getJoinedRecordTypesBuilder(0);
            final RecordMetaDataProto.JoinedRecordType.Builder type2 = metaDataBuilder.getJoinedRecordTypesBuilder(1);

            // Swap the two types' record type keys
            RecordKeyExpressionProto.Value type1Key = type1.getRecordTypeKey();
            type1.setRecordTypeKey(type2.getRecordTypeKey());
            type2.setRecordTypeKey(type1Key);
        });

        assertNotEquals(metaData1.getSyntheticRecordType("join_nv2").getRecordTypeKey(), metaData2.getSyntheticRecordType("join_nv2").getRecordTypeKey());
        assertNotEquals(metaData1.getSyntheticRecordType("self_join_svi").getRecordTypeKey(), metaData2.getSyntheticRecordType("self_join_svi").getRecordTypeKey());

        assertInvalid("join constituent name changed", metaData1, metaData2);
        final MetaDataEvolutionValidator stricterValidator = validator.asBuilder()
                .setDisallowTypeRenames(true)
                .build();
        assertInvalid("synthetic record type name changed", stricterValidator, metaData1, metaData2);
    }

    @Test
    void swapJoinConstituents() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2", joinedType -> {
            final var constituentsList = joinedType.getJoinConstituentsList();
            joinedType.clearJoinConstituents();
            joinedType.addJoinConstituents(constituentsList.get(1));
            joinedType.addJoinConstituents(constituentsList.get(0));
        });

        assertInvalid("join constituent name changed", metaData1, metaData2);
        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowFieldRenames(true)
                .build();
        assertInvalid("join constituent type changed", laxerValidator, metaData1, metaData2);
    }

    @Test
    void swapSelfJoinConstituents() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addStrValueSelfJoin);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "self_join_svi", joinedType -> {
            final var constituentsList = joinedType.getJoinConstituentsList();
            joinedType.clearJoinConstituents();
            joinedType.addJoinConstituents(constituentsList.get(1));
            joinedType.addJoinConstituents(constituentsList.get(0));
        });

        assertInvalid("join constituent name changed", metaData1, metaData2);
        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowFieldRenames(true)
                .build();
        assertInvalid("join changed left constituent", laxerValidator, metaData1, metaData2);

        final RecordMetaData metaData3 = mutateJoinedRecordType(metaData2, "self_join_svi",
                // Swap the names in the Join definition
                joinedType -> joinedType.getJoinsBuilder(0).setLeft("s2").setRight("s1"));
        assertInvalid("join constituent name changed", metaData1, metaData3);
        assertInvalid("index key expression does not match required", laxerValidator, metaData1, metaData3);

        final RecordMetaData metaData4 = mutateIndex(metaData3, "self_join_svi$num_value_unique",
                // Swap the names in the index definition
                index -> index.setRootExpression(Key.Expressions.concat(Key.Expressions.field("s2").nest("num_value_unique"), Key.Expressions.field("s1").nest("num_value_unique")).toKeyExpression()));
        assertInvalid("join constituent name changed", metaData1, metaData4);
        laxerValidator.validate(metaData1, metaData4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"MySimpleRecord", "MyOtherRecord"})
    void changeUnderlyingJoinConstituentType(String recordType) {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        // Delete and recreate the MySimpleRecord type. As we can't actually delete types, this is done by
        // renaming the old type and deprecating its field in the union descriptor. We then add a new copy of
        // it to the list of types, ensuring to assign that new type a new union descriptor field.
        // We use a recreated version of the same type to maximize the number of things that are the same:
        // the same type name, the same set of fields, etc. The only way that it should notice that something
        // is up is that it has to check the position in the union descriptor
        FileDescriptor updatedDescriptor = mutateFile(TestRecords1Proto.getDescriptor(), fileBuilder -> {
            DescriptorProtos.DescriptorProto oldType = null;
            for (DescriptorProtos.DescriptorProto.Builder messageType : fileBuilder.getMessageTypeBuilderList()) {
                if (messageType.getName().equals(recordType)) {
                    // Rename the old MySimpleRecord. Save a copy prior to the rename
                    oldType = messageType.build();
                    messageType.setName(recordType + "__old");
                } else if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                    // Rename the reference in the union descriptor
                    for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : messageType.getFieldBuilderList()) {
                        if (fieldBuilder.getTypeName().endsWith(recordType)) {
                            deprecateField(fieldBuilder);
                            fieldBuilder
                                    .setName("_" + recordType + "__old")
                                    .setTypeName(recordType + "__old");
                        }
                    }
                    // Create a field for the recreated MySimpleRecord (added below)
                    addField(messageType)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(recordType)
                            .setName("_" + recordType);

                }
            }
            // Add a copy of MySimpleRecord to the fileBuilder. This represents recreating a new type with
            // the same name (and in this case, the same set of fields).
            fileBuilder.addMessageType(oldType);
        });
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedDescriptor, metaDataBuilder -> {
            // Rename the old record type
            for (RecordMetaDataProto.RecordType.Builder recordTypeBuilder : metaDataBuilder.getRecordTypesBuilderList()) {
                if (recordTypeBuilder.getName().equals(recordType)) {
                    recordTypeBuilder.setName(recordType + "__old");
                }
            }
            // Create a new record type with an updated since version
            metaDataBuilder.addRecordTypesBuilder()
                    .setName(recordType)
                    .setSinceVersion(metaDataBuilder.getVersion())
                    .setPrimaryKey(Key.Expressions.field("rec_no").toKeyExpression());

            for (RecordMetaDataProto.Index.Builder indexBuilder : metaDataBuilder.getIndexesBuilderList()) {
                // Rename any references to the type in the indexes. Note that the joined record type's index is _not_ updated
                if (indexBuilder.getRecordTypeList().contains(recordType)) {
                    final List<String> oldTypes = indexBuilder.getRecordTypeList();
                    indexBuilder.clearRecordType();
                    oldTypes.stream()
                            .map(type -> type.equals(recordType) ? (recordType + "__old") : type)
                            .forEach(indexBuilder::addRecordType);
                }
            }
        });

        assertFalse(validator.disallowsTypeRenames());
        assertInvalid("join constituent type changed", metaData1, metaData2);
        final MetaDataEvolutionValidator stricterValidator = validator.asBuilder()
                .setDisallowTypeRenames(true)
                .build();
        assertTrue(stricterValidator.disallowsTypeRenames());
        assertInvalid("record type name changed", stricterValidator, metaData1, metaData2);

        RecordMetaData metaData3 = mutateJoinedRecordType(metaData2, "join_nv2", typeBuilder -> {
            for (RecordMetaDataProto.JoinedRecordType.JoinConstituent.Builder constituent : typeBuilder.getJoinConstituentsBuilderList()) {
                if (constituent.getRecordType().equals(recordType)) {
                    constituent.setRecordType(recordType + "__old");
                }
            }
        });
        validator.validate(metaData1, metaData3);
        assertInvalid("record type name changed", stricterValidator, metaData1, metaData2);
    }

    @Test
    void changeJoinConstituentName() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addStrValueSelfJoin);

        final RecordMetaData metaData2 = updateMetaData(metaData1, metaDataBuilder -> {
            for (RecordMetaDataProto.JoinedRecordType.Builder joinedTypeBuilder : metaDataBuilder.getJoinedRecordTypesBuilderList()) {
                if (joinedTypeBuilder.getName().equals("self_join_svi")) {
                    for (RecordMetaDataProto.JoinedRecordType.JoinConstituent.Builder constituentBuilder : joinedTypeBuilder.getJoinConstituentsBuilderList()) {
                        if (constituentBuilder.getName().equals("s1")) {
                            constituentBuilder.setName("x");
                        }
                    }
                    joinedTypeBuilder.getJoinsBuilderList().get(0)
                            .setLeft("x");
                }
            }

            for (RecordMetaDataProto.Index.Builder indexBuilder : metaDataBuilder.getIndexesBuilderList()) {
                if (indexBuilder.getName().equals("self_join_svi$num_value_unique")) {
                    indexBuilder.setRootExpression(Key.Expressions.concat(Key.Expressions.field("x").nest("num_value_unique"), Key.Expressions.field("s2").nest("num_value_unique")).toKeyExpression());
                }
            }
        });

        assertInvalid("join constituent name changed", metaData1, metaData2);
        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowFieldRenames(true)
                .build();
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    void addJoinConstituent() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestRecords1EvolvedProto.getDescriptor(), metaDataBuilder -> {
            metaDataBuilder.addRecordTypesBuilder()
                    .setName("AnotherRecord")
                    .setPrimaryKey(Key.Expressions.field("rec_no").toKeyExpression())
                    .setSinceVersion(metaDataBuilder.getVersion());

            for (RecordMetaDataProto.JoinedRecordType.Builder joinTypeBuilder : metaDataBuilder.getJoinedRecordTypesBuilderList()) {
                joinTypeBuilder.addJoinConstituentsBuilder()
                        .setName("x")
                        .setRecordType("AnotherRecord")
                        .setOuterJoined(false);
                joinTypeBuilder.addJoinsBuilder()
                        .setLeft("l")
                        .setLeftExpression(Key.Expressions.field("num_value_2").toKeyExpression())
                        .setRight("x")
                        .setRightExpression(Key.Expressions.field("num_value_2").toKeyExpression());
            }
        });
        assertInvalid("join constituent count changed", metaData1, metaData2);
    }

    @Test
    void removeJoinConstituent() {
        final RecordMetaData metaData1 = createEvolvedMetaData(this::addThreeWayNumValue2Join);

        final RecordMetaData metaData2 = updateMetaData(metaData1, metaDataBuilder -> {
            for (RecordMetaDataProto.JoinedRecordType.Builder joinedTypeBuilder : metaDataBuilder.getJoinedRecordTypesBuilderList()) {
                if (joinedTypeBuilder.getName().equals("join_nv2")) {
                    joinedTypeBuilder.removeJoinConstituents(0);
                    joinedTypeBuilder.clearJoins();
                    joinedTypeBuilder.addJoinsBuilder()
                            .setLeft("o")
                            .setLeftExpression(Key.Expressions.field("num_value_2").toKeyExpression())
                            .setRight("a")
                            .setRightExpression(Key.Expressions.field("num_value_2").toKeyExpression());
                }
            }
            for (RecordMetaDataProto.Index.Builder index : metaDataBuilder.getIndexesBuilderList()) {
                if (index.getRecordTypeList().contains("join_nv2")) {
                    index.setRootExpression(Key.Expressions.concat(
                            Key.Expressions.field("o").nest("num_value_3_indexed"),
                            Key.Expressions.field("a").nest("num_value_3_indexed")
                    ).toKeyExpression());
                }
            }
        });
        assertInvalid("join constituent count changed", metaData1, metaData2);
    }

    @Test
    void addJoinCondition() {
        final RecordMetaData metaData1 = createEvolvedMetaData(this::addThreeWayNumValue2Join);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2", joinedTypeBuilder ->
                // Technically, this join criterion is implied by the existing two criteria via transitivity, so
                // this may need to change if we ever modify the check to account for that
                joinedTypeBuilder.addJoinsBuilder()
                        .setLeft("o")
                        .setLeftExpression(Key.Expressions.field("num_value_2").toKeyExpression())
                        .setLeft("a")
                        .setRightExpression(Key.Expressions.field("num_value_2").toKeyExpression()));

        assertInvalid("join type join count changed", metaData1, metaData2);
    }

    @Test
    void removeJoinCondition() {
        final RecordMetaData metaData1 = createEvolvedMetaData(this::addThreeWayNumValue2Join);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2",
                joinedTypeBuilder -> joinedTypeBuilder.removeJoins(0));
        assertInvalid("join type join count changed", metaData1, metaData2);
    }

    @Test
    void changeOuterJoinedProperty() {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        // Assert cannot go from not outer joined to outer joined
        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2",
                joinedTypeBuilder -> joinedTypeBuilder.getJoinConstituentsBuilder(0).setOuterJoined(true));
        assertInvalid("join constituent outer-joined property changed", metaData1, metaData2);

        // Assert cannot go from outer joined to not outer joined
        final RecordMetaData metaData3 = updateMetaData(metaData1, metaDataBuilder -> metaDataBuilder.setVersion(metaData2.getVersion() + 1));
        assertInvalid("join constituent outer-joined property changed", metaData2, metaData3);
    }

    @Test
    void changeJoinLeftComponents() {
        final RecordMetaData metaData1 = createEvolvedMetaData(this::addThreeWayNumValue2Join);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setLeft("a");
        });
        assertInvalid("join changed left constituent", metaData1, metaData2);

        final RecordMetaData metaData3 = mutateJoinedRecordType(metaData1, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setLeftExpression(Key.Expressions.field("num_value_unique").toKeyExpression());
        });
        assertInvalid("join changed left expression", metaData1, metaData3);
    }

    @Test
    void changeJoinRightComponents() {
        final RecordMetaData metaData1 = createEvolvedMetaData(this::addThreeWayNumValue2Join);

        final RecordMetaData metaData2 = mutateJoinedRecordType(metaData1, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setRight("a");
        });
        assertInvalid("join changed right constituent", metaData1, metaData2);

        final RecordMetaData metaData3 = mutateJoinedRecordType(metaData1, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setRightExpression(Key.Expressions.field("num_value_3_indexed").toKeyExpression());
        });
        assertInvalid("join changed right expression", metaData1, metaData3);
    }

    @ParameterizedTest
    @MethodSource("deprecatedArgs")
    void renameFieldInJoinExpression(boolean deprecated) {
        final RecordMetaData metaData1 = createSimpleMetaData(this::addNumValue2Join);

        // Change a field in MySimpleRecord used in the join
        FileDescriptor renamedSimpleNum2 = mutateMessageType("MySimpleRecord", typeBuilder -> {
            for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : typeBuilder.getFieldBuilderList()) {
                if (fieldBuilder.getName().equals("num_value_2")) {
                    if (deprecated) {
                        deprecateField(fieldBuilder);
                    }
                    fieldBuilder.setName("num_value_2__old");
                }
            }
            addField(typeBuilder)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64)
                    .setName("num_value_2");
        });
        final RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, renamedSimpleNum2);
        fieldRenameChecker.assertInvalidRenaming("join changed left expression", deprecated, metaData1, metaData2);

        final RecordMetaData metaData3 = mutateJoinedRecordType(metaData2, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setLeftExpression(Key.Expressions.field("num_value_2__old").toKeyExpression());
        });
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData3);

        // Now change the same field in MyOtherRecord
        FileDescriptor renamedOtherNum2 = mutateMessageType("MyOtherRecord", renamedSimpleNum2, typeBuilder -> {
            for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : typeBuilder.getFieldBuilderList()) {
                if (fieldBuilder.getName().equals("num_value_2")) {
                    if (deprecated) {
                        deprecateField(fieldBuilder);
                    }
                    fieldBuilder.setName("num_value_2__old");
                }
            }
            addField(typeBuilder)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64)
                    .setName("num_value_2");
        });
        final RecordMetaData metaData4 = replaceRecordsDescriptor(metaData3, renamedOtherNum2);
        fieldRenameChecker.assertInvalidRenaming("join changed right expression", deprecated, metaData1, metaData4);
        fieldRenameChecker.assertInvalidRenaming("join changed right expression", deprecated, metaData3, metaData4);

        final RecordMetaData metaData5 = mutateJoinedRecordType(metaData4, "join_nv2", joinedTypeBuilder -> {
            final RecordMetaDataProto.JoinedRecordType.Join.Builder joinBuilder = joinedTypeBuilder.getJoinsBuilder(0);
            joinBuilder.setRightExpression(Key.Expressions.field("num_value_2__old").toKeyExpression());
        });
        fieldRenameChecker.assertValidRenaming(deprecated, metaData1, metaData5);
        fieldRenameChecker.assertValidRenaming(deprecated, metaData3, metaData5);
    }

    // UnnestedRecordTypeTests



    // Former index tests

    @Test
    void removeFormerIndex() {
        RecordMetaDataBuilder metaData1Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData1Builder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData1 = metaData1Builder.getRecordMetaData();
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder().setVersion(metaData1.getVersion() + 1).clearFormerIndexes().build()
        );
        assertInvalid("former index removed", metaData1, metaData2);
    }

    @Test
    void changeFormerIndexVersion() {
        RecordMetaDataBuilder metaData1Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData1Builder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData1 = metaData1Builder.getRecordMetaData();
        RecordMetaDataProto.MetaData.Builder metaData2ProtoBuilder = metaData1.toProto()
                .toBuilder()
                .setVersion(metaData1.getVersion() + 1);
        metaData2ProtoBuilder.setFormerIndexes(0, metaData2ProtoBuilder.getFormerIndexesBuilder(0).setRemovedVersion(metaData1.getVersion() + 1));
        RecordMetaData metaData2 = RecordMetaData.build(metaData2ProtoBuilder.build());
        assertInvalid("removed version of former index differs from prior version", metaData1, metaData2);

        metaData2ProtoBuilder.setFormerIndexes(0, metaData2ProtoBuilder.getFormerIndexesBuilder(0).setRemovedVersion(metaData1.getVersion()).setAddedVersion(metaData1.getVersion() - 2));
        metaData2 = RecordMetaData.build(metaData2ProtoBuilder.build());
        assertInvalid("added version of former index differs from prior version", metaData1, metaData2);

        metaData2ProtoBuilder.setFormerIndexes(0, metaData2ProtoBuilder.getFormerIndexesBuilder(0).clearAddedVersion());
        metaData2 = RecordMetaData.build(metaData2ProtoBuilder.build());
        assertInvalid("added version of former index differs from prior version", metaData1, metaData2);
    }

    @Test
    void changeFormerIndexName() {
        RecordMetaDataBuilder metaData1Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData1Builder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData1 = metaData1Builder.getRecordMetaData();
        RecordMetaDataProto.MetaData.Builder metaData2ProtoBuilder = metaData1.toProto()
                .toBuilder()
                .setVersion(metaData1.getVersion() + 1);
        metaData2ProtoBuilder.setFormerIndexes(0, metaData2ProtoBuilder.getFormerIndexesBuilder(0).setFormerName("some_other_name"));
        RecordMetaData metaData2 = RecordMetaData.build(metaData2ProtoBuilder.build());
        assertInvalid("name of former index differs from prior version", metaData1, metaData2);

        metaData2ProtoBuilder.setFormerIndexes(0, metaData2ProtoBuilder.getFormerIndexesBuilder(0).clearFormerName());
        metaData2 = RecordMetaData.build(metaData2ProtoBuilder.build());
        assertInvalid("name of former index differs from prior version", metaData1, metaData2);

        // For existing former indexes, changing the name is not allowed even with the laxer validation option
        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowMissingFormerIndexNames(true)
                .build();
        assertInvalid("name of former index differs from prior version", laxerValidator, metaData1, metaData2);
    }

    @Test
    void formerIndexFromThePast() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder()
                        .setVersion(metaData1.getVersion() + 1)
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dummy_key").pack()))
                                .setRemovedVersion(metaData1.getVersion() - 1)
                                .build())
                        .build()
        );
        assertInvalid("new former index has removed version that is not newer than the old meta-data version", metaData1, metaData2);
        RecordMetaData metaData3 = RecordMetaData.build(
                metaData1.toProto().toBuilder()
                        .setVersion(metaData1.getVersion() + 1)
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dummy_key").pack()))
                                .setRemovedVersion(metaData1.getVersion())
                                .build())
                        .build()
        );
        assertInvalid("new former index has removed version that is not newer than the old meta-data version", metaData1, metaData3);
    }

    @Test
    void formerIndexWithoutExistingIndex() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder()
                        .setVersion(metaData1.getVersion() + 2)
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dummy_key").pack()))
                                .setRemovedVersion(metaData1.getVersion() + 2)
                                .setAddedVersion(metaData1.getVersion() + 1)
                                .build())
                        .build()
        );
        validator.validate(metaData1, metaData2);

        metaData2 = RecordMetaData.build(
                metaData2.toProto().toBuilder()
                        .setFormerIndexes(0, metaData2.getFormerIndexes().get(0).toProto().toBuilder()
                                .setAddedVersion(metaData1.getVersion())
                                .build())
                        .build()
        );
        assertInvalid("former index without existing index has added version prior to old meta-data version", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowOlderFormerIndexAddedVerions(true)
                .build();
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    void indexUsedWhereFormerIndexWas() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaDataBuilder metaData2Builder = RecordMetaData.newBuilder().setRecords(
                metaData1.toProto().toBuilder().clearFormerIndexes().build()
        );
        Index newIndex = new Index("newIndex", "str_value_indexed");
        newIndex.setSubspaceKey("MySimpleRecord$str_value_indexed");
        metaData2Builder.addIndex("MySimpleRecord", newIndex);
        RecordMetaData metaData2 = metaData2Builder.getRecordMetaData();
        assertInvalid("former index key used for new index in meta-data", metaData1, metaData2);
    }

    @Test
    void removeIndexAndChangeName() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        // FIXME: Calling getRecordMetaData appears to pollute the FormerIndexes list
        RecordMetaData metaData1 = RecordMetaData.build(metaDataBuilder.getRecordMetaData().toProto());
        metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaDataProto.MetaData metaData2Proto = metaDataBuilder.getRecordMetaData().toProto();
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData2Proto.toBuilder()
                        .setVersion(metaData2Proto.getVersion() + 1)
                        .removeFormerIndexes(0)
                        .addFormerIndexes(metaData2Proto.getFormerIndexes(0).toBuilder().setFormerName("some_other_name"))
                        .build()
        );
        assertInvalid("former index has different name", metaData1, metaData2);

        // Dropping the name is fine if and only if the corresponding option is set
        RecordMetaData metaData3 = RecordMetaData.newBuilder().setRecords(
                metaData2Proto.toBuilder()
                        .setVersion(metaData2Proto.getVersion() + 1)
                        .removeFormerIndexes(0)
                        .addFormerIndexes(metaData2Proto.getFormerIndexes(0).toBuilder().clearFormerName())
                        .build()
        ).getRecordMetaData();
        assertFalse(validator.allowsMissingFormerIndexNames());
        assertInvalid("former index has different name", metaData1, metaData3);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowMissingFormerIndexNames(true)
                .build();
        assertTrue(laxerValidator.allowsMissingFormerIndexNames());
        laxerValidator.validate(metaData1, metaData3);
    }

    @Test
    void removeIndexAndDropAddedVersion() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData1 = RecordMetaData.build(metaDataBuilder.getRecordMetaData().toProto());
        metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaDataProto.MetaData metaData2Proto = metaDataBuilder.getRecordMetaData().toProto();
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData2Proto.toBuilder()
                    .removeFormerIndexes(0)
                    .addFormerIndexes(metaData2Proto.getFormerIndexes(0).toBuilder().clearAddedVersion())
                    .build()
        );
        assertFalse(validator.allowsOlderFormerIndexAddedVersions());
        assertInvalid("former index reports added version older than replacing index", metaData1, metaData2);

        // With the option set, it should validate
        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowOlderFormerIndexAddedVerions(true)
                .build();
        assertTrue(laxerValidator.allowsOlderFormerIndexAddedVersions());
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    void removeIndexAndChangeAddedVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(metaData1.toProto());
        metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData2 = metaDataBuilder.build();
        validator.validate(metaData1, metaData2); // index correctly removed

        // Modify the proto so that the added version is not correct
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData2.toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).setAddedVersion(metaData2.getVersion());
        RecordMetaData metaData3 = RecordMetaData.build(protoBuilder.build());
        assertInvalid("former index added after old index", metaData1, metaData3);
    }

    @Test
    void removeIndexAndChangeLastModifiedVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // Step 1: Update the index definition in a way that updates the last modified version
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", index -> {
            // Mark the index as unique (and bump its last modified version
            makeUnique(index);
            index.setLastModifiedVersion(index.getLastModifiedVersion() + 1);
        });
        assertFalse(validator.allowsIndexRebuilds());
        assertInvalid("last modified version of index changed", metaData1, metaData2);

        final MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        assertTrue(laxerValidator.allowsIndexRebuilds());
        laxerValidator.validate(metaData1, metaData2);

        // Step 2: Modify the original meta-data to remove the index. This will insert a former index with
        // the wrong last modified version into the meta-data
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(metaData1.toProto());
        metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        metaDataBuilder.setVersion(metaData2.getVersion() + 1);
        RecordMetaData metaData3 = metaDataBuilder.build();
        validator.validate(metaData1, metaData3);
        assertInvalid("new former index has removed version that is not newer than the old meta-data version", metaData2, metaData3);
        // This is why we can't allow this transformation: the former index is not found when updating from metaData2 to metaData3
        assertThat(metaData3.getFormerIndexesSince(metaData2.getVersion()), empty());
    }

    /**
     * This test is supposed to validate that the "default" way of removing an index actually updates fields
     * in a safe way. As such, it is more to validate the methods on a RecordMetaDataBuilder that mutate version
     * information than the evolution validator.
     */
    @Test
    void defaultIndexRemovalPath() {
        final String newIndexName = "MySimpleRecord$num_value_2";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", newIndexName, "num_value_2");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertNotNull(metaData1.getIndex(newIndexName));
        assertEquals(metaData1.getVersion(), metaData1.getIndex(newIndexName).getAddedVersion());
        assertEquals(metaData1.getVersion(), metaData1.getIndex(newIndexName).getLastModifiedVersion());
        metaDataBuilder.removeIndex(newIndexName);
        RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
        assertEquals(1, metaData2.getFormerIndexes().size());
        final FormerIndex newFormerIndex = metaData2.getFormerIndexes().get(0);
        assertEquals(newIndexName, newFormerIndex.getFormerName());
        assertEquals(metaData1.getVersion(), newFormerIndex.getAddedVersion());
        assertEquals(metaData2.getVersion(), newFormerIndex.getRemovedVersion());
        validator.validate(metaData1, metaData2);
    }

    // Index tests

    private void changeOption(@Nonnull RecordMetaDataProto.Index.Builder indexProto, @Nonnull String key, @Nullable String value) {
        boolean found = false;
        int i = 0;
        for (RecordMetaDataProto.Index.Option.Builder option : indexProto.getOptionsBuilderList()) {
            if (key.equals(option.getKey())) {
                if (value != null) {
                    option.setValue(value);
                }
                found = true;
                break;
            }
            i++;
        }
        if (found && value == null) {
            indexProto.removeOptions(i);
        } else if (!found && value != null) {
            indexProto.addOptions(RecordMetaDataProto.Index.Option.newBuilder().setKey(key).setValue(value));
        }
    }

    private void makeUnique(@Nonnull RecordMetaDataProto.Index.Builder indexProto) {
        changeOption(indexProto, IndexOptions.UNIQUE_OPTION, "true");
    }

    @Nonnull
    private void clearOptions(@Nonnull RecordMetaDataProto.Index.Builder indexProto) {
        indexProto.clearOptions();
    }

    @Nonnull
    private RecordMetaData mutateIndex(@Nonnull RecordMetaData metaData, @Nonnull String indexName, @Nonnull Consumer<RecordMetaDataProto.Index.Builder> indexMutator) {
        return updateMetaData(metaData, metaDataProtoBuilder -> {
            for (RecordMetaDataProto.Index.Builder indexProto : metaDataProtoBuilder.getIndexesBuilderList()) {
                if (indexProto.getName().equals(indexName)) {
                    indexMutator.accept(indexProto);
                }
            }
        });
    }

    @Test
    void silentlyRemoveIndex() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder().setVersion(metaData1.getVersion() + 1).removeIndexes(0).build()
        );
        assertInvalid("index missing in new meta-data", metaData1, metaData2);
    }

    @Test
    void newIndexFromThePast() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        Index newIndex = new Index("newIndex", Key.Expressions.field("num_value_2"));
        newIndex.setAddedVersion(metaData1.getVersion() - 1);
        newIndex.setLastModifiedVersion(metaData1.getVersion() - 1);
        RecordMetaDataBuilder metaData2Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData2Builder.addIndex(metaData2Builder.getRecordType("MySimpleRecord"), newIndex);
        metaData2Builder.setVersion(metaData1.getVersion() + 1);
        RecordMetaData metaData2 = metaData2Builder.getRecordMetaData();
        assertInvalid("new index has version that is not newer than the old meta-data version", metaData1, metaData2);

        RecordMetaDataBuilder metaData3Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        newIndex.setAddedVersion(metaData1.getVersion());
        metaData3Builder.addIndex(metaData2Builder.getRecordType("MySimpleRecord"), newIndex);
        metaData2Builder.setVersion(metaData1.getVersion() + 1);
        RecordMetaData metaData3 = metaData2.getRecordMetaData();
        assertInvalid("new index has version that is not newer than the old meta-data version", metaData1, metaData3);
    }

    @Test
    void indexSubspaceKeyChanged() {
        // The index subspace key is the thing that determines whether an index is even there, so changing it
        // is identical to removing the index
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.setSubspaceKey(ByteString.copyFrom(Tuple.from("dummy_key").pack()))
        );
        assertInvalid("index missing in new meta-data", metaData1, metaData2);
    }

    @Test
    void indexNameChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto
                        .setSubspaceKey(ByteString.copyFrom(Tuple.from("MySimpleRecord$str_value_indexed").pack()))
                        .setName("a_different_name")
        );
        assertInvalid("index name changed", metaData1, metaData2);
    }

    @Test
    void indexAddedVersionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setAddedVersion(metaData1.getVersion() + 1).setLastModifiedVersion(metaData1.getVersion() + 1));
        assertInvalid("new index added version does not match old index added version", metaData1, metaData2);

        metaData2 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setAddedVersion(indexProto.getAddedVersion() - 1));
        assertInvalid("new index added version does not match old index added version", metaData1, metaData2);
    }

    @Test
    void indexLastModifiedVersionTooOld() {
        RecordMetaData metaData1 = mutateIndex(RecordMetaData.build(TestRecords1Proto.getDescriptor()), "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setLastModifiedVersion(2));
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setLastModifiedVersion(1));
        assertInvalid("old index has last-modified version newer than new index", metaData1, metaData2);
    }

    @Test
    void indexLastModifiedVersionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setLastModifiedVersion(metaData1.getVersion() + 1));
        assertFalse(validator.allowsIndexRebuilds());
        assertInvalid("last modified version of index changed", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        assertTrue(laxerValidator.allowsIndexRebuilds());
        laxerValidator.validate(metaData1, metaData2);
    }

    private void validateIndexMutation(@Nonnull String errMsg, @Nonnull RecordMetaData metaData1, @Nonnull String indexName, Consumer<RecordMetaDataProto.Index.Builder> indexReplacement) {
        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        RecordMetaData metaData2 = mutateIndex(metaData1, indexName, indexReplacement);
        assertInvalid(errMsg, metaData1, metaData2);
        assertInvalid(errMsg, laxerValidator, metaData1, metaData2);

        // Allow the change if and only if the last modified version is updated and the option allowing rebuilds is set
        RecordMetaData metaData3 = mutateIndex(metaData2, indexName,
                indexProto -> indexProto.setLastModifiedVersion(metaData2.getVersion()));
        assertInvalid("last modified version of index changed", metaData1, metaData3);
        laxerValidator.validate(metaData1, metaData3);
    }

    @Test
    void indexTypeChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index type changed", metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setType(IndexTypes.RANK)
        );
    }

    @Test
    void indexKeyExpressionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index key expression changed", metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> indexProto.setRootExpression(Key.Expressions.field("num_value_2").toKeyExpression()));
    }

    @Test
    void indexRecordTypeRemoved() {
        final String indexName = "simple&other$num_value_2";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"), metaDataBuilder.getRecordType("MyOtherRecord")),
                new Index(indexName, "num_value_2"));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        validateIndexMutation("new index removes record type", metaData1, indexName,
                indexProto -> indexProto.clearRecordType().addRecordType("MySimpleRecord")
        );
    }

    @Test
    void indexRecordTypeAdded() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("new index adds record type that is not newer than old meta-data", metaData1, "MySimpleRecord$num_value_3_indexed", indexProto ->
                indexProto.addRecordType("MyOtherRecord").build()
        );

        // Add NewRecord as a record type to the existing meta-data and index and validate that this change is okay
        // because the new record type is newer the old meta-data version.
        RecordMetaData tempMetaData = addNewRecordType(metaData1);
        RecordMetaData metaData2 = mutateIndex(tempMetaData, "MySimpleRecord$num_value_3_indexed",
                indexProto -> indexProto.addRecordType("NewRecord"));
        validator.validate(metaData1, metaData2); // valid if type and index change happen together
        assertInvalid("new index adds record type that is not newer than old meta-data", tempMetaData, metaData2);
    }

    @Test
    void indexPrimaryKeyComponentsChanged() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "rec_no", "rec_no");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getIndex("rec_no").hasPrimaryKeyComponentPositions(), is(true));

        RecordMetaData tempMetaData = addNewRecordType(metaData1);
        RecordMetaData metaData2 = mutateIndex(tempMetaData, "rec_no",
                indexProto -> indexProto.addRecordType("NewRecord"));
        assertInvalid("new index drops primary key component positions", metaData1, metaData2);

        // This is essentially the behavior change outlined by: https://github.com/FoundationDB/fdb-record-layer/issues/93
        metaData2.getIndex("rec_no").setPrimaryKeyComponentPositions(new int[]{0});
        validator.validate(metaData1, metaData2);

        metaData1.getIndex("rec_no").setPrimaryKeyComponentPositions(null);
        assertInvalid("new index adds primary key component positions", metaData1, metaData2);

        metaData1.getIndex("rec_no").setPrimaryKeyComponentPositions(new int[]{0});
        metaData2.getIndex("rec_no").setPrimaryKeyComponentPositions(new int[]{1});
        assertInvalid("new index changes primary key component positions", metaData1, metaData2);
    }

    @Test
    void addRecordTypeWithUniversalIndex() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(new Index("rec_no", "rec_no"));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertNotNull(metaData1.getUniversalIndex("rec_no"));

        // Keep the index universal
        RecordMetaData metaData2 = addNewRecordType(metaData1);
        assertNotNull(metaData2.getUniversalIndex("rec_no"));
        validator.validate(metaData1, metaData2);

        // Make the index a multi-type index on the original record types
        RecordMetaData metaData3 = mutateIndex(metaData2, "rec_no",
                indexProto -> indexProto.addRecordType("MySimpleRecord").addRecordType("MyOtherRecord"));
        MetaDataException e = assertThrows(MetaDataException.class, () -> metaData3.getUniversalIndex("rec_no"));
        assertThat(e.getMessage(), containsString("Index rec_no not defined"));
        assertInvalid("new index removes record type", metaData2, metaData3);
        validator.validate(metaData1, metaData3);
    }

    @Test
    void uniquenessConstraintChanged() {
        // Adding a uniqueness constraint should throw an error
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index adds uniqueness constraint", metaData1, "MySimpleRecord$str_value_indexed", this::makeUnique);

        // Removing the uniqueness constraint is fine
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", this::makeUnique);
        RecordMetaData metaData3 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        validator.validate(metaData2, metaData3);
        RecordMetaData metaData4 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed", indexProto -> changeOption(indexProto, IndexOptions.UNIQUE_OPTION, "false"));
        validator.validate(metaData2, metaData4);
    }

    @Test
    void allowedForQueriesChanged() {
        // Changing this option is always fine
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.ALLOWED_FOR_QUERY_OPTION, "false"));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.ALLOWED_FOR_QUERY_OPTION, "true"));
        validator.validate(metaData2, metaData3);
        RecordMetaData metaData4 = mutateIndex(metaData3, "MySimpleRecord$str_value_indexed", this::clearOptions);
        validator.validate(metaData3, metaData4);
    }

    @Test
    void changeReplacedByIndex() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.REPLACED_BY_OPTION_PREFIX, "MySimpleRecord$num_value_3_indexed"));
        assertEquals(Collections.singletonList("MySimpleRecord$num_value_3_indexed"),
                metaData2.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        assertEquals(Collections.emptyList(),
                metaData3.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData2, metaData3);
    }

    @Test
    void changeReplacedByIndexSet() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto -> {
            changeOption(indexProto, IndexOptions.REPLACED_BY_OPTION_PREFIX + "_0", "MySimpleRecord$num_value_3_indexed");
            changeOption(indexProto, IndexOptions.REPLACED_BY_OPTION_PREFIX + "_1", "MySimpleRecord$num_value_unique");
        });
        assertThat(metaData2.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames(),
                containsInAnyOrder("MySimpleRecord$num_value_3_indexed", "MySimpleRecord$num_value_unique"));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        assertEquals(Collections.emptyList(),
                metaData3.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData2, metaData3);
    }

    @Test
    void unknownOptionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index option changed", metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, "dummyOption", "dummyValue"));
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto -> {
            makeUnique(indexProto);
            changeOption(indexProto, "dummyOption", "dummyValue");
        });
        RecordMetaData metaData3 = mutateIndex(metaData2, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.UNIQUE_OPTION, null));
        validator.validate(metaData2, metaData3);
        validateIndexMutation("index option changed", metaData3, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, "dummyOption", "dummyValue2"));
    }

    @Test
    void rankLevelsChanged() {
        final String indexName = "MySimpleRecord$rank(num_value_2)";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index(indexName, Key.Expressions.field("num_value_2").ungrouped(), IndexTypes.RANK));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        validateIndexMutation("rank levels changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "4"));
        validateIndexMutation("rank levels changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "" + RankedSet.MAX_LEVELS));

        // Setting the default explicitly is fine
        RecordMetaData metaData2 = mutateIndex(metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "" + RankedSet.DEFAULT_LEVELS));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = mutateIndex(metaData2, indexName, this::clearOptions);
        validator.validate(metaData2, metaData3);
    }

    @Test
    void textOptionsChanged() {
        final String indexName = "MySimpleRecord$text(str_value_indexed)";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index(indexName, Key.Expressions.field("str_value_indexed"), IndexTypes.TEXT));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        validateIndexMutation("text tokenizer changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        // Setting the default explicitly is fine
        RecordMetaData metaData2 = mutateIndex(metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, DefaultTextTokenizer.NAME));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = mutateIndex(metaData2, indexName, this::clearOptions);
        validator.validate(metaData2, metaData3);

        // Increasing the tokenizer version is fine, but decreasing it is not
        RecordMetaData metaData4 = mutateIndex(metaData3, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME));
        RecordMetaData metaData5 = mutateIndex(metaData4, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + TextTokenizer.GLOBAL_MIN_VERSION));
        validator.validate(metaData4, metaData5);
        RecordMetaData metaData6 = mutateIndex(metaData5, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + (TextTokenizer.GLOBAL_MIN_VERSION + 1)));
        validator.validate(metaData5, metaData6);
        validateIndexMutation("text tokenizer version downgraded", metaData6, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + TextTokenizer.GLOBAL_MIN_VERSION));

        // Changing whether aggressive conflict ranges are allowed is safe
        RecordMetaData metaData7 = mutateIndex(metaData6, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "true"));
        validator.validate(metaData6, metaData7);
        RecordMetaData metaData8 = mutateIndex(metaData7, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "false"));
        validator.validate(metaData7, metaData8);

        // Changing whether position lists are omitted is safe
        RecordMetaData metaData9 = mutateIndex(metaData8, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_OMIT_POSITIONS_OPTION, "true"));
        validator.validate(metaData8, metaData9);
        RecordMetaData metaData10 = mutateIndex(metaData9, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_OMIT_POSITIONS_OPTION, "false"));
        validator.validate(metaData9, metaData10);
    }

    @Test
    void optionChangeAllowedWithCustomIndexValidatorRegistry() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = mutateIndex(metaData1, "MySimpleRecord$str_value_indexed", this::makeUnique);
        assertSame(IndexMaintainerFactoryRegistryImpl.instance(), validator.getIndexValidatorRegistry());
        assertInvalid("index adds uniqueness constraint", metaData1, metaData2);

        final IndexValidatorRegistry noOptionsCheckRegistry = validatorRegistryWithNoOptionsCheck();
        MetaDataEvolutionValidator laxerValidator = validator.asBuilder()
                .setIndexValidatorRegistry(noOptionsCheckRegistry)
                .build();
        assertSame(noOptionsCheckRegistry, laxerValidator.getIndexValidatorRegistry());
        laxerValidator.validate(metaData1, metaData2);
    }

    private static class IndexValidatorWithNoOptionsCheck extends IndexValidator {
        public IndexValidatorWithNoOptionsCheck(@Nonnull final Index index) {
            super(index);
        }

        @Override
        protected void validateChangedOptions(@Nonnull final Index oldIndex, @Nonnull final Set<String> changedOptions) {
            // Always say it's good to go
        }
    }

    private static IndexValidatorRegistry validatorRegistryWithNoOptionsCheck() {
        return IndexValidatorWithNoOptionsCheck::new;
    }
}
