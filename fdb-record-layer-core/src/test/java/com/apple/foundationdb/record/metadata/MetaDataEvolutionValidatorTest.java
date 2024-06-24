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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.evolution.TestHeaderAsGroupProto;
import com.apple.foundationdb.record.evolution.TestMergedNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestNewRecordTypeProto;
import com.apple.foundationdb.record.evolution.TestSelfReferenceProto;
import com.apple.foundationdb.record.evolution.TestSelfReferenceUnspooledProto;
import com.apple.foundationdb.record.evolution.TestSplitNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestUnmergedNestedTypesProto;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.PrefixTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link MetaDataEvolutionValidator} class. This mostly consists of trying to perform illegal updates
 * to the meta-data and then verifying that the update fails. Some of the tests may try doing things that
 * <i>seem</i> like they should be illegal but are actually fine.
 */
public class MetaDataEvolutionValidatorTest {
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

    @Test
    public void doNotChangeVersion() {
        // Check if a naive removal of the index without updating the version is checked
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        assertInvalid("new meta-data does not have newer version", metaData1, metaData1);
        RecordMetaDataBuilder metaData2Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData2Builder.removeIndex("MySimpleRecord$str_value_indexed");
        validator.validate(metaData1, metaData2Builder.getRecordMetaData());
        metaData2Builder.setVersion(metaData1.getVersion());
        assertInvalid("new meta-data does not have newer version", metaData1, metaData2Builder.build(false));

        // If the validator allows not changing the version, it should make sure all of the changes are compatible
        MetaDataEvolutionValidator validatorAcceptingSameVersion = MetaDataEvolutionValidator.newBuilder()
                .setAllowNoVersionChange(true)
                .build();
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
    static RecordMetaData replaceRecordsDescriptor(@Nonnull RecordMetaData metaData, @Nonnull FileDescriptor newDescriptor,
                                                    @Nonnull Consumer<RecordMetaDataProto.MetaData.Builder> metaDataMutation) {
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.toProto().toBuilder()
                .setVersion(metaData.getVersion() + 1)
                .setRecords(newDescriptor.toProto())
                .addDependencies(TestRecords1Proto.getDescriptor().toProto());
        metaDataMutation.accept(protoBuilder);
        return RecordMetaData.build(protoBuilder.build());
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
    static FileDescriptor mutateField(@Nonnull String messageName, @Nonnull String fieldName, @Nonnull FileDescriptor originalFile,
                                      @Nonnull Consumer<DescriptorProtos.FieldDescriptorProto.Builder> fieldMutation) {
        return mutateFile(originalFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(messageName)) {
                        message.getFieldBuilderList().forEach(field -> {
                            if (field.getName().equals(fieldName)) {
                                fieldMutation.accept(field);
                            }
                        });
                    }
                })
        );
    }

    @Nonnull
    static FileDescriptor mutateField(@Nonnull String messageName, @Nonnull String fieldName, @Nonnull Consumer<DescriptorProtos.FieldDescriptorProto.Builder> fieldMutation) {
        return mutateField(messageName, fieldName, TestRecords1Proto.getDescriptor(), fieldMutation);
    }

    @Test
    public void changeSplitLongRecords() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        assertThat(metaData1.isSplitLongRecords(), is(false));
        RecordMetaData metaData2 = RecordMetaData.build(metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1)
                .setSplitLongRecords(true)
                .build()
        );
        assertInvalid("new meta-data splits long records", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowUnsplitToSplit(true)
                .build();
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
    public void changeStoreRecordVersions() {
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
    public void swapUnionFields() {
        FileDescriptor updatedDescriptor = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        message.getFieldBuilderList().forEach(field -> {
                            if (field.getNumber() == 1) {
                                field.setNumber(2);
                            } else {
                                field.setNumber(1);
                            }
                        });
                    }
                })
        );
        // The two record types do not have the same form, so swapping them should fail.
        // However, the exact way they fail isn't super important.
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedDescriptor);
        assertInvalid("", metaData1, metaData2);
    }

    /**
     * Validate that changes to the union descriptor that equate two previously differentiated
     * types--even if they have the same form--are disallowed. In theory, this could be allowed if
     * the machinery to handle indexes were sophisticated enough. It would require that every index
     * either be dropped or that all indexes were previously defined on both records and are now
     * defined on the combined record. It also requires that any indexes on record type be dropped.
     */
    @Test
    public void mergeTypes() {
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
                    message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord")
                            .setNumber(message.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().orElse(0) + 1)
                    );
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
    public void splitTypes() {
        // Add a second "MyOtherRecord" to the union descriptor
        FileDescriptor updatedDescriptor = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("MyOtherRecord")
                                .setName("_MyOtherOtherRecord")
                                .setNumber(message.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().orElse(0) + 1));
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
    public void changeRecordTypeName() {
        final MetaDataEvolutionValidator renameDisallowingValidator = MetaDataEvolutionValidator.newBuilder()
                .setDisallowTypeRenames(true)
                .build();

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
    public void swapRecordTypes() {
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
    public void swapIsomorphicRecordTypesWithIndexes() {
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
                    messageType.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord")
                            .setNumber(messageType.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().orElse(0) + 1));
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
    public void swapIsomorphicRecordTypesWithExplicitKeys() {
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
                    messageType.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("MyOtherOtherRecord")
                            .setName("_MyOtherOtherRecord")
                            .setNumber(messageType.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().orElse(0) + 1));
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
    public void dropField() {
        FileDescriptor updatedFile = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("MySimpleRecord")) {
                        int fieldNumValue2Index = 0;
                        while (!message.getField(fieldNumValue2Index).getName().equals("num_value_2")) {
                            fieldNumValue2Index++;
                        }
                        message.removeField(fieldNumValue2Index);
                    }
                })
        );
        assertInvalid("field removed from message descriptor", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field removed from message descriptor", metaData1, metaData2);
    }

    @Test
    public void renameField() {
        FileDescriptor updatedFile = mutateField("MySimpleRecord", "num_value_2",
                field -> field.setName("num_value_too"));
        assertInvalid("field renamed", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field renamed", metaData1, metaData2);

        // This updates both the field name and its indexes which means that this is actually okay.
        updatedFile = mutateField("MySimpleRecord", "str_value_indexed",
                field -> field.setName("str_value_still_indexed"));
        RecordMetaData metaData3 = replaceRecordsDescriptor(metaData1, updatedFile, protoBuilder ->
                protoBuilder.getIndexesBuilderList().forEach(index -> {
                    if (index.getName().equals("MySimpleRecord$str_value_indexed")) {
                        index.setRootExpression(Key.Expressions.field("str_value_still_indexed").toKeyExpression());
                    }
                })
        );
        assertInvalid("field renamed", metaData1, metaData3);
    }

    @Test
    public void fieldTypeChanged() throws InvalidProtocolBufferException {
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
    public void fieldChangedFromMessageToGroup() {
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
    public void enumFieldChanged() {
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
    public void selfReferenceChanged() {
        // This is largely to test that messages which include themselves as nested types don't cause the validator to blow up
        final Descriptor selfReferenceUnion = TestSelfReferenceProto.RecordTypeUnion.getDescriptor();
        final Descriptor unspooledUnion = TestSelfReferenceUnspooledProto.RecordTypeUnion.getDescriptor();
        validator.validateUnion(selfReferenceUnion, unspooledUnion);
        assertInvalid("field removed", unspooledUnion, selfReferenceUnion);

        FileDescriptor updatedUnspooledFile = mutateFile(TestSelfReferenceUnspooledProto.getDescriptor(), fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("Node")) {
                        message.removeField(0);
                    }
                })
        );
        assertNull(updatedUnspooledFile.findMessageTypeByName("Node").findFieldByName("rec_no"));
        assertInvalid("field removed", TestSelfReferenceUnspooledProto.getDescriptor(), updatedUnspooledFile);
    }

    @Test
    public void nestedTypeChangesName() {
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

    @Test
    public void nestedTypeChangesFieldName() {
        FileDescriptor updatedFile = mutateField("HeaderRecord", "num", TestRecordsWithHeaderProto.getDescriptor(),
                field -> field.setName("numb"));
        assertInvalid("field renamed", TestRecordsWithHeaderProto.getDescriptor(), updatedFile);
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field renamed", metaData1, metaData2);
    }

    @Test
    public void nestedTypeChangesFieldType() {
        FileDescriptor updatedFile = mutateField("HeaderRecord", "num", TestRecordsWithHeaderProto.getDescriptor(),
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32));
        assertInvalid("field type changed", TestRecordsWithHeaderProto.getDescriptor(), updatedFile);
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        metaDataBuilder.getRecordType("MyRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("path", "rec_no")));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("field type changed", metaData1, metaData2);
    }

    @Test
    public void nestedTypesMerged() {
        validator.validateUnion(TestUnmergedNestedTypesProto.RecordTypeUnion.getDescriptor(), TestMergedNestedTypesProto.RecordTypeUnion.getDescriptor());

        FileDescriptor updatedMergedFile = mutateField("OneTrueNested", "b", TestMergedNestedTypesProto.getDescriptor(),
                field -> field.setName("c"));
        assertInvalid("field renamed", TestUnmergedNestedTypesProto.getDescriptor(), updatedMergedFile);
    }

    @Test
    public void nestedTypesSplit() {
        validator.validateUnion(TestMergedNestedTypesProto.RecordTypeUnion.getDescriptor(), TestSplitNestedTypesProto.RecordTypeUnion.getDescriptor());

        FileDescriptor updatedSplitFile = mutateField("NestedB", "b", TestSplitNestedTypesProto.getDescriptor(),
                field -> field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES));
        assertInvalid("field type changed", TestUnmergedNestedTypesProto.getDescriptor(), updatedSplitFile);
    }

    @Test
    public void fieldLabelChanged() {
        FileDescriptor oldFile = TestRecords1Proto.getDescriptor();
        List<DescriptorProtos.FieldDescriptorProto.Label> labels = Arrays.asList(
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED
        );
        for (int i = 0; i < labels.size(); i++) {
            final int itr = i;
            final DescriptorProtos.FieldDescriptorProto.Label label = labels.get(itr);
            final String labelText = label.name().substring(label.name().indexOf('_') + 1).toLowerCase(Locale.ROOT);
            final String errMsg = labelText + " field is no longer " + labelText;
            FileDescriptor updatedFile = mutateField("MySimpleRecord", "str_value_indexed", oldFile,
                    field -> field.setLabel(labels.get((itr + 1) % labels.size())));
            assertInvalid(errMsg, oldFile, updatedFile);

            oldFile = updatedFile;
        }
    }

    @Test
    public void addRequiredField() {
        FileDescriptor updatedFile = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("MySimpleRecord")) {
                        message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("new_int_field")
                                .setNumber(message.getFieldList().stream().mapToInt(DescriptorProtos.FieldDescriptorProto::getNumber).max().getAsInt() + 1)
                        );
                    }
                })
        );
        assertInvalid("required field added to record type", TestRecords1Proto.getDescriptor(), updatedFile);
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("required field added to record type", metaData1, metaData2);
    }

    @Test
    public void dropType() {
        FileDescriptor updatedFile = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        message.removeField(1);
                    }
                })
        );
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
    public void addNewPlaceInUnionDescriptor() {
        // Add a new field to the union descriptor that points to an existing record; leave the old one
        FileDescriptor updatedFile = mutateFile(fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
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
                    }
                })
        );
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertEquals(1066, metaData2.getUnionFieldForRecordType(metaData2.getRecordType("MySimpleRecord")).getNumber());
        validator.validate(metaData1, metaData2);
        assertEquals(metaData1.getRecordType("MySimpleRecord").getRecordTypeKey(), metaData2.getRecordType("MySimpleRecord").getRecordTypeKey());

        // Add a new field that points to an existing record but put it in a lower position in the union which makes the record type key change
        updatedFile = mutateFile(updatedFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        message.removeField(0);
                    }
                })
        );
        RecordMetaData metaData3 = RecordMetaData.build(updatedFile);
        updatedFile = mutateFile(updatedFile, fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                        message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("MySimpleRecord")
                                .setName("_MyOtherSimpleRecord")
                                .setNumber(800));
                    }
                })
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
    public void newTypeWithoutSinceVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = addNewRecordType(metaData1, RecordMetaDataProto.RecordType.Builder::clearSinceVersion);
        assertInvalid("new record type is missing since version", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowNoSinceVersion(true)
                .build();
        laxerValidator.validate(metaData1, metaData2);
    }

    @Test
    public void newTypeWithOlderSinceVersion() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = addNewRecordType(metaData1, protoBuilder -> protoBuilder.setSinceVersion(metaData1.getVersion() - 1));
        assertInvalid("new record type has since version older than old meta-data", metaData1, metaData2);

        RecordMetaData metaData3 = addNewRecordType(metaData1, protoBuilder -> protoBuilder.setSinceVersion(metaData1.getVersion()));
        assertInvalid("new record type has since version older than old meta-data", metaData1, metaData3);
    }

    @Test
    public void recordTypeKeyChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData1.toProto().toBuilder()
                .setVersion(metaData1.getVersion() + 1);
        protoBuilder.getRecordTypesBuilder(0)
                .setExplicitKey(RecordMetaDataProto.Value.newBuilder()
                    .setStringValue("new_key"));
        RecordMetaData metaData2 = RecordMetaData.build(protoBuilder.build());
        assertInvalid("record type key changed", metaData1, metaData2);
    }

    // Former index tests

    @Test
    public void removeFormerIndex() {
        RecordMetaDataBuilder metaData1Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData1Builder.removeIndex("MySimpleRecord$str_value_indexed");
        RecordMetaData metaData1 = metaData1Builder.getRecordMetaData();
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder().setVersion(metaData1.getVersion() + 1).clearFormerIndexes().build()
        );
        assertInvalid("former index removed", metaData1, metaData2);
    }

    @Test
    public void changeFormerIndexVersion() {
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
    public void changeFormerIndexName() {
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
    public void formerIndexFromThePast() {
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
    public void formerIndexWithoutExistingIndex() {
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
    public void indexUsedWhereFormerIndexWas() {
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
    public void removeIndexAndChangeName() {
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
        assertInvalid("former index has different name", metaData1, metaData3);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowMissingFormerIndexNames(true)
                .build();
        laxerValidator.validate(metaData1, metaData3);
    }

    @Test
    public void removeIndexAndDropAddedVersion() {
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
        assertInvalid("former index reports added version older than replacing index", metaData1, metaData2);

        // With the option set, it should validate
        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowOlderFormerIndexAddedVerions(true)
                .build();
        laxerValidator.validate(metaData1, metaData2);
    }

    /**
     * This test is supposed to validate that the "default" way of removing an index actually updates fields
     * in a safe way. As such, it is more to validate the methods on a RecordMetaDataBuilder that mutate version
     * information than the evolution validator.
     */
    @Test
    public void defaultIndexRemovalPath() {
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

    @Nonnull
    private RecordMetaDataProto.Index changeOption(@Nonnull RecordMetaDataProto.Index indexProto, @Nonnull String key, @Nullable String value) {
        RecordMetaDataProto.Index.Builder builder = indexProto.toBuilder();
        boolean found = false;
        for (int i = 0; i < builder.getOptionsCount(); i++) {
            final RecordMetaDataProto.Index.Option option = builder.getOptions(i);
            if (key.equals(option.getKey())) {
                if (value == null) {
                    builder.removeOptions(i);
                } else {
                    builder.setOptions(i, RecordMetaDataProto.Index.Option.newBuilder().setKey(key).setValue(value));
                }
                found = true;
                break;
            }
        }
        if (!found && value != null) {
            builder.addOptions(RecordMetaDataProto.Index.Option.newBuilder().setKey(key).setValue(value));
        }
        return builder.build();
    }

    @Nonnull
    private RecordMetaDataProto.Index makeUnique(@Nonnull RecordMetaDataProto.Index indexProto) {
        return changeOption(indexProto, IndexOptions.UNIQUE_OPTION, "true");
    }

    @Nonnull
    private RecordMetaDataProto.Index clearOptions(@Nonnull RecordMetaDataProto.Index indexProto) {
        return indexProto.toBuilder().clearOptions().build();
    }

    @Nonnull
    private RecordMetaData replaceIndex(@Nonnull RecordMetaData metaData, @Nonnull String indexName, UnaryOperator<RecordMetaDataProto.Index> indexReplacement) {
        RecordMetaDataProto.MetaData metaDataProto = metaData.toProto();
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaDataProto.toBuilder();
        metaDataProtoBuilder.setVersion(metaData.getVersion() + 1);
        for (int i = 0; i < metaDataProto.getIndexesCount(); i ++) {
            RecordMetaDataProto.Index indexProto = metaDataProto.getIndexes(i);
            if (indexProto.getName().equals(indexName)) {
                metaDataProtoBuilder.setIndexes(i, indexReplacement.apply(indexProto));
            }
        }
        return RecordMetaData.build(metaDataProtoBuilder.build());
    }

    @Test
    public void silentlyRemoveIndex() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = RecordMetaData.build(
                metaData1.toProto().toBuilder().setVersion(metaData1.getVersion() + 1).removeIndexes(0).build()
        );
        assertInvalid("index missing in new meta-data", metaData1, metaData2);
    }

    @Test
    public void newIndexFromThePast() {
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
    public void indexSubspaceKeyChanged() {
        // The index subspace key is the thing that determines whether an index is even there, so changing it
        // is identical to removing the index
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setSubspaceKey(ByteString.copyFrom(Tuple.from("dummy_key").pack())).build()
        );
        assertInvalid("index missing in new meta-data", metaData1, metaData2);
    }

    @Test
    public void indexNameChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder()
                        .setSubspaceKey(ByteString.copyFrom(Tuple.from("MySimpleRecord$str_value_indexed").pack()))
                        .setName("a_different_name")
                        .build()
        );
        assertInvalid("index name changed", metaData1, metaData2);
    }

    @Test
    public void indexAddedVersionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setAddedVersion(metaData1.getVersion() + 1).setLastModifiedVersion(metaData1.getVersion() + 1).build()
        );
        assertInvalid("new index added version does not match old index added version", metaData1, metaData2);

        metaData2 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setAddedVersion(indexProto.getAddedVersion() - 1).build()
        );
        assertInvalid("new index added version does not match old index added version", metaData1, metaData2);
    }

    @Test
    public void indexLastModifiedVersionTooOld() {
        RecordMetaData metaData1 = replaceIndex(RecordMetaData.build(TestRecords1Proto.getDescriptor()), "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setLastModifiedVersion(2).build()
        );
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setLastModifiedVersion(1).build()
        );
        assertInvalid("old index has last-modified version newer than new index", metaData1, metaData2);
    }

    @Test
    public void indexLastModifiedVersionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setLastModifiedVersion(metaData1.getVersion() + 1).build()
        );
        assertInvalid("last modified version of index changed", metaData1, metaData2);

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        laxerValidator.validate(metaData1, metaData2);
    }

    private void validateIndexMutation(@Nonnull String errMsg, @Nonnull RecordMetaData metaData1, @Nonnull String indexName, UnaryOperator<RecordMetaDataProto.Index> indexReplacement) {
        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        RecordMetaData metaData2 = replaceIndex(metaData1, indexName, indexReplacement);
        assertInvalid(errMsg, metaData1, metaData2);
        assertInvalid(errMsg, laxerValidator, metaData1, metaData2);

        // Allow the change if and only if the last modified version is updated and the option allowing rebuilds is set
        RecordMetaData metaData3 = replaceIndex(metaData2, indexName, indexProto ->
                indexProto.toBuilder().setLastModifiedVersion(metaData2.getVersion()).build()
        );
        assertInvalid("last modified version of index changed", metaData1, metaData3);
        laxerValidator.validate(metaData1, metaData3);
    }

    @Test
    public void indexTypeChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index type changed", metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setType(IndexTypes.RANK).build()
        );
    }

    @Test
    public void indexKeyExpressionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index key expression changed", metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                indexProto.toBuilder().setRootExpression(Key.Expressions.field("num_value_2").toKeyExpression()).build()
        );
    }

    @Test
    public void indexRecordTypeRemoved() {
        final String indexName = "simple&other$num_value_2";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"), metaDataBuilder.getRecordType("MyOtherRecord")),
                new Index(indexName, "num_value_2"));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        validateIndexMutation("new index removes record type", metaData1, indexName, indexProto ->
                indexProto.toBuilder().clearRecordType().addRecordType("MySimpleRecord").build()
        );
    }

    @Test
    public void indexRecordTypeAdded() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("new index adds record type that is not newer than old meta-data", metaData1, "MySimpleRecord$num_value_3_indexed", indexProto ->
                indexProto.toBuilder().addRecordType("MyOtherRecord").build()
        );

        // Add NewRecord as a record type to the existing meta-data and index and validate that this change is okay
        // because the new record type is newer the old meta-data version.
        RecordMetaData tempMetaData = addNewRecordType(metaData1);
        RecordMetaData metaData2 = replaceIndex(tempMetaData, "MySimpleRecord$num_value_3_indexed", indexProto ->
                indexProto.toBuilder().addRecordType("NewRecord").build());
        validator.validate(metaData1, metaData2); // valid if type and index change happen together
        assertInvalid("new index adds record type that is not newer than old meta-data", tempMetaData, metaData2);
    }

    @Test
    public void indexPrimaryKeyComponentsChanged() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "rec_no", "rec_no");
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getIndex("rec_no").hasPrimaryKeyComponentPositions(), is(true));

        RecordMetaData tempMetaData = addNewRecordType(metaData1);
        RecordMetaData metaData2 = replaceIndex(tempMetaData, "rec_no", indexProto ->
                indexProto.toBuilder().addRecordType("NewRecord").build());
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
    public void addRecordTypeWithUniversalIndex() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(new Index("rec_no", "rec_no"));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        assertNotNull(metaData1.getUniversalIndex("rec_no"));

        // Keep the index universal
        RecordMetaData metaData2 = addNewRecordType(metaData1);
        assertNotNull(metaData2.getUniversalIndex("rec_no"));
        validator.validate(metaData1, metaData2);

        // Make the index a multi-type index on the original record types
        RecordMetaData metaData3 = replaceIndex(metaData2, "rec_no", indexProto ->
                indexProto.toBuilder().addRecordType("MySimpleRecord").addRecordType("MyOtherRecord").build());
        MetaDataException e = assertThrows(MetaDataException.class, () -> metaData3.getUniversalIndex("rec_no"));
        assertThat(e.getMessage(), containsString("Index rec_no not defined"));
        assertInvalid("new index removes record type", metaData2, metaData3);
        validator.validate(metaData1, metaData3);
    }

    @Test
    public void uniquenessConstraintChanged() {
        // Adding a uniqueness constraint should throw an error
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index adds uniqueness constraint", metaData1, "MySimpleRecord$str_value_indexed", this::makeUnique);

        // Removing the uniqueness constraint is fine
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", this::makeUnique);
        RecordMetaData metaData3 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        validator.validate(metaData2, metaData3);
        RecordMetaData metaData4 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed", indexProto -> changeOption(indexProto, IndexOptions.UNIQUE_OPTION, "false"));
        validator.validate(metaData2, metaData4);
    }

    @Test
    public void allowedForQueriesChanged() {
        // Changing this option is always fine
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.ALLOWED_FOR_QUERY_OPTION, "false"));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.ALLOWED_FOR_QUERY_OPTION, "true"));
        validator.validate(metaData2, metaData3);
        RecordMetaData metaData4 = replaceIndex(metaData3, "MySimpleRecord$str_value_indexed", this::clearOptions);
        validator.validate(metaData3, metaData4);
    }

    @Test
    public void changeReplacedByIndex() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.REPLACED_BY_OPTION_PREFIX, "MySimpleRecord$num_value_3_indexed"));
        assertEquals(Collections.singletonList("MySimpleRecord$num_value_3_indexed"),
                metaData2.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        assertEquals(Collections.emptyList(),
                metaData3.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData2, metaData3);
    }

    @Test
    public void changeReplacedByIndexSet() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed", indexProto ->
                changeOption(changeOption(indexProto, IndexOptions.REPLACED_BY_OPTION_PREFIX + "_0", "MySimpleRecord$num_value_3_indexed"),
                        IndexOptions.REPLACED_BY_OPTION_PREFIX + "_1", "MySimpleRecord$num_value_unique")
        );
        assertThat(metaData2.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames(),
                containsInAnyOrder("MySimpleRecord$num_value_3_indexed", "MySimpleRecord$num_value_unique"));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed", this::clearOptions);
        assertEquals(Collections.emptyList(),
                metaData3.getIndex("MySimpleRecord$str_value_indexed").getReplacedByIndexNames());
        validator.validate(metaData2, metaData3);
    }

    @Test
    public void unknownOptionChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        validateIndexMutation("index option changed", metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, "dummyOption", "dummyValue"));
        RecordMetaData metaData2 = replaceIndex(metaData1, "MySimpleRecord$str_value_indexed",
                indexProto -> makeUnique(changeOption(indexProto, "dummyOption", "dummyValue")));
        RecordMetaData metaData3 = replaceIndex(metaData2, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, IndexOptions.UNIQUE_OPTION, null));
        validator.validate(metaData2, metaData3);
        validateIndexMutation("index option changed", metaData3, "MySimpleRecord$str_value_indexed",
                indexProto -> changeOption(indexProto, "dummyOption", "dummyValue2"));
    }

    @Test
    public void rankLevelsChanged() {
        final String indexName = "MySimpleRecord$rank(num_value_2)";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index(indexName, Key.Expressions.field("num_value_2").ungrouped(), IndexTypes.RANK));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        validateIndexMutation("rank levels changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "4"));
        validateIndexMutation("rank levels changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "" + RankedSet.MAX_LEVELS));

        // Setting the default explicitly is fine
        RecordMetaData metaData2 = replaceIndex(metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.RANK_NLEVELS, "" + RankedSet.DEFAULT_LEVELS));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = replaceIndex(metaData2, indexName, this::clearOptions);
        validator.validate(metaData2, metaData3);
    }

    @Test
    public void textOptionsChanged() {
        final String indexName = "MySimpleRecord$text(str_value_indexed)";
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index(indexName, Key.Expressions.field("str_value_indexed"), IndexTypes.TEXT));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        validateIndexMutation("text tokenizer changed", metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        // Setting the default explicitly is fine
        RecordMetaData metaData2 = replaceIndex(metaData1, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, DefaultTextTokenizer.NAME));
        validator.validate(metaData1, metaData2);
        RecordMetaData metaData3 = replaceIndex(metaData2, indexName, this::clearOptions);
        validator.validate(metaData2, metaData3);

        // Increasing the tokenizer version is fine, but decreasing it is not
        RecordMetaData metaData4 = replaceIndex(metaData3, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME));
        RecordMetaData metaData5 = replaceIndex(metaData4, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + TextTokenizer.GLOBAL_MIN_VERSION));
        validator.validate(metaData4, metaData5);
        RecordMetaData metaData6 = replaceIndex(metaData5, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + (TextTokenizer.GLOBAL_MIN_VERSION + 1)));
        validator.validate(metaData5, metaData6);
        validateIndexMutation("text tokenizer version downgraded", metaData6, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "" + TextTokenizer.GLOBAL_MIN_VERSION));

        // Changing whether aggressive conflict ranges are allowed is safe
        RecordMetaData metaData7 = replaceIndex(metaData6, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "true"));
        validator.validate(metaData6, metaData7);
        RecordMetaData metaData8 = replaceIndex(metaData7, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "false"));
        validator.validate(metaData7, metaData8);

        // Changing whether position lists are omitted is safe
        RecordMetaData metaData9 = replaceIndex(metaData8, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_OMIT_POSITIONS_OPTION, "true"));
        validator.validate(metaData8, metaData9);
        RecordMetaData metaData10 = replaceIndex(metaData9, indexName,
                indexProto -> changeOption(indexProto, IndexOptions.TEXT_OMIT_POSITIONS_OPTION, "false"));
        validator.validate(metaData9, metaData10);
    }
}
