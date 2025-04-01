/*
 * MetaDataEvolutionValidatorTestV3.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.evolution.TestNestedProto3;
import com.apple.foundationdb.record.evolution.TestRecords1ImportedProto;
import com.apple.foundationdb.record.evolution.TestRecords3ProtoV3;
import com.apple.foundationdb.record.evolution.TestRecordsEnumProtoV3;
import com.apple.foundationdb.record.evolution.TestRecordsNestedProto2;
import com.apple.foundationdb.record.evolution.TestRecordsNestedProto3;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;


import javax.annotation.Nonnull;

import java.util.function.Consumer;

import static com.apple.foundationdb.record.metadata.MetaDataEvolutionValidatorTest.assertInvalid;
import static com.apple.foundationdb.record.metadata.MetaDataEvolutionValidatorTest.mutateFile;
import static com.apple.foundationdb.record.metadata.MetaDataEvolutionValidatorTest.replaceRecordsDescriptor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the {@link MetaDataEvolutionValidator} that require proto3.
 */
public class MetaDataEvolutionValidatorTestV3 {

    @Nonnull
    private static FileDescriptor mutateEnum(@Nonnull Consumer<DescriptorProtos.EnumDescriptorProto.Builder> enumMutation) {
        return mutateFile(TestRecordsEnumProtoV3.getDescriptor(), fileBuilder ->
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("MyShapeRecord")) {
                        message.getEnumTypeBuilderList().forEach(enumType -> {
                            if (enumType.getName().equals("Size")) {
                                enumMutation.accept(enumType);
                            }
                        });
                    }
                })
        );
    }

    /**
     * Enums are different with proto3 syntax. In particular, they require one of their values have value 0,
     * and they introduce an "UNKNOWN" value all of the time.
     */
    @Test
    public void enumChanged() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecordsEnumProtoV3.getDescriptor());

        FileDescriptor updatedFile = mutateEnum(enumType ->
                enumType.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                        .setName("X_LARGE")
                        .setNumber(4))
        );
        assertNotNull(updatedFile.findMessageTypeByName("MyShapeRecord").findEnumTypeByName("Size").findValueByName("X_LARGE"));
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        MetaDataEvolutionValidator.getDefaultInstance().validate(metaData1, metaData2);

        updatedFile = mutateEnum(enumType ->
                enumType.getValueBuilderList().forEach(enumValue -> {
                    if (enumValue.getName().equals("SMALL")) {
                        enumValue.setName("PETIT");
                    }
                })
        );
        assertNotNull(updatedFile.findMessageTypeByName("MyShapeRecord").findEnumTypeByName("Size").findValueByName("PETIT"));
        metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        MetaDataEvolutionValidator.getDefaultInstance().validate(metaData1, metaData2);

        updatedFile = mutateEnum(enumType ->
                enumType.removeValue(enumType.getValueCount() - 1)
        );
        assertNull(updatedFile.findMessageTypeByName("MyShapeRecord").findEnumTypeByName("Size").findValueByName("LARGE"));
        metaData2 = replaceRecordsDescriptor(metaData1, updatedFile);
        assertInvalid("enum removes value", metaData1, metaData2);
    }

    /**
     * Validate that if the syntax is changed, then the meta-data evolution is invalidated.
     */
    @Test
    public void proto2ToProto3() {
        assertInvalid("message descriptor proto syntax changed",
                TestRecords3Proto.UnionDescriptor.getDescriptor(), TestRecords3ProtoV3.UnionDescriptor.getDescriptor());
        assertInvalid("message descriptor proto syntax changed",
                TestRecords3ProtoV3.UnionDescriptor.getDescriptor(), TestRecords3Proto.UnionDescriptor.getDescriptor());

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
        metaDataBuilder.getRecordType("MyHierarchicalRecord").setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
        RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestRecords3ProtoV3.getDescriptor());
        assertInvalid("message descriptor proto syntax changed", metaData1, metaData2);
    }

    /**
     * Validate that if a nested field changes syntax, then the meta-data evolution is invalidated.
     */
    @Test
    public void nestedProto2ToProto3() {
        assertInvalid("message descriptor proto syntax changed",
                TestRecordsNestedProto2.getDescriptor(), TestRecordsNestedProto3.getDescriptor());
        assertInvalid("message descriptor proto syntax changed",
                TestRecordsNestedProto3.getDescriptor(), TestRecordsNestedProto2.getDescriptor());

        RecordMetaData metaData1 = RecordMetaData.build(TestRecordsNestedProto2.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestRecordsNestedProto3.getDescriptor(),
                protoBuilder -> protoBuilder.clearDependencies().addDependencies(TestNestedProto3.getDescriptor().toProto()));
        assertInvalid("message descriptor proto syntax changed", metaData1, metaData2);
    }

    /**
     * Validate that it is legal to change the records descriptor from proto2 to proto3 as long as all of the records
     * contained within that file are still the same syntax.
     */
    @SuppressWarnings("deprecation") // test focuses on checking the (deprecated) syntax field
    @Test
    public void onlyFileProto2ToProto3() throws InvalidProtocolBufferException {
        assertNotEquals(TestRecords1Proto.getDescriptor().toProto().getSyntax(), TestRecords1ImportedProto.getDescriptor().toProto().getSyntax());
        MetaDataEvolutionValidator.getDefaultInstance().validateUnion(
                TestRecords1Proto.RecordTypeUnion.getDescriptor(),
                TestRecords1ImportedProto.RecordTypeUnion.getDescriptor()
        );
        MetaDataEvolutionValidator.getDefaultInstance().validateUnion(
                TestRecords1ImportedProto.RecordTypeUnion.getDescriptor(),
                TestRecords1Proto.RecordTypeUnion.getDescriptor()
        );

        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaData metaData2 = replaceRecordsDescriptor(metaData1, TestRecords1ImportedProto.getDescriptor());
        MetaDataEvolutionValidator.getDefaultInstance().validate(metaData1, metaData2);

        // Validate that the nested proto2 records in the proto3 file have proper nullability semantics
        TestRecords1Proto.RecordTypeUnion unionRecordProto2 = TestRecords1Proto.RecordTypeUnion.newBuilder()
                .setMySimpleRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(0).setStrValueIndexed(""))
                .build();
        assertThat(unionRecordProto2.getMySimpleRecord().hasNumValue2(), is(false));
        assertThat(unionRecordProto2.getMySimpleRecord().hasStrValueIndexed(), is(true));

        TestRecords1ImportedProto.RecordTypeUnion unionRecordProto3 = TestRecords1ImportedProto.RecordTypeUnion.parseFrom(unionRecordProto2.toByteString());
        assertThat(unionRecordProto3.getMySimpleRecord().hasNumValue2(), is(false));
        assertThat(unionRecordProto3.getMySimpleRecord().hasStrValueIndexed(), is(true));

        final FieldDescriptor unionField = TestRecords1ImportedProto.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord");
        final FieldDescriptor numValue2Field = TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("num_value_2");
        final FieldDescriptor strValueIndexedField = TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("str_value_indexed");
        DynamicMessage dynamicUnionRecordProto3 = DynamicMessage.parseFrom(TestRecords1ImportedProto.RecordTypeUnion.getDescriptor(), unionRecordProto2.toByteString());
        Message dynamicSimpleRecord = (Message)dynamicUnionRecordProto3.getField(unionField);
        assertThat(dynamicSimpleRecord.hasField(numValue2Field), is(false));
        assertThat(dynamicSimpleRecord.hasField(strValueIndexedField), is(true));
    }
}
