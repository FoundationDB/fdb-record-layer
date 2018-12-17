/*
 * DynamicMessageRecordSerializerTest.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.UnaryOperator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link DynamicMessageRecordSerializer}.
 */
public class DynamicMessageRecordSerializerTest {
    @Nonnull private static RecordMetaData metaData;
    @Nonnull private static DynamicMessageRecordSerializer serializer;

    @BeforeAll
    @SuppressWarnings("unchecked")
    public static void setUpMetaData() {
        metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        serializer = (DynamicMessageRecordSerializer)DynamicMessageRecordSerializer.instance();
    }

    private Descriptors.FileDescriptor buildRecordDescriptor(@Nonnull DescriptorProtos.FileDescriptorProto proto) throws Descriptors.DescriptorValidationException {
        final Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[metaData.getRecordsDescriptor().getDependencies().size()];
        metaData.getRecordsDescriptor().getDependencies().toArray(dependencies);
        return Descriptors.FileDescriptor.buildFrom(proto, dependencies);
    }

    private Descriptors.Descriptor mutateUnionDescriptor(@Nonnull UnaryOperator<DescriptorProtos.DescriptorProto> mutation) throws Descriptors.DescriptorValidationException {
        final Descriptors.Descriptor oldUnionDescriptor = metaData.getUnionDescriptor();
        final DescriptorProtos.DescriptorProto oldUnionDescriptorProto = oldUnionDescriptor.toProto();
        final DescriptorProtos.FileDescriptorProto fileDescriptorProto = metaData.getRecordsDescriptor().toProto();
        int unionDescriptorIndex = fileDescriptorProto.getMessageTypeList().indexOf(oldUnionDescriptorProto);
        final DescriptorProtos.FileDescriptorProto newFileDescriptorProto = metaData.getRecordsDescriptor().toProto().toBuilder()
                .setMessageType(unionDescriptorIndex, mutation.apply(oldUnionDescriptorProto))
                .build();
        return buildRecordDescriptor(newFileDescriptorProto).findMessageTypeByName(oldUnionDescriptor.getName());
    }

    private Descriptors.Descriptor addFieldToUnionDescriptor(@Nonnull String name, @Nonnull DescriptorProtos.FieldDescriptorProto.Type type) throws Descriptors.DescriptorValidationException {
        final int fieldNumber = metaData.getUnionDescriptor().getFields().stream()
                .mapToInt(Descriptors.FieldDescriptor::getNumber)
                .max()
                .orElse(0) + 1;
        return mutateUnionDescriptor(oldUnionDescriptorProto ->
                oldUnionDescriptorProto.toBuilder()
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(name)
                        .setType(type)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setNumber(fieldNumber)
                        .build()
                )
                .build()
        );
    }

    private Descriptors.Descriptor removeFieldFromUnionDescriptor(@Nonnull String name) throws Descriptors.DescriptorValidationException {
        final int fieldIndex = metaData.getUnionDescriptor().findFieldByName(name).getIndex();
        return mutateUnionDescriptor(oldUnionDescriptorProto ->
                oldUnionDescriptorProto.toBuilder()
                .removeField(fieldIndex)
                .build()
        );
    }

    @Test
    public void deserializeUnionWithUnknownFields() throws Descriptors.DescriptorValidationException {
        // Add a field to the union descriptor message so as
        // to make it possible to read an unknown field.
        final Descriptors.Descriptor biggerUnionDescriptor = addFieldToUnionDescriptor("dummy_field", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
        final DynamicMessage message1 = DynamicMessage.newBuilder(biggerUnionDescriptor)
                .setField(biggerUnionDescriptor.findFieldByName("_MySimpleRecord"), TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build())
                .setField(biggerUnionDescriptor.findFieldByName("dummy_field"), Boolean.TRUE)
                .build();
        RecordSerializationException ex1 = assertThrows(RecordSerializationException.class,
                () -> serializer.deserializeUnion(metaData.getUnionDescriptor(), Tuple.from(1066L), message1.toByteArray(), metaData.getVersion()));
        assertThat(ex1.getMessage(), containsString("Could not deserialize union message"));
        assertThat(ex1.getMessage(), containsString("there are unknown fields"));
        assertThat((Collection<?>)ex1.getLogInfo().get("unknownFields"), not(empty()));

        // Remove a field from the union descriptor and set it.
        final Descriptors.Descriptor smallerUnionDescriptor = removeFieldFromUnionDescriptor("_MySimpleRecord");
        final Message message2 = TestRecords1Proto.RecordTypeUnion.newBuilder()
                .setMySimpleRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build())
                .build();
        RecordSerializationException ex2 = assertThrows(RecordSerializationException.class,
                () -> serializer.deserializeUnion(smallerUnionDescriptor, Tuple.from(1066L), message2.toByteArray(), metaData.getVersion()));
        assertThat(ex2.getMessage(), containsString("Could not deserialize union message"));
        assertThat(ex2.getMessage(), containsString("there are unknown fields"));
        assertThat((Collection<?>)ex2.getLogInfo().get("unknownFields"), not(empty()));
    }

    @Test
    public void deserializeUnionWithMultipleFields() {
        final Message message = TestRecords1Proto.RecordTypeUnion.newBuilder()
                .setMySimpleRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build())
                .setMyOtherRecord(TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(1413L).build())
                .build();
        RecordSerializationException ex = assertThrows(RecordSerializationException.class,
                () -> serializer.deserializeUnion(metaData.getUnionDescriptor(), Tuple.from(1066L), message.toByteArray(), metaData.getVersion()));
        assertThat(ex.getMessage(), containsString("Could not deserialize union message"));
        assertThat(ex.getMessage(), containsString("there are extra known fields"));
        assertThat((Collection<?>)ex.getLogInfo().get("fields"), not(empty()));
    }

    @Test
    public void deserializeUnionWithNoFields() {
        final Message message = TestRecords1Proto.RecordTypeUnion.getDefaultInstance();
        RecordSerializationException ex = assertThrows(RecordSerializationException.class,
                () -> serializer.deserializeUnion(metaData.getUnionDescriptor(), Tuple.from(1066L), message.toByteArray(), metaData.getVersion()));
        assertThat(ex.getMessage(), containsString("Could not deserialize union message"));
        assertThat(ex.getMessage(), containsString("there are no fields"));
        assertThat((Collection<?>)ex.getLogInfo().get("fields"), empty());
    }
}
