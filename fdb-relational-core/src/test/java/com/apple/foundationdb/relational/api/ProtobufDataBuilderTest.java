/*
 * ProtobufDataBuilderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.JavaType;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.stream.Stream;

@SuppressWarnings("deprecation") // this is a test of a deprecated class, ProtobufDataBuilder, so will need to be here until we remove it
class ProtobufDataBuilderTest {

    private static Stream<Arguments> possibleFields() {
        return Stream.of(
                fieldArgs("string", JavaType.STRING, null, false),
                fieldArgs("integer", JavaType.INT, null, false),
                fieldArgs("bigint", JavaType.LONG, null, false),
                fieldArgs("float", JavaType.FLOAT, null, false),
                fieldArgs("double", JavaType.DOUBLE, null, false)
        );
    }

    private static Stream<Arguments> invalidFields() {
        return Stream.of(
                invalidField("string", JavaType.STRING, null, false),
                invalidField("integer", JavaType.INT, null, false),
                invalidField("bigint", JavaType.LONG, null, false),
                invalidField("float", JavaType.FLOAT, null, false),
                invalidField("double", JavaType.DOUBLE, null, false)
        );
    }

    private static Arguments invalidField(String name, JavaType javaType, String messageType, boolean isRepeated) {
        DescriptorProtos.FieldDescriptorProto proto = newDescriptor(name, javaType, messageType, isRepeated);
        Object value;
        switch (javaType) {
            case INT:
                value = "hello";
                break;
            case LONG:
                value = Boolean.TRUE;
                break;
            case FLOAT:
                value = "float";
                break;
            case DOUBLE:
                value = "double-v";
                break;
            case BOOLEAN:
                value = 0d;
                break;
            case STRING:
                value = -0f;
                break;
            default:
                throw new UnsupportedOperationException("Don't have a test for these yet");
        }
        return Arguments.of(proto, value);
    }

    private static Arguments fieldArgs(String name, JavaType javaType, String messageType, boolean isRepeated) {
        DescriptorProtos.FieldDescriptorProto proto = newDescriptor(name, javaType, messageType, isRepeated);
        Object value;
        switch (javaType) {
            case INT:
                value = Integer.MIN_VALUE;
                break;
            case LONG:
                value = Long.MAX_VALUE;
                break;
            case FLOAT:
                value = -0f;
                break;
            case DOUBLE:
                value = 0d;
                break;
            case BOOLEAN:
                value = Boolean.FALSE;
                break;
            case STRING:
                value = "test";
                break;
            default:
                throw new UnsupportedOperationException("Don't have a test for these yet");
        }
        return Arguments.of(proto, value);
    }

    public static DescriptorProtos.FieldDescriptorProto newDescriptor(String name,
                                                                      JavaType javaType,
                                                                      String messageType,
                                                                      boolean isRepeated) {
        DescriptorProtos.FieldDescriptorProto.Builder proto = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name).setJsonName(name);
        switch (javaType) {
            case INT:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
                break;
            case LONG:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
                break;
            case FLOAT:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT);
                break;
            case DOUBLE:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
                break;
            case BOOLEAN:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
                break;
            case STRING:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
                break;
            case BYTE_STRING:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);
                break;
            case ENUM:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM).setTypeName(messageType);
                break;
            case MESSAGE:
                proto.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(messageType);
                break;
            default:
                throw new IllegalStateException("Unexpected java type " + javaType);
        }
        if (isRepeated) {
            proto.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        }
        proto.setNumber(1);

        return proto.build();
    }

    @ParameterizedTest
    @MethodSource("possibleFields")
    void worksForFieldTypes(DescriptorProtos.FieldDescriptorProto field, Object value) throws Descriptors.DescriptorValidationException, SQLException {
        DescriptorProtos.DescriptorProto descProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("test")
                .addField(field).build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build(), new Descriptors.FileDescriptor[]{});
        final Descriptors.Descriptor typeDesc = file.findMessageTypeByName("test");
        ProtobufDataBuilder dataBuilder = new ProtobufDataBuilder(typeDesc);
        dataBuilder.setField(field.getName(), value);
        final Message message = dataBuilder.build();
        final Object serializedField = message.getField(typeDesc.findFieldByName(field.getName()));
        Assertions.assertEquals(value, serializedField, "Incorrect set field!");
    }

    @Test
    void failsForMissingField() throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto descProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("test")
                .addField(newDescriptor("blah", JavaType.STRING, null, false)).build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build(), new Descriptors.FileDescriptor[]{});
        ProtobufDataBuilder dataBuilder = new ProtobufDataBuilder(file.findMessageTypeByName("test"));
        RelationalAssertions.assertThrowsSqlException(() -> dataBuilder.setField("noSuchField", "12"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void failsForNonRepeatingField() throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto descProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("test")
                .addField(newDescriptor("blah", JavaType.STRING, null, false)).build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build(), new Descriptors.FileDescriptor[]{});
        ProtobufDataBuilder dataBuilder = new ProtobufDataBuilder(file.findMessageTypeByName("test"));
        RelationalAssertions.assertThrowsSqlException(() -> dataBuilder.addRepeatedField("noSuchField", "12"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void failsWhenGettingNestedFieldWhichIsActuallyAPrimitive() throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto descProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("test")
                .addField(newDescriptor("blah", JavaType.STRING, null, false)).build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build(), new Descriptors.FileDescriptor[]{});
        ProtobufDataBuilder dataBuilder = new ProtobufDataBuilder(file.findMessageTypeByName("test"));
        RelationalAssertions.assertThrowsSqlException(() -> dataBuilder.getNestedMessageBuilder("blah"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void failWhenAddRepeatedField() throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto descProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("test")
                .addField(newDescriptor("field1", JavaType.STRING, null, false)).build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build(), new Descriptors.FileDescriptor[]{});
        ProtobufDataBuilder dataBuilder = new ProtobufDataBuilder(file.findMessageTypeByName("test"));
        RelationalAssertions.assertThrowsSqlException(() -> dataBuilder.addRepeatedField("field1", "foo"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("is not repeated");
    }
}
