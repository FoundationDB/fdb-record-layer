/*
 * ProtobufDdlUtilTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.ddl;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Types;
import java.util.stream.Stream;

class ProtobufDdlUtilTest {

    public static Stream<DescriptorProtos.FieldDescriptorProto> fieldDescriptors() {
        return Stream.of(
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(1).setName("aInt").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(2).setName("aLong").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(3).setName("aFloat").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(4).setName("aDouble").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(5).setName("aString").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(6).setName("aBoolean").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(7).setName("aBytes").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES).build(),
                //                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(8).setName("aMessage").setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName("aMessageType").build(),

                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(9).setName("aRepeatedInt").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(10).setName("aRepeatedLong").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(11).setName("aRepeatedFloat").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(12).setName("aRepeatedDouble").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(13).setName("aRepeatedString").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(14).setName("aRepeatedBoolean").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL).build(),
                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(15).setName("aRepeatedBytes").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES).build()
        //                DescriptorProtos.FieldDescriptorProto.newBuilder().setNumber(16).setName("aRepeatedMessage").setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName("aMessageType").build()
        );
    }

    public static Stream<Arguments> fieldProtos() {
        return fieldDescriptors().map(Arguments::of);
    }

    public static Stream<Arguments> fields() {
        return fieldDescriptors().map(fdProto -> {
            try {
                DescriptorProtos.DescriptorProto dp = DescriptorProtos.DescriptorProto.newBuilder().addField(fdProto).setName("testDescriptor").build();
                DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(dp).build();
                Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[]{});
                return file.findMessageTypeByName("testDescriptor").findFieldByName(fdProto.getName());
            } catch (Descriptors.DescriptorValidationException e) {
                throw new RuntimeException(e);
            }
        }).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("fieldProtos")
    void getTypeNameFromProto(DescriptorProtos.FieldDescriptorProto fieldProto) {
        final String typeName = ProtobufDdlUtil.getTypeName(fieldProto);
        if (fieldProto.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED) {
            Assertions.assertThat(typeName).containsIgnoringCase("array");
        } else {
            Assertions.assertThat(typeName).doesNotContainIgnoringCase("array");
        }
        switch (fieldProto.getType()) {
            case TYPE_FLOAT:
                Assertions.assertThat(typeName).containsIgnoringCase("float");
                break;
            case TYPE_DOUBLE:
                Assertions.assertThat(typeName).containsIgnoringCase("double");
                break;
            case TYPE_INT32:
                Assertions.assertThat(typeName).containsIgnoringCase("integer");
                break;
            case TYPE_INT64:
                Assertions.assertThat(typeName).containsIgnoringCase("bigint");
                break;
            case TYPE_BOOL:
                Assertions.assertThat(typeName).containsIgnoringCase("boolean");
                break;
            case TYPE_STRING:
                Assertions.assertThat(typeName).containsIgnoringCase("string");
                break;
            case TYPE_MESSAGE:
                Assertions.assertThat(typeName).containsIgnoringCase(fieldProto.getTypeName());
                break;
            case TYPE_BYTES:
                Assertions.assertThat(typeName).containsIgnoringCase("bytes");
                break;
            default:
                Assertions.fail("Unexpected field type: <" + fieldProto.getType() + ">");
        }
    }

    @ParameterizedTest
    @MethodSource("fields")
    void getTypeNameFromDescriptor(Descriptors.FieldDescriptor field) {
        Assumptions.assumeThat(field.getJavaType()).isNotEqualTo(Descriptors.FieldDescriptor.JavaType.MESSAGE);
        final String typeName = ProtobufDdlUtil.getTypeName(field);
        if (field.isRepeated()) {
            Assertions.assertThat(typeName).containsIgnoringCase("array");
        } else {
            Assertions.assertThat(typeName).doesNotContainIgnoringCase("array");
        }
        switch (field.getJavaType()) {
            case FLOAT:
                Assertions.assertThat(typeName).containsIgnoringCase("float");
                break;
            case DOUBLE:
                Assertions.assertThat(typeName).containsIgnoringCase("double");
                break;
            case INT:
                Assertions.assertThat(typeName).containsIgnoringCase("integer");
                break;
            case LONG:
                Assertions.assertThat(typeName).containsIgnoringCase("bigint");
                break;
            case BOOLEAN:
                Assertions.assertThat(typeName).containsIgnoringCase("boolean");
                break;
            case STRING:
                Assertions.assertThat(typeName).containsIgnoringCase("string");
                break;
            case BYTE_STRING:
                Assertions.assertThat(typeName).containsIgnoringCase("bytes");
                break;
            default:
                Assertions.fail("Unexpected java type: <" + field.getJavaType() + ">");
        }
    }

    @ParameterizedTest
    @MethodSource("fields")
    void getSqlTypeFromDescriptor(Descriptors.FieldDescriptor field) {
        Assumptions.assumeThat(field.getJavaType()).isNotEqualTo(Descriptors.FieldDescriptor.JavaType.MESSAGE);
        final int sqlType = ProtobufDdlUtil.getSqlType(field);
        if (field.isRepeated()) {
            Assertions.assertThat(sqlType).isEqualTo(Types.ARRAY);
        } else {
            Assertions.assertThat(sqlType).isNotEqualTo(Types.ARRAY);
            switch (field.getJavaType()) {
                case FLOAT:
                    Assertions.assertThat(sqlType).isEqualTo(Types.FLOAT);
                    break;
                case DOUBLE:
                    Assertions.assertThat(sqlType).isEqualTo(Types.DOUBLE);
                    break;
                case INT:
                    Assertions.assertThat(sqlType).isEqualTo(Types.INTEGER);
                    break;
                case LONG:
                    Assertions.assertThat(sqlType).isEqualTo(Types.BIGINT);
                    break;
                case BOOLEAN:
                    Assertions.assertThat(sqlType).isEqualTo(Types.BOOLEAN);
                    break;
                case STRING:
                    Assertions.assertThat(sqlType).isEqualTo(Types.VARCHAR);
                    break;
                case BYTE_STRING:
                    Assertions.assertThat(sqlType).isEqualTo(Types.VARBINARY);
                    break;
                default:
                    Assertions.fail("Unexpected sql type: <" + sqlType + ">");
            }
        }
    }
}
