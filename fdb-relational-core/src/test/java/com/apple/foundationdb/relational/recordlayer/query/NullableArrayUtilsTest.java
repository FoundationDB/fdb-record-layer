/*
 * NullableArrayUtilsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordKeyExpressionProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.relational.util.NullableArrayUtils;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NullableArrayUtilsTest {
    @Test
    void testGetRepeatedFieldName() {
        Assertions.assertEquals(NullableArrayUtils.getRepeatedFieldName(), NullableArrayUtils.getRepeatedFieldName());
    }

    @Test
    void testDescribesWrappedArray() throws Descriptors.DescriptorValidationException {
        Descriptors.FileDescriptor file = generateFileDescriptor();
        Descriptors.Descriptor stringListDescriptor = file.findMessageTypeByName("StringList");
        Descriptors.Descriptor userListDescriptor = file.findMessageTypeByName("UserList");
        Descriptors.Descriptor parentDescriptor = file.findMessageTypeByName("Parent");
        // nullable array, array element is of primitive type
        Assertions.assertTrue(NullableArrayUtils.isWrappedArrayDescriptor(stringListDescriptor));
        Assertions.assertTrue(NullableArrayUtils.isWrappedArrayDescriptor("stringListField", parentDescriptor));
        // nullable array, array element is of struct type
        Assertions.assertTrue(NullableArrayUtils.isWrappedArrayDescriptor(userListDescriptor));
        Assertions.assertTrue(NullableArrayUtils.isWrappedArrayDescriptor("userListField", parentDescriptor));
    }

    @Test
    void testWrapArray() throws Descriptors.DescriptorValidationException {
        Descriptors.FileDescriptor file = generateFileDescriptor();
        Descriptors.Descriptor parentDescriptor = file.findMessageTypeByName("Parent");
        // original field("stringListField", FAN_OUT), expected to become stringListField.values
        RecordKeyExpressionProto.KeyExpression original1 = RecordKeyExpressionProto.KeyExpression.newBuilder()
                .setField(RecordKeyExpressionProto.Field.newBuilder()
                        .setFieldName("stringListField")
                        .setFanType(RecordKeyExpressionProto.Field.FanType.FAN_OUT)
                        .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))
                .build();
        RecordKeyExpressionProto.KeyExpression expected1 = RecordKeyExpressionProto.KeyExpression.newBuilder()
                .setNesting(RecordKeyExpressionProto.Nesting.newBuilder()
                        .setParent(RecordKeyExpressionProto.Field.newBuilder()
                                .setFieldName("stringListField")
                                .setFanType(RecordKeyExpressionProto.Field.FanType.SCALAR)
                                .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))
                        .setChild(RecordKeyExpressionProto.KeyExpression.newBuilder()
                                .setField(RecordKeyExpressionProto.Field.newBuilder()
                                        .setFieldName(NullableArrayUtils.getRepeatedFieldName())
                                        .setFanType(RecordKeyExpressionProto.Field.FanType.FAN_OUT)
                                        .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))))
                .build();
        Assertions.assertEquals(expected1, NullableArrayUtils.wrapArray(original1, parentDescriptor, true));

        // userListField.name, expected to become userListField.values.name
        RecordKeyExpressionProto.KeyExpression original2 = RecordKeyExpressionProto.KeyExpression.newBuilder()
                .setNesting(RecordKeyExpressionProto.Nesting.newBuilder()
                        .setParent(RecordKeyExpressionProto.Field.newBuilder()
                                .setFieldName("userListField")
                                .setFanType(RecordKeyExpressionProto.Field.FanType.FAN_OUT)
                                .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))
                        .setChild(RecordKeyExpressionProto.KeyExpression.newBuilder()
                                .setField(RecordKeyExpressionProto.Field.newBuilder()
                                        .setFieldName("name")
                                        .setFanType(RecordKeyExpressionProto.Field.FanType.SCALAR)
                                        .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))))
                .build();
        RecordKeyExpressionProto.KeyExpression expected2 = RecordKeyExpressionProto.KeyExpression.newBuilder()
                .setNesting(RecordKeyExpressionProto.Nesting.newBuilder()
                        .setParent(RecordKeyExpressionProto.Field.newBuilder()
                                .setFieldName("userListField")
                                .setFanType(RecordKeyExpressionProto.Field.FanType.SCALAR)
                                .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))
                        .setChild(RecordKeyExpressionProto.KeyExpression.newBuilder()
                                .setNesting(RecordKeyExpressionProto.Nesting.newBuilder()
                                        .setParent(RecordKeyExpressionProto.Field.newBuilder()
                                                .setFieldName(NullableArrayUtils.getRepeatedFieldName())
                                                .setFanType(RecordKeyExpressionProto.Field.FanType.FAN_OUT)
                                                .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))
                                        .setChild(RecordKeyExpressionProto.KeyExpression.newBuilder()
                                                .setField(RecordKeyExpressionProto.Field.newBuilder()
                                                        .setFieldName("name")
                                                        .setFanType(RecordKeyExpressionProto.Field.FanType.SCALAR)
                                                        .setNullInterpretation(RecordKeyExpressionProto.Field.NullInterpretation.NOT_UNIQUE))))))
                .build();
        Assertions.assertEquals(expected2, NullableArrayUtils.wrapArray(original2, parentDescriptor, true));
    }

    private Descriptors.FileDescriptor generateFileDescriptor() throws Descriptors.DescriptorValidationException {
        // construct a test descriptor
        /*
        message Parent {
          StringList stringListField = 1;
          UserList userListField = 2;
        }
        message StringList {
          repeated string values = 1;
        }
        message UserList {
          repeated User values = 1;
        }
        message User {
          string name = 1;
        }
         */
        DescriptorProtos.DescriptorProto user = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("User")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("name")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();
        DescriptorProtos.DescriptorProto userList = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("UserList")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(NullableArrayUtils.getRepeatedFieldName())
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("User")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .build();
        DescriptorProtos.DescriptorProto stringList = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("StringList")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(NullableArrayUtils.getRepeatedFieldName())
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .build();
        DescriptorProtos.DescriptorProto parent = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Parent")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("stringListField")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("StringList")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("userListField")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("UserList")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();
        return Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto").addAllMessageType(List.of(stringList, userList, user, parent)).build(), new Descriptors.FileDescriptor[]{});
    }
}
