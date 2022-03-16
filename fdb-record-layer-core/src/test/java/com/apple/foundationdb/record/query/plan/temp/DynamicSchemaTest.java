/*
 * DynamicSchemaTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Tests for {@link com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema}.
 */
class DynamicSchemaTest {

    static final int SEED = 42;
    static final int MAX_ALLOWED_DEPTH = 100;
    static final Random random = new Random(SEED);
    static int counter = 0;

    private static Type generateRandomType() {
        return generateRandomTypeInternal(0);
    }

    private static Type generateRandomStructuredType() {
        boolean isRecordType = random.nextBoolean();
        if (isRecordType) {
            return generateType(0, Type.TypeCode.RECORD);
        } else {
            return generateType(0, Type.TypeCode.ARRAY);
        }
    }

    private static Type generateRandomTypeInternal(int depth) {
        int booleanIndex = Type.TypeCode.valueOf("BOOLEAN").ordinal();
        int stringIndex = Type.TypeCode.valueOf("STRING").ordinal();
        int recordIndex = Type.TypeCode.valueOf("RECORD").ordinal();
        int lowerBound = booleanIndex;
        int upperBound = (depth >= MAX_ALLOWED_DEPTH ? stringIndex + 1 : recordIndex + 1) - lowerBound;
        int pick = random.nextInt(upperBound) + lowerBound;
        Type.TypeCode randomTypeCode = Type.TypeCode.values()[pick];
        return generateType(depth, randomTypeCode);
    }

    private static Type generateType(int depth, Type.TypeCode requestedTypeCode) {
        switch (requestedTypeCode) {
            case BOOLEAN: // fallthrough
            case BYTES: // fallthrough
            case DOUBLE: // fallthrough
            case FLOAT: // fallthrough
            case INT: // fallthrough
            case LONG: // fallthrough
            case STRING:
                return Type.primitiveType(requestedTypeCode, random.nextBoolean());
            case ARRAY:
                return new Type.Array(generateRandomTypeInternal(depth + 1));
            case RECORD:
                int numFields = random.nextInt(3) + 1;
                List<Type.Record.Field> fields = new ArrayList<>();
                for (int i = 0; i < numFields; ++i) {
                    fields.add(Type.Record.Field.of(generateRandomTypeInternal(depth + 1), Optional.of("random" + ++counter), Optional.empty()));
                }
                return Type.Record.fromFields(fields);
            case RELATION: // fallthrough
            case UNKNOWN: // fallthrough
            case ANY: // fallthrough
            default:
                throw new IllegalArgumentException("unexpected random type: " + requestedTypeCode);
        }
    }

    private static String generateRandomString() {
        return "str" + RandomStringUtils.randomAlphanumeric(10);
    }

    @Test
    void addPrimitiveTypeIsNotAllowed() {
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();
        try {
            builder.addType(generateType(0, Type.TypeCode.DOUBLE));
            Assertions.fail("expected an exception to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("unexpected primitive type " + Type.TypeCode.DOUBLE));
        }
    }

    @Test
    void createDynamicSchemaFromRecordTypeWorks() {
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();
        Type t = generateType(0, Type.TypeCode.RECORD);
        builder.addType(t);
        DynamicSchema actualSchema = builder.build();
        String typeName = actualSchema.getFileDescriptorSet().getFile(0).getMessageTypeList().get(0).getName();
        Descriptors.Descriptor actualDescriptor = actualSchema.getMessageDescriptor(typeName);
        Assertions.assertEquals(t.buildDescriptor(typeName), actualDescriptor.toProto());
    }

    @Test
    void createDynamicSchemaFromArrayTypeWorks() {
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();
        Type t = generateType(0, Type.TypeCode.ARRAY);
        builder.addType(t);
        DynamicSchema actualSchema = builder.build();
        String typeName = actualSchema.getFileDescriptorSet().getFile(0).getMessageTypeList().get(0).getName();
        Descriptors.Descriptor actualDescriptor = actualSchema.getMessageDescriptor(typeName);
        Assertions.assertEquals(t.buildDescriptor(typeName), actualDescriptor.toProto());
    }

    @Test
    void addSameTypeMultipleTimesShouldNotCreateMultipleMessages() {
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();
        Type t = generateRandomStructuredType();
        builder.addType(t);
        builder.addType(t);
        builder.addType(t);
        DynamicSchema actualSchema = builder.build();
        Assertions.assertEquals(1, actualSchema.getMessageTypes().size());
        String typeName = actualSchema.getMessageTypes().stream().findFirst().get();
        Descriptors.Descriptor actualDescriptor = actualSchema.getMessageDescriptor(typeName);
        Assertions.assertEquals(t.buildDescriptor(typeName), actualDescriptor.toProto());
    }
}
