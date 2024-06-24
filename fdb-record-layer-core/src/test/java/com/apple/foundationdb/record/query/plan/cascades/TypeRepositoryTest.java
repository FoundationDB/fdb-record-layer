/*
 * TypeRepositoryTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Tests for:
 *  <ul>
 *      <li>{@link TypeRepository}.</li>
 *      <li>{@link AbstractArrayConstructorValue} type hierarchy.</li>
 *      <li>{@link RecordConstructorValue}.</li>
 *  </ul>
 * Tests mainly target aspects of dynamic schema generation in these classes.
 */
@SuppressWarnings("ConstantConditions")
class TypeRepositoryTest {
    private static final int SEED = 0x4015e;
    private static final int MAX_ALLOWED_DEPTH = 10;
    private static final Random random = new Random(SEED);
    private static int counter = 0;

    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_NOT_NULLABLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT, false), 1);
    private static final LiteralValue<Integer> INT_NOT_NULLABLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT, false), 2);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");

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

    private static int countTypes(@Nonnull final Type type) {
        if (type.isPrimitive()) {
            return 0;
        }
        if (type instanceof Type.Array) {
            if (type.isNullable()) {
                return 1 + countTypes(((Type.Array)type).getElementType());
            } else {
                return countTypes(((Type.Array)type).getElementType());
            }
        }
        if (type instanceof Type.Record) {
            return 1 + ((Type.Record)type).getFields().stream().map(f -> countTypes(f.getFieldType())).reduce(0, Integer::sum);
        }
        throw new IllegalArgumentException("unexpected type " + type.getTypeCode().toString());
    }

    private static Type generateRandomTypeInternal(int depth) {
        Type.TypeCode randomTypeCode = generateRandomTypeCode(depth);
        return generateType(depth, randomTypeCode);
    }

    private static Type.TypeCode generateRandomTypeCode(int depth) {
        List<Type.TypeCode> choices = new ArrayList<>();
        for (Type.TypeCode typeCode : Type.TypeCode.values()) {
            if (typeCode != Type.TypeCode.UNKNOWN && typeCode != Type.TypeCode.ANY && typeCode != Type.TypeCode.NULL) {
                if (typeCode.isPrimitive()) {
                    choices.add(typeCode);
                } else if (depth < MAX_ALLOWED_DEPTH && (typeCode == Type.TypeCode.RECORD || typeCode == Type.TypeCode.ARRAY)) {
                    choices.add(typeCode);
                }
            }
        }
        int choice = random.nextInt(choices.size());
        return choices.get(choice);
    }

    private static Type generateType(int depth, Type.TypeCode requestedTypeCode) {
        switch (requestedTypeCode) {
            case BOOLEAN: // fallthrough
            case BYTES: // fallthrough
            case DOUBLE: // fallthrough
            case FLOAT: // fallthrough
            case INT: // fallthrough
            case LONG: // fallthrough
            case STRING: // fallthrough
            case VERSION:
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

    @Test
    void addPrimitiveTypeIsNotAllowed() {
        TypeRepository.Builder builder = TypeRepository.newBuilder();
        try {
            final var type = generateType(0, Type.TypeCode.DOUBLE);
            builder.addTypeIfNeeded(type);
            Assertions.assertTrue(builder.getTypeName(type).isEmpty());
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("unexpected type " + Type.TypeCode.DOUBLE));
        }
    }

    @Test
    void createTypeRepositoryFromRecordTypeWorks() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        final Type.Record parent = (Type.Record)generateType(0, Type.TypeCode.RECORD);
        final Type.Record child = (Type.Record)generateType(0, Type.TypeCode.RECORD);
        final List<Type.Record.Field> fields = new ArrayList<>(parent.getFields());
        fields.add(Type.Record.Field.of(child, Optional.of("nestedField")));
        final Type.Record t = Type.Record.fromFields(fields);
        builder.addTypeIfNeeded(t);
        final TypeRepository actualSchemaBefore = builder.build();
        Assertions.assertEquals(countTypes(t), actualSchemaBefore.getMessageTypes().size());
        // add record type explicitly, this should NOT cause the addition of a new descriptor.
        builder.addTypeIfNeeded(child);
        final TypeRepository actualSchemaAfter = builder.build();
        Assertions.assertEquals(actualSchemaAfter.getMessageTypes().size(), actualSchemaBefore.getMessageTypes().size());
    }

    @Test
    void createTypeRepositoryFromArrayTypeWorks() {
        final Type.Record child = (Type.Record)generateType(0, Type.TypeCode.RECORD);
        final Type.Array array = new Type.Array(child);
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        builder.addTypeIfNeeded(array);
        final TypeRepository actualSchemaBefore = builder.build();
        Assertions.assertEquals(countTypes(array), actualSchemaBefore.getMessageTypes().size());
        // add record type explicitly, this should NOT cause the addition of a new descriptor.
        builder.addTypeIfNeeded(child);
        final TypeRepository actualSchemaAfter = builder.build();
        Assertions.assertEquals(actualSchemaAfter.getMessageTypes().size(), actualSchemaBefore.getMessageTypes().size());
    }

    @Test
    void createTypeRepositoryFromNullableArrayTypeWorks() {
        final Type.Record child = (Type.Record)generateType(0, Type.TypeCode.RECORD);
        final Type.Array array = new Type.Array(true, child);
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        builder.addTypeIfNeeded(array);
        final TypeRepository actualSchemaBefore = builder.build();
        Assertions.assertEquals(countTypes(array), actualSchemaBefore.getMessageTypes().size());
        // add record type explicitly, this should NOT cause the addition of a new descriptor.
        builder.addTypeIfNeeded(child);
        final TypeRepository actualSchemaAfter = builder.build();
        Assertions.assertEquals(actualSchemaAfter.getMessageTypes().size(), actualSchemaBefore.getMessageTypes().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testArrayElementTypeIsNotNullable(boolean arrayNullable) {
        final Type.Record record1 = Type.Record.fromFields(List.of(Type.Record.Field.of(new Type.Array(arrayNullable, Type.primitiveType(Type.TypeCode.DOUBLE, true)), Optional.of("array_field"))));
        final Type.Record record2 = Type.Record.fromFields(List.of(Type.Record.Field.of(new Type.Array(arrayNullable, Type.primitiveType(Type.TypeCode.DOUBLE, false)), Optional.of("array_field"))));
        final TypeRepository typeRepository = TypeRepository.newBuilder()
                .addTypeIfNeeded(record1)
                .addTypeIfNeeded(record2)
                .build();
        for (var rec: List.of(record1, record2)) {
            final Type.Record deserialized = Type.Record.fromDescriptor(typeRepository.getMessageDescriptor(rec));
            Assertions.assertTrue(deserialized.getElementTypes().get(0) instanceof Type.Array);
            final Type elementType = ((Type.Array) deserialized.getElementTypes().get(0)).getElementType();
            Assertions.assertFalse(elementType.isNullable());
        }
    }

    @Test
    void addSameTypeMultipleTimesShouldNotCreateMultipleMessageTypes() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        final Type t = generateRandomStructuredType();
        builder.addTypeIfNeeded(t);
        builder.addTypeIfNeeded(t);
        builder.addTypeIfNeeded(t);
        final TypeRepository actualRepository = builder.build();
        Assertions.assertEquals(countTypes(t), actualRepository.getMessageTypes().size());
    }

    @Test
    void attemptToCreateArrayConstructorValueWithDifferentChildrenTypesFails() {
        Assertions.assertThrows(SemanticException.class, () -> new AbstractArrayConstructorValue.ArrayFn().encapsulate(List.of(INT_1, STRING_1 /*invalid*/)));
    }

    @Test
    void createLightArrayConstructorValueWorks() {
        final Typed value = new AbstractArrayConstructorValue.ArrayFn().encapsulate(List.of(INT_NOT_NULLABLE_1, INT_NOT_NULLABLE_2));
        Assertions.assertTrue(value instanceof AbstractArrayConstructorValue.LightArrayConstructorValue);
        final AbstractArrayConstructorValue.LightArrayConstructorValue arrayConstructorValue = (AbstractArrayConstructorValue.LightArrayConstructorValue)value;
        final Type resultType = arrayConstructorValue.getResultType();
        Assertions.assertEquals(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false)), resultType);
        final Object result = arrayConstructorValue.compileTimeEval(EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(arrayConstructorValue.getResultType()).build()));
        Assertions.assertTrue(result instanceof List);
        final List<?> list = (List<?>)result;
        Assertions.assertEquals(2, list.size());
        Assertions.assertEquals(1, list.get(0));
        Assertions.assertEquals(2, list.get(1));
    }

    @Test
    void createRecordTypeConstructorWorks() {
        final Typed value = new RecordConstructorValue.RecordFn().encapsulate(List.of(STRING_1, INT_2, FLOAT_1));
        Assertions.assertTrue(value instanceof RecordConstructorValue);
        final RecordConstructorValue recordConstructorValue = (RecordConstructorValue)value;
        final Type resultType = recordConstructorValue.getResultType();
        Assertions.assertEquals(Type.Record.fromFields(false, List.of(Type.Record.Field.of(STRING_1.getResultType(), Optional.empty()),
                Type.Record.Field.of(INT_2.getResultType(), Optional.empty()),
                Type.Record.Field.of(FLOAT_1.getResultType(), Optional.empty())
                )), resultType);
        final Object result = recordConstructorValue.compileTimeEval(EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(recordConstructorValue.getResultType()).build()));
        Assertions.assertTrue(result instanceof DynamicMessage);
        final DynamicMessage resultMessage = (DynamicMessage)result;
        Assertions.assertEquals(3, resultMessage.getAllFields().size());
        List<Object> fieldSorted = resultMessage.getAllFields().entrySet().stream()
                .map(kv -> Pair.of(kv.getKey().getIndex(), kv.getValue()))
                .sorted(Map.Entry.comparingByKey())
                .map(Pair::getValue)
                .collect(Collectors.toList());
        Assertions.assertEquals("a", fieldSorted.get(0));
        Assertions.assertEquals(2, fieldSorted.get(1));
        Assertions.assertEquals(1.0F, fieldSorted.get(2));
    }
}
