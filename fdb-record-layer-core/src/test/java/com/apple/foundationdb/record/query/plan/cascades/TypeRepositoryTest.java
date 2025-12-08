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

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.VerifyException;
import com.google.protobuf.ByteString;
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
import java.util.UUID;
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
    private static final LiteralValue<UUID> UUID_1 = new LiteralValue<>(Type.uuidType(false), UUID.fromString("eebee473-690b-48c1-beb0-07c3aca77768"));
    private static final LiteralValue<RealVector> HALF_VECTOR_1_2_3 = new LiteralValue<>(Type.Vector.of(false, 16, 3), new HalfRealVector(new double[] {1.0d, 2.0d, 3.0d}));

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
        if (type.isPrimitive() || type.isUuid()) {
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
                if (typeCode.isPrimitive() || typeCode.equals(Type.TypeCode.UUID)) {
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
            case UUID:
                return Type.uuidType(random.nextBoolean());
            case VECTOR:
                return randomVectorType();
            case RELATION: // fallthrough
            case UNKNOWN: // fallthrough
            case ANY: // fallthrough
            default:
                throw new IllegalArgumentException("unexpected random type: " + requestedTypeCode);
        }
    }

    @Nonnull
    private static Type.Vector randomVectorType() {
        final var validPrecisions = new int[] {16, 32, 64};
        final var randomPrecision = validPrecisions[random.nextInt(validPrecisions.length)];
        final var randomDimensions = random.nextInt(3000);
        return Type.Vector.of(random.nextBoolean(), randomPrecision, randomDimensions);
    }

    @Test
    void addUnsupportedTypesIsNotAllowed() {
        TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Test primitive types
        for (Type.TypeCode primitiveCode : List.of(Type.TypeCode.BOOLEAN, Type.TypeCode.BYTES,
                Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, Type.TypeCode.INT,
                Type.TypeCode.LONG, Type.TypeCode.STRING, Type.TypeCode.VERSION)) {
            final var primitiveType = Type.primitiveType(primitiveCode, true);
            builder.addTypeIfNeeded(primitiveType);
            Assertions.assertTrue(builder.getTypeName(primitiveType).isEmpty(),
                    "Primitive type " + primitiveCode + " should not be added to repository");
        }

        // Test special types that should not be allowed
        List<Type> unsupportedTypes = List.of(
                Type.any(),                           // ANY type
                Type.nullType(),                      // NULL type
                new Type.None(),                      // NONE type
                new Type.Relation(Type.primitiveType(Type.TypeCode.INT))  // RELATION type
        );

        for (Type unsupportedType : unsupportedTypes) {
            builder.addTypeIfNeeded(unsupportedType);
            Assertions.assertTrue(builder.getTypeName(unsupportedType).isEmpty(),
                    "Unsupported type " + unsupportedType.getTypeCode() + " should not be added to repository");
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
    void addNullableAndNonNullableVariantsSameProtobufType() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create a record type (same structural type with different nullability)
        final Type.Record nullableRecord = Type.Record.fromFields(true, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("field1")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("field2"))
        ));
        final Type.Record nonNullableRecord = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("field1")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("field2"))
        ));

        // Add both nullable and non-nullable variants
        builder.addTypeIfNeeded(nullableRecord);
        builder.addTypeIfNeeded(nonNullableRecord);

        final TypeRepository repository = builder.build();

        // Both should resolve to the same protobuf type name
        String nullableTypeName = repository.getProtoTypeName(nullableRecord);
        String nonNullableTypeName = repository.getProtoTypeName(nonNullableRecord);

        Assertions.assertEquals(nullableTypeName, nonNullableTypeName,
                "Nullable and non-nullable variants should have the same protobuf type name");

        // Should only create one message type since both variants use the same canonical type
        Assertions.assertEquals(1, repository.getMessageTypes().size(),
                "Should only create one message type for nullable and non-nullable variants");
    }

    @Test
    void addNullableAndNonNullableVariantsShouldNotCreateMultipleMessageTypes() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        final Type.Record baseType = (Type.Record)generateType(0, Type.TypeCode.RECORD);

        // Create nullable and non-nullable variants of the same structural type
        final Type nullableVariant = baseType.withNullability(true);
        final Type nonNullableVariant = baseType.withNullability(false);

        // Add both variants multiple times
        builder.addTypeIfNeeded(nullableVariant);
        builder.addTypeIfNeeded(nonNullableVariant);
        builder.addTypeIfNeeded(nullableVariant);  // Add again
        builder.addTypeIfNeeded(nonNullableVariant);  // Add again

        final TypeRepository actualRepository = builder.build();

        // Should only create message types based on the count of the base type
        // Both variants should resolve to the same canonical type
        Assertions.assertEquals(countTypes(baseType), actualRepository.getMessageTypes().size(),
                "Adding nullable and non-nullable variants should not create duplicate message types");

        // Both should resolve to the same protobuf type name
        String nullableTypeName = actualRepository.getProtoTypeName(nullableVariant);
        String nonNullableTypeName = actualRepository.getProtoTypeName(nonNullableVariant);
        Assertions.assertEquals(nullableTypeName, nonNullableTypeName,
                "Both variants should resolve to the same protobuf type name");
    }

    @Test
    void addNullableAndNonNullableEnumVariantsSameProtobufType() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create an enum type (same structural type with different nullability)
        final List<Type.Enum.EnumValue> enumValues = List.of(
                Type.Enum.EnumValue.from("VALUE1", 0),
                Type.Enum.EnumValue.from("VALUE2", 1),
                Type.Enum.EnumValue.from("VALUE3", 2)
        );
        final Type.Enum nullableEnum = Type.Enum.fromValues(true, enumValues);
        final Type.Enum nonNullableEnum = Type.Enum.fromValues(false, enumValues);

        // Add both nullable and non-nullable variants
        builder.addTypeIfNeeded(nullableEnum);
        builder.addTypeIfNeeded(nonNullableEnum);

        final TypeRepository repository = builder.build();

        // Both should resolve to the same protobuf type name
        String nullableTypeName = repository.getProtoTypeName(nullableEnum);
        String nonNullableTypeName = repository.getProtoTypeName(nonNullableEnum);

        Assertions.assertEquals(nullableTypeName, nonNullableTypeName,
                "Nullable and non-nullable enum variants should have the same protobuf type name");

        // Should only create one enum type since both variants use the same canonical type
        Assertions.assertEquals(1, repository.getEnumTypes().size(),
                "Should only create one enum type for nullable and non-nullable variants");
    }

    @Test
    void addNullableAndNonNullableEnumVariantsShouldNotCreateMultipleEnumTypes() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create an enum type with multiple values
        final List<Type.Enum.EnumValue> enumValues = List.of(
                Type.Enum.EnumValue.from("OPTION_A", 0),
                Type.Enum.EnumValue.from("OPTION_B", 1),
                Type.Enum.EnumValue.from("OPTION_C", 2),
                Type.Enum.EnumValue.from("OPTION_D", 3)
        );
        final Type.Enum baseEnum = Type.Enum.fromValues(true, enumValues);

        // Create nullable and non-nullable variants of the same structural enum
        final Type nullableVariant = baseEnum.withNullability(true);
        final Type nonNullableVariant = baseEnum.withNullability(false);

        // Add both variants multiple times
        builder.addTypeIfNeeded(nullableVariant);
        builder.addTypeIfNeeded(nonNullableVariant);
        builder.addTypeIfNeeded(nullableVariant);  // Add again
        builder.addTypeIfNeeded(nonNullableVariant);  // Add again

        final TypeRepository actualRepository = builder.build();

        // Should only create one enum type since both variants resolve to the same canonical type
        Assertions.assertEquals(1, actualRepository.getEnumTypes().size(),
                "Adding nullable and non-nullable enum variants should not create duplicate enum types");

        // Both should resolve to the same protobuf type name
        String nullableTypeName = actualRepository.getProtoTypeName(nullableVariant);
        String nonNullableTypeName = actualRepository.getProtoTypeName(nonNullableVariant);
        Assertions.assertEquals(nullableTypeName, nonNullableTypeName,
                "Both enum variants should resolve to the same protobuf type name");
    }

    @Test
    void attemptToCreateArrayConstructorValueWithDifferentChildrenTypesFails() {
        Assertions.assertThrows(SemanticException.class, () -> new AbstractArrayConstructorValue.ArrayFn().encapsulate(List.of(INT_1, STRING_1 /*invalid*/)));
    }

    @Test
    void addSameTypeWithDifferentNamesAndDifferentNullabilityShouldFail() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create the same structural record type with different nullability
        final Type.Record nullableRecord = Type.Record.fromFields(true, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("field1"))
        ));
        final Type.Record nonNullableRecord = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("field1"))
        ));

        // Register the nullable variant with one name
        builder.registerTypeToTypeNameMapping(nullableRecord, "TestRecord");

        // Attempt to register the non-nullable variant (same structural type) with a different name
        // This should fail because both variants canonicalize to the same type but try to use different names
        Assertions.assertThrows(VerifyException.class, () -> {
            builder.registerTypeToTypeNameMapping(nonNullableRecord, "DifferentName");
        });
    }

    @Test
    void addDifferentTypesWithSameNameShouldFail() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create two structurally different record types
        final Type.Record record1 = Type.Record.fromFields(true, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("stringField"))
        ));
        final Type.Record record2 = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("intField"))
        ));

        // Register the first record with a name
        builder.registerTypeToTypeNameMapping(record1, "ConflictingName");

        // Attempt to register a structurally different record with the same name
        // This should fail because the name is already taken by a different structural type
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            builder.registerTypeToTypeNameMapping(record2, "ConflictingName");
        });
    }

    @Test
    void accessTypeWithDifferentNullabilityThanRegistered() {
        final TypeRepository.Builder builder = TypeRepository.newBuilder();

        // Create a record type and register it with nullable variant
        final Type.Record baseRecord = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("testField")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("numberField"))
        ));
        final Type.Record nullableRecord = baseRecord.withNullability(true);
        final Type.Record nonNullableRecord = baseRecord.withNullability(false);

        // Add the nullable variant to the builder
        builder.addTypeIfNeeded(nullableRecord);
        final TypeRepository repository = builder.build();

        // Test accessing with the same nullability (should work)
        String nullableTypeName = repository.getProtoTypeName(nullableRecord);
        DynamicMessage.Builder nullableBuilder = repository.newMessageBuilder(nullableRecord);
        Assertions.assertNotNull(nullableTypeName, "Should be able to get proto type name for registered nullability");
        Assertions.assertNotNull(nullableBuilder, "Should be able to get message builder for registered nullability");

        // Test accessing with different nullability
        // With the canonicalization approach, this should work since both variants resolve to the same canonical type
        String nonNullableTypeName;
        DynamicMessage.Builder nonNullableMessageBuilder;

        nonNullableTypeName = repository.getProtoTypeName(nonNullableRecord);
        nonNullableMessageBuilder = repository.newMessageBuilder(nonNullableRecord);

        // If we get here without exception, verify they resolve to the same type
        Assertions.assertEquals(nullableTypeName, nonNullableTypeName,
                "Both nullability variants should resolve to the same protobuf type name");
        Assertions.assertNotNull(nonNullableMessageBuilder,
                "Should be able to get message builder for opposite nullability variant");
    }

    @Test
    void createLightArrayConstructorValueWorks() {
        final Typed value = new AbstractArrayConstructorValue.ArrayFn().encapsulate(List.of(INT_NOT_NULLABLE_1, INT_NOT_NULLABLE_2));
        Assertions.assertTrue(value instanceof AbstractArrayConstructorValue.LightArrayConstructorValue);
        final AbstractArrayConstructorValue.LightArrayConstructorValue arrayConstructorValue = (AbstractArrayConstructorValue.LightArrayConstructorValue)value;
        final Type resultType = arrayConstructorValue.getResultType();
        Assertions.assertEquals(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false)), resultType);
        final Object result = arrayConstructorValue.evalWithoutStore(EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(arrayConstructorValue.getResultType()).build()));
        Assertions.assertTrue(result instanceof List);
        final List<?> list = (List<?>)result;
        Assertions.assertEquals(2, list.size());
        Assertions.assertEquals(1, list.get(0));
        Assertions.assertEquals(2, list.get(1));
    }

    @Test
    void createRecordTypeConstructorWorks() {
        final Typed value = new RecordConstructorValue.RecordFn().encapsulate(List.of(STRING_1, INT_2, FLOAT_1, UUID_1, HALF_VECTOR_1_2_3));
        Assertions.assertInstanceOf(RecordConstructorValue.class, value);
        final RecordConstructorValue recordConstructorValue = (RecordConstructorValue)value;
        final Type resultType = recordConstructorValue.getResultType();
        Assertions.assertEquals(Type.Record.fromFields(false, List.of(Type.Record.Field.of(STRING_1.getResultType(), Optional.empty()),
                Type.Record.Field.of(INT_2.getResultType(), Optional.empty()),
                Type.Record.Field.of(FLOAT_1.getResultType(), Optional.empty()),
                Type.Record.Field.of(UUID_1.getResultType(), Optional.empty()),
                Type.Record.Field.of(HALF_VECTOR_1_2_3.getResultType(), Optional.empty())
                )), resultType);
        final Object result = recordConstructorValue.evalWithoutStore(EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(recordConstructorValue.getResultType()).build()));
        Assertions.assertInstanceOf(DynamicMessage.class, result);
        final DynamicMessage resultMessage = (DynamicMessage)result;
        Assertions.assertEquals(5, resultMessage.getAllFields().size());
        List<Object> fieldSorted = resultMessage.getAllFields().entrySet().stream()
                .map(kv -> Pair.of(kv.getKey().getIndex(), kv.getValue()))
                .sorted(Map.Entry.comparingByKey())
                .map(Pair::getValue)
                .collect(Collectors.toList());
        Assertions.assertEquals("a", fieldSorted.get(0));
        Assertions.assertEquals(2, fieldSorted.get(1));
        Assertions.assertEquals(1.0F, fieldSorted.get(2));
        Assertions.assertEquals(TupleFieldsProto.UUID.newBuilder()
                .setMostSignificantBits(UUID_1.getLiteralValue().getMostSignificantBits())
                .setLeastSignificantBits(UUID_1.getLiteralValue().getLeastSignificantBits())
                .build(), fieldSorted.get(3));
        Assertions.assertEquals(ByteString.copyFrom(HALF_VECTOR_1_2_3.getLiteralValue().getRawData()),
                fieldSorted.get(4));
    }
}
