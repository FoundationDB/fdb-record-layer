/*
 * RecordConstructorValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases to test {@link RecordConstructorValue#deepCopyIfNeeded}.
 */
public class RecordConstructorValueTest {

    private static final String[] suits = new String[] {"SPADES", "HEARTS", "DIAMONDS", "CLUBS"};

    private static Type.Enum getCardsEnum() {
        final var enumValues = new ArrayList<Type.Enum.EnumValue>();
        for (var i = 0; i < suits.length; i++) {
            enumValues.add(Type.Enum.EnumValue.from(suits[i], i));
        }
        return Type.Enum.fromValuesWithName("enumType", false, enumValues);
    }

    @Test
    public void deepCopyMessageWithEnumTest() {
        final var enumType = getCardsEnum();
        var type = Type.Record.fromFieldsWithName("simpleType", false, List.of(Type.Record.Field.of(enumType, Optional.of("suit"))));
        var repo = TypeRepository.newBuilder();
        type.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var typeDescriptor = typeRepoSrc.getMessageDescriptor(type);
        var messageBuilder = DynamicMessage.newBuilder(Objects.requireNonNull(typeDescriptor));
        var enumFieldSrc = typeDescriptor.findFieldByName("suit");
        messageBuilder.setField(enumFieldSrc, Objects.requireNonNull(typeRepoSrc.getEnumValue(enumType, "SPADES")));
        var message = messageBuilder.build();

        var copiedMessage = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, type, message);
        assertNotNull(copiedMessage);
        assertNotSame(message, copiedMessage);
        var enumFieldTargetValue = ((Message)copiedMessage).getField(Objects.requireNonNull(typeRepoTarget.getMessageDescriptor(type)).findFieldByName("suit"));
        assertThat(enumFieldTargetValue, instanceOf(Descriptors.EnumValueDescriptor.class));
        assertEquals(((Descriptors.EnumValueDescriptor) message.getField(enumFieldSrc)).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
        assertNotSame(message.getField(enumFieldSrc), enumFieldTargetValue);
    }

    @Test
    public void deepCopyEnumValueTest() {
        final var enumType = getCardsEnum();
        var repo = TypeRepository.newBuilder();
        enumType.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var enumValueSrc = typeRepoSrc.getEnumValue(enumType, "SPADES");

        final var copied = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, enumType, enumValueSrc);
        assertTrue(copied instanceof Descriptors.EnumValueDescriptor);
        assertEquals(enumValueSrc.getName(), ((Descriptors.EnumValueDescriptor) copied).getName());
        assertNotSame(enumValueSrc, copied);
    }

    @Test
    public void deepCopyMessageWithRepeatedEnumValueTest() {
        final var enumType = getCardsEnum();
        var arrayType = new Type.Array(false, enumType);
        var type = Type.Record.fromFieldsWithName("simpleType", false, List.of(Type.Record.Field.of(arrayType, Optional.of("suits"))));
        var repo = TypeRepository.newBuilder();
        arrayType.defineProtoType(repo);
        type.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var typeDescriptor = typeRepoSrc.getMessageDescriptor(type);
        var messageBuilder = DynamicMessage.newBuilder(Objects.requireNonNull(typeDescriptor));
        var enumFieldSrc = typeDescriptor.findFieldByName("suits");
        for (int i = suits.length - 1; i >= 0; i--) {
            messageBuilder.addRepeatedField(enumFieldSrc, Objects.requireNonNull(typeRepoSrc.getEnumValue(enumType, suits[i])));
        }
        var message = messageBuilder.build();

        var copiedMessage = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, type, message);
        assertNotNull(copiedMessage);
        var enumFieldTarget = Objects.requireNonNull(typeRepoTarget.getMessageDescriptor(type)).findFieldByName("suits");
        for (int i = 0; i < 4; i++) {
            var enumFieldTargetValue = ((Message) copiedMessage).getRepeatedField(enumFieldTarget, i);
            assertTrue(enumFieldTargetValue instanceof Descriptors.EnumValueDescriptor);
            assertEquals(((Descriptors.EnumValueDescriptor) message.getRepeatedField(enumFieldSrc, i)).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
            assertNotSame(message.getRepeatedField(enumFieldSrc, i), enumFieldTargetValue);
        }
    }

    @Test
    public void deepCopyEnumValueArrayTest() {
        final var enumType = getCardsEnum();
        var arrayType = new Type.Array(false, enumType);
        var repo = TypeRepository.newBuilder();
        arrayType.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var list = new ArrayList<Descriptors.EnumValueDescriptor>();
        for (int i = suits.length - 1; i >= 0; i--) {
            list.add(Objects.requireNonNull(typeRepoSrc.getEnumValue(enumType, suits[i])));
        }

        var copied = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, arrayType, list);
        assertNotNull(copied);
        for (int i = 0; i < 4; i++) {
            var enumFieldTargetValue = ((List<?>) copied).get(i);
            assertTrue(enumFieldTargetValue instanceof Descriptors.EnumValueDescriptor);
            assertEquals(list.get(i).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
            assertNotSame(list.get(i), enumFieldTargetValue);
        }
    }

    @Test
    public void evalWithVersion() {
        List<Type.Record.Field> fields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.VERSION), Optional.of("version")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("number"))
        );
        var type = Type.Record.fromFieldsWithName("TypeWithVersionField", false, fields);

        var repo = TypeRepository.newBuilder();
        type.defineProtoType(repo);
        EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(repo.build());

        final FDBRecordVersion version = FDBRecordVersion.firstInDBVersion(0x5ca1ab1e);
        RecordConstructorValue recordConstructorValue = RecordConstructorValue.ofColumns(List.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), version)),
                Column.of(fields.get(1), new LiteralValue<>(fields.get(1).getFieldType(), null))
        ));

        Object result = recordConstructorValue.eval(null, evaluationContext);
        assertNotNull(result);
        assertThat(result, instanceOf(Message.class));
        Message resultMessage = (Message) result;

        Descriptors.Descriptor descriptor = resultMessage.getDescriptorForType();
        Descriptors.FieldDescriptor versionField = descriptor.findFieldByName("version");
        assertTrue(resultMessage.hasField(versionField));
        assertEquals(version, FDBRecordVersion.fromBytes(((ByteString) resultMessage.getField(versionField)).toByteArray()));

        Descriptors.FieldDescriptor numberField = descriptor.findFieldByName("number");
        assertFalse(resultMessage.hasField(numberField));

        // Copy through intermediate protobuf
        RecordConstructorValue copiedRecordConstructorValue = RecordConstructorValue.ofColumns(List.of(
                Column.of(fields.get(0), FieldValue.ofFieldName(new LiteralValue<>(type, result), fields.get(0).getFieldName())),
                Column.of(fields.get(1), FieldValue.ofFieldName(new LiteralValue<>(type, result), fields.get(1).getFieldName()))
        ));

        Object result2 = copiedRecordConstructorValue.eval(null, evaluationContext);
        assertEquals(result, result2);
    }

    @Test
    void evalVersionArray() {
        List<Type.Record.Field> fields = List.of(
                Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.VERSION)), Optional.of("non_null_versions")),
                Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.VERSION)), Optional.of("nullable_versions"))
        );
        var type = Type.Record.fromFieldsWithName("TypeWithVersionArrayFields", false, fields);

        var repo = TypeRepository.newBuilder();
        type.defineProtoType(repo);
        EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(repo.build());

        List<FDBRecordVersion> versions = List.of(
                FDBRecordVersion.firstInDBVersion(0xfdb),
                FDBRecordVersion.firstInDBVersion(0x1337)
        );
        RecordConstructorValue populatedRecordConstructorValue = RecordConstructorValue.ofColumns(List.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), versions)),
                Column.of(fields.get(1), new LiteralValue<>(fields.get(1).getFieldType(), versions))
        ));

        Object populatedResult = populatedRecordConstructorValue.eval(null, evaluationContext);
        assertEquals(versions, FieldValue.ofFieldName(new LiteralValue<>(type, populatedResult), fields.get(0).getFieldName()).eval(null, evaluationContext));
        assertEquals(versions, FieldValue.ofFieldName(new LiteralValue<>(type, populatedResult), fields.get(1).getFieldName()).eval(null, evaluationContext));

        // Now set both fields to null/empty
        RecordConstructorValue nullRecordConstructorValue = RecordConstructorValue.ofColumns(List.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), Collections.emptyList())),
                Column.of(fields.get(1), new LiteralValue<>(fields.get(1).getFieldType(), null))
        ));
        Object nullResult = nullRecordConstructorValue.eval(null, evaluationContext);
        assertEquals(Collections.emptyList(), FieldValue.ofFieldName(new LiteralValue<>(type, nullResult), fields.get(0).getFieldName()).eval(null, evaluationContext));
        assertNull(FieldValue.ofFieldName(new LiteralValue<>(type, nullResult), fields.get(1).getFieldName()).eval(null, evaluationContext));
    }
}
