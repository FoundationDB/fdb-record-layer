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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Test cases to test {@link RecordConstructorValue#deepCopyIfNeeded}.
 */
public class RecordConstructorValueTest {

    private static final String[] suites = new String[] {"SPADES", "HEARTS", "DIAMONDS", "CLUBS"};

    private static Type.Enum getCardsEnum() {
        final var enumValues = new ArrayList<Type.Enum.EnumValue>();
        for (var i = 0; i < suites.length; i++) {
            enumValues.add(new Type.Enum.EnumValue(suites[i], i));
        }
        return new Type.Enum(false, enumValues, "enumType");
    }

    @Test
    public void deepCopyMessageWithEnumTest() {
        final var enumType = getCardsEnum();
        var type = Type.Record.fromFieldsWithName("simpleType", false, List.of(Type.Record.Field.of(enumType, Optional.of("suit"))));
        var repo = TypeRepository.newBuilder();
        type.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var typeDescriptor = typeRepoSrc.getMessageDescriptor("simpleType");
        var messageBuilder = DynamicMessage.newBuilder(Objects.requireNonNull(typeDescriptor));
        var enumFieldSrc = typeDescriptor.findFieldByName("suit");
        messageBuilder.setField(enumFieldSrc, Objects.requireNonNull(typeRepoSrc.getEnumValue("enumType", "SPADES")));
        var message = messageBuilder.build();

        var copiedMessage = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, type, message);
        Assertions.assertNotNull(copiedMessage);
        Assertions.assertNotSame(message, copiedMessage);
        var enumFieldTargetValue = ((Message)copiedMessage).getField(Objects.requireNonNull(typeRepoTarget.getMessageDescriptor("simpleType")).findFieldByName("suit"));
        Assertions.assertTrue(enumFieldTargetValue instanceof Descriptors.EnumValueDescriptor);
        Assertions.assertEquals(((Descriptors.EnumValueDescriptor) message.getField(enumFieldSrc)).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
        Assertions.assertNotSame(message.getField(enumFieldSrc), enumFieldTargetValue);
    }

    @Test
    public void deepCopyEnumValueTest() {
        final var enumType = getCardsEnum();
        var repo = TypeRepository.newBuilder();
        enumType.defineProtoType(repo);

        final var typeRepoSrc = repo.build();
        final var typeRepoTarget = repo.build();

        final var enumValueSrc = typeRepoSrc.getEnumValue("enumType", "SPADES");

        final var copied = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, enumType, enumValueSrc);
        Assertions.assertTrue(copied instanceof Descriptors.EnumValueDescriptor);
        Assertions.assertEquals(enumValueSrc.getName(), ((Descriptors.EnumValueDescriptor) copied).getName());
        Assertions.assertNotSame(enumValueSrc, copied);
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

        final var typeDescriptor = typeRepoSrc.getMessageDescriptor("simpleType");
        var messageBuilder = DynamicMessage.newBuilder(Objects.requireNonNull(typeDescriptor));
        var enumFieldSrc = typeDescriptor.findFieldByName("suits");
        for (int i = suites.length - 1; i >= 0; i--) {
            messageBuilder.addRepeatedField(enumFieldSrc, Objects.requireNonNull(typeRepoSrc.getEnumValue("enumType", suites[i])));
        }
        var message = messageBuilder.build();

        var copiedMessage = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, type, message);
        Assertions.assertNotNull(copiedMessage);
        var enumFieldTarget = Objects.requireNonNull(typeRepoTarget.getMessageDescriptor("simpleType")).findFieldByName("suits");
        for (int i = 0; i < 4; i++) {
            var enumFieldTargetValue = ((Message) copiedMessage).getRepeatedField(enumFieldTarget, i);
            Assertions.assertTrue(enumFieldTargetValue instanceof Descriptors.EnumValueDescriptor);
            Assertions.assertEquals(((Descriptors.EnumValueDescriptor) message.getRepeatedField(enumFieldSrc, i)).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
            Assertions.assertNotSame(message.getRepeatedField(enumFieldSrc, i), enumFieldTargetValue);
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
        for (int i = suites.length - 1; i >= 0; i--) {
            list.add(Objects.requireNonNull(typeRepoSrc.getEnumValue("enumType", suites[i])));
        }

        var copied = RecordConstructorValue.deepCopyIfNeeded(typeRepoTarget, arrayType, list);
        Assertions.assertNotNull(copied);
        for (int i = 0; i < 4; i++) {
            var enumFieldTargetValue = ((List<?>) copied).get(i);
            Assertions.assertTrue(enumFieldTargetValue instanceof Descriptors.EnumValueDescriptor);
            Assertions.assertEquals(list.get(i).getName(), ((Descriptors.EnumValueDescriptor) enumFieldTargetValue).getName());
            Assertions.assertNotSame(list.get(i), enumFieldTargetValue);
        }
    }
}
