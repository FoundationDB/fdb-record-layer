/*
 * ConstantObjectValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests for {@link ConstantObjectValueTest}.
 */
public class ConstantObjectValueTest {

    private static Stream<Arguments> argumentsProvider() {
        final var type1 = Type.Record.fromFieldsWithName("str_long_long", true, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1"))
        ));
        final var type2 = Type.Record.fromFieldsWithName("str_int_int", true, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("f1"))
        ));
        final var typeRepository = TypeRepository.newBuilder().addAllTypes(List.of(type1, type2)).build();
        final var descriptor1 = Objects.requireNonNull(typeRepository.getMessageDescriptor(type1));
        final var message1 = DynamicMessage.newBuilder(descriptor1)
                .setField(descriptor1.findFieldByName("f0"), "blah1")
                .setField(descriptor1.findFieldByName("f1"), 1L)
                .build();
        final var descriptor2 = Objects.requireNonNull(typeRepository.getMessageDescriptor(type2));
        final var message2 = DynamicMessage.newBuilder(descriptor2)
                .setField(descriptor2.findFieldByName("f0"), "blah1")
                .setField(descriptor2.findFieldByName("f1"), 1)
                .build();
        return Stream.of(
                // null
                Arguments.of(Type.nullType(), true, null, null),

                // primitive type
                Arguments.of(Type.primitiveType(Type.TypeCode.INT), true, 10, 10),
                Arguments.of(Type.primitiveType(Type.TypeCode.LONG, false), true, 10L, 10L),
                Arguments.of(Type.primitiveType(Type.TypeCode.LONG, false), true, 10, 10L),
                Arguments.of(Type.primitiveType(Type.TypeCode.FLOAT, false), true, 10, 10.0f),
                Arguments.of(Type.primitiveType(Type.TypeCode.FLOAT, false), true, 10L, 10.0f),
                Arguments.of(Type.primitiveType(Type.TypeCode.FLOAT, false), true, 10.0f, 10.0f),
                Arguments.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), true, 10, 10.0),
                Arguments.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), true, 10L, 10.0),
                Arguments.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), true, 10.0f, 10.0),
                Arguments.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), true, 10.0, 10.0),

                // DynamicMessage
                Arguments.of(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1"))
                )), true, message1, message1),
                // Different shape of DynamicMessage is not allowed
                Arguments.of(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0"))
                )), false, message1, null),
                // Promotion in DynamicMessage is not allowed
                Arguments.of(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1"))
                )), false, message2, null),

                // List type
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.LONG)), true, List.of(1L, 2L, 3L), List.of(1L, 2L, 3L)),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.INT)), true, List.of(1, 2, 3), List.of(1, 2, 3)),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.DOUBLE)), true, List.of(1.0, 2.0, 3.0), List.of(1.0, 2.0, 3.0)),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.FLOAT)), true, List.of(1.0f, 2.0f, 3.0f), List.of(1.0f, 2.0f, 3.0f)),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.STRING)), true, List.of("1", "2", "3"), List.of("1", "2", "3")),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.BOOLEAN)), true, List.of(true, false, true), List.of(true, false, true)),
                // List of DynamicMessage
                Arguments.of(new Type.Array(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1"))
                ))), true, List.of(message1, message1), List.of(message1, message1)),
                // promotion in Array is not allowed
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.LONG)), false, List.of(1, 2, 3), null),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.DOUBLE)), false, List.of(1, 2, 3), null),
                Arguments.of(new Type.Array(Type.primitiveType(Type.TypeCode.FLOAT)), false, List.of(1, 2, 3), null),
                Arguments.of(new Type.Array(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1"))
                ))), false, List.of(message2), null),
                Arguments.of(new Type.Array(Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f1")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f0"))
                ))), false, List.of(message1), null)
        );
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void testEval(@Nonnull Type covResultType, boolean expectedSuccess, @Nullable Object bindingObject, @Nullable Object expectedObject) {
        final var alias = Bindings.Internal.CONSTANT.bindingName("blah");
        final var bindingMap = new HashMap<String, Object>();
        bindingMap.put("key", bindingObject);
        final var ctx = EvaluationContext.forBindings(Bindings.newBuilder().set(alias, bindingMap).build());
        final var cov = ConstantObjectValue.of(CorrelationIdentifier.of("blah"), "key", covResultType);
        if (expectedSuccess) {
            final var actualValue =  cov.eval(null, ctx);
            Assertions.assertEquals(expectedObject, actualValue);
        } else {
            Assertions.assertThrows(Throwable.class, () -> cov.eval(null, ctx));
        }
    }
}
