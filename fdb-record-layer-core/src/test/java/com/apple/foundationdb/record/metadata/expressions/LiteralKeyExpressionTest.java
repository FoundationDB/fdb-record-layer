/*
 * LiteralKeyExpressionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests for the {@link LiteralKeyExpression}.
 */
public class LiteralKeyExpressionTest {
    private static final byte[] byteArray = {0x00, 0x0f, 0x16};

    private static Stream<Object> correctValues() {
        return Stream.of(
                1.0f,
                2.0d,
                100L,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                5,
                -10,
                Integer.MIN_VALUE,
                Integer.MAX_VALUE,
                true,
                false,
                "a string",
                "(╯°□°)╯︵ ┻━┻",
                "┳━┳ ヽ(ಠل͜ಠ)ﾉ",
                TextSamples.EMOJIS,
                TextSamples.YIDDISH,
                "\n",
                byteArray);
    }

    @ParameterizedTest
    @MethodSource("correctValues")
    public void serializationTest(@Nonnull Object value) throws InvalidProtocolBufferException {
        final LiteralKeyExpression<?> keyExpression = Key.Expressions.value(value);
        final LiteralKeyExpression<?> parsedViaProto = LiteralKeyExpression.fromProto(keyExpression.toProto());
        final LiteralKeyExpression<?> parsedViaBytes = LiteralKeyExpression.fromProto(
                RecordKeyExpressionProto.Value.parseFrom(keyExpression.toProto().toByteArray()));
        assertEquals(keyExpression, parsedViaProto);
        assertEquals(keyExpression, parsedViaBytes);
        if (value instanceof byte[]) {
            assertArrayEquals(((byte[]) value), ((byte[]) parsedViaProto.getValue()));
            assertArrayEquals(((byte[]) value), ((byte[]) parsedViaBytes.getValue()));
        } else {
            assertEquals(value, parsedViaProto.getValue());
            assertEquals(value, parsedViaBytes.getValue());
        }
    }

    @Test
    @SuppressWarnings("AvoidEscapedUnicodeCharacters")
    public void incorrectUnicodeSurrogatePairSerializationTest() throws InvalidProtocolBufferException {
        final String malformedSurrogateValue = "malformed surrogate pair: \uD83C";
        final LiteralKeyExpression<String> keyExpression = Key.Expressions.value(malformedSurrogateValue);
        assertEquals(malformedSurrogateValue, keyExpression.getValue());

        final LiteralKeyExpression<?> parsedViaProto = LiteralKeyExpression.fromProto(keyExpression.toProto());
        final LiteralKeyExpression<?> parsedViaBytes = LiteralKeyExpression.fromProto(
                RecordKeyExpressionProto.Value.parseFrom(keyExpression.toProto().toByteArray()));

        assertEquals(keyExpression, parsedViaProto); // Comparison uses proto objects, so both sides have Longs.
        // Comparison with proto objects works since we never leave Java.
        assertEquals(malformedSurrogateValue, parsedViaProto.getValue());
        // Comparison with Protobuf byte serialization doesn't because it uses UTF-8 instead of UTF-16.
        assertNotEquals(malformedSurrogateValue, parsedViaBytes.getValue());

    }
}
