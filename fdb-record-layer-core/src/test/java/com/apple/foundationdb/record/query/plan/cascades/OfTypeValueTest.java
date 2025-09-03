/*
 * OfTypeValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OfTypeValue} and its ability to distinguish between {@code long} and {@code int}, as well as
 * {@code float} and {@code double}.
 */
public class OfTypeValueTest {

    @Test
    void ofTypeIntWorksCorrectly() {
        final var intValue = LiteralValue.ofScalar(42);
        Assertions.assertThat(
                        OfTypeValue.of(intValue, Type.primitiveType(Type.TypeCode.INT))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(intValue, Type.primitiveType(Type.TypeCode.INT, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                OfTypeValue.of(intValue, Type.primitiveType(Type.TypeCode.LONG))
                        .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
        Assertions.assertThat(
                        OfTypeValue.of(intValue, Type.primitiveType(Type.TypeCode.LONG, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
    }

    @Test
    void ofTypeLongWorksCorrectly() {
        final var longValue = LiteralValue.ofScalar(42L);
        Assertions.assertThat(
                        OfTypeValue.of(longValue, Type.primitiveType(Type.TypeCode.INT))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
        Assertions.assertThat(
                        OfTypeValue.of(longValue, Type.primitiveType(Type.TypeCode.INT, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
        Assertions.assertThat(
                        OfTypeValue.of(longValue, Type.primitiveType(Type.TypeCode.LONG))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(longValue, Type.primitiveType(Type.TypeCode.LONG, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
    }

    @Test
    void ofTypeFloatWorksCorrectly() {
        final var floatValue = LiteralValue.ofScalar(42.5f);
        Assertions.assertThat(
                        OfTypeValue.of(floatValue, Type.primitiveType(Type.TypeCode.FLOAT))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(floatValue, Type.primitiveType(Type.TypeCode.FLOAT, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(floatValue, Type.primitiveType(Type.TypeCode.DOUBLE))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
        Assertions.assertThat(
                        OfTypeValue.of(floatValue, Type.primitiveType(Type.TypeCode.DOUBLE, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
    }

    @Test
    void ofTypeDoubleWorksCorrectly() {
        final var doubleValue = LiteralValue.ofScalar(42.5d);
        Assertions.assertThat(
                        OfTypeValue.of(doubleValue, Type.primitiveType(Type.TypeCode.DOUBLE))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(doubleValue, Type.primitiveType(Type.TypeCode.DOUBLE, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isTrue();
        Assertions.assertThat(
                        OfTypeValue.of(doubleValue, Type.primitiveType(Type.TypeCode.FLOAT))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
        Assertions.assertThat(
                        OfTypeValue.of(doubleValue, Type.primitiveType(Type.TypeCode.FLOAT, true))
                                .evalWithoutStore(EvaluationContext.empty()))
                .isFalse();
    }
}
