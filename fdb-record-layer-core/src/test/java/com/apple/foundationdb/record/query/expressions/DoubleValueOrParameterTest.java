/*
 * DoubleValueOrParameterTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PDoubleValueOrParameter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runtime-evaluation branches of {@link DoubleValueOrParameter}'s Value-backed variant. The literal and named-parameter
 * variants trust their source to hand back a {@code Double} directly; the {@code Value}-backed variant must coerce any
 * {@link Number} to double, propagate a {@code null} result unchanged, and reject anything else so a misconfigured
 * center/radius expression surfaces a diagnostic error rather than a raw {@code ClassCastException}.
 */
class DoubleValueOrParameterTest {

    @Test
    void getValueOnValueBackedSourceReturnsNullWhenExpressionEvaluatesToNull() {
        final DoubleValueOrParameter source = DoubleValueOrParameter.valueExpression(
                new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE, true), null));

        assertThat(source.getValue(EvaluationContext.EMPTY)).isNull();
        assertThat(source.getValue(null, EvaluationContext.EMPTY)).isNull();
    }

    @Test
    void getValueOnValueBackedSourceWidensNonDoubleNumberToDouble() {
        // An Integer literal exercises the Number-but-not-Double branch (doubleValue() coercion) that both
        // getValue overloads must apply.
        final DoubleValueOrParameter source =
                DoubleValueOrParameter.valueExpression(LiteralValue.ofScalar(42));

        assertThat(source.getValue(EvaluationContext.EMPTY)).isEqualTo(42.0);
        assertThat(source.getValue(null, EvaluationContext.EMPTY)).isEqualTo(42.0);
    }

    @Test
    void getValueOnValueBackedSourceThrowsWhenExpressionEvaluatesToNonNumeric() {
        final DoubleValueOrParameter source =
                DoubleValueOrParameter.valueExpression(LiteralValue.ofScalar("not-a-number"));

        assertThatThrownBy(() -> source.getValue(EvaluationContext.EMPTY))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("value expression did not evaluate to a numeric result");
    }

    @Test
    void getValueWithStoreOnValueBackedSourceThrowsWhenExpressionEvaluatesToNonNumeric() {
        final DoubleValueOrParameter source =
                DoubleValueOrParameter.valueExpression(LiteralValue.ofScalar("not-a-number"));

        assertThatThrownBy(() -> source.getValue(null, EvaluationContext.EMPTY))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("value expression did not evaluate to a numeric result");
    }

    @Test
    void valueProtoRoundTripTest() {
        final DoubleValueOrParameter original = DoubleValueOrParameter.value(37.77);
        final PDoubleValueOrParameter proto = original.toProto(PlanSerializationContext.newForCurrentMode());
        assertThat(proto.getKindCase()).isEqualTo(PDoubleValueOrParameter.KindCase.VALUE);

        final DoubleValueOrParameter afterRoundTrip =
                DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(afterRoundTrip).isEqualTo(original);
        assertThat(afterRoundTrip).hasSameHashCodeAs(original);
        assertThat(afterRoundTrip).hasToString(original.toString());
        assertThat(afterRoundTrip.getValue(EvaluationContext.EMPTY)).isEqualTo(37.77);
        assertThat(afterRoundTrip.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void parameterProtoRoundTripTest() {
        final DoubleValueOrParameter original = DoubleValueOrParameter.parameter("lat");
        final PDoubleValueOrParameter proto = original.toProto(PlanSerializationContext.newForCurrentMode());
        assertThat(proto.getKindCase()).isEqualTo(PDoubleValueOrParameter.KindCase.PARAMETER);

        final DoubleValueOrParameter afterRoundTrip =
                DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(afterRoundTrip).isEqualTo(original);
        assertThat(afterRoundTrip).hasSameHashCodeAs(original);
        assertThat(afterRoundTrip).hasToString(original.toString());
        assertThat(afterRoundTrip.getValue(EvaluationContext.forBindings(Bindings.newBuilder().set("lat", 12.5).build())))
                .isEqualTo(12.5);
        assertThat(afterRoundTrip.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void fromProtoWithoutKindThrowsTest() {
        final PDoubleValueOrParameter empty = PDoubleValueOrParameter.getDefaultInstance();
        assertThatThrownBy(() -> DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), empty))
                .isInstanceOf(RecordCoreException.class);
    }

    @Test
    void equalityTest() {
        assertThat(DoubleValueOrParameter.value(1.0)).isEqualTo(DoubleValueOrParameter.value(1.0));
        assertThat(DoubleValueOrParameter.value(1.0)).isNotEqualTo(DoubleValueOrParameter.value(2.0));
        assertThat(DoubleValueOrParameter.parameter("a")).isEqualTo(DoubleValueOrParameter.parameter("a"));
        assertThat(DoubleValueOrParameter.parameter("a")).isNotEqualTo(DoubleValueOrParameter.parameter("b"));
        assertThat(DoubleValueOrParameter.value(1.0)).isNotEqualTo(DoubleValueOrParameter.parameter("a"));
        assertThat(DoubleValueOrParameter.value(1.0)).isNotEqualTo(null);
    }

    @Test
    void planHashTest() {
        final PlanHashable.PlanHashMode mode = PlanHashable.CURRENT_FOR_CONTINUATION;
        assertThat(DoubleValueOrParameter.value(1.0).planHash(mode))
                .isNotEqualTo(DoubleValueOrParameter.value(2.0).planHash(mode));
        assertThat(DoubleValueOrParameter.parameter("a").planHash(mode))
                .isNotEqualTo(DoubleValueOrParameter.parameter("b").planHash(mode));
    }
}
