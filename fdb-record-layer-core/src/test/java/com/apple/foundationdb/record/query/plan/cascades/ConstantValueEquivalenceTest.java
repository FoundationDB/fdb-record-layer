/*
 * ConstantValueEquivalenceTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ValueEquivalence.ConstantValueEquivalence}, which relates a {@link ConstantObjectValue} to a
 * {@link LiteralValue} by comparing the value bound to the constant.
 *
 * <p>The interesting case is a constant that carries no value at all, because it is planned from its declared type
 * alone. Its absence must not be mistaken for the value {@code NULL}.</p>
 */
class ConstantValueEquivalenceTest {

    private static final String CONSTANT_ID = "c1";
    private static final Type LONG_TYPE = Type.primitiveType(Type.TypeCode.LONG);

    @Nonnull
    private static ConstantObjectValue constant() {
        return ConstantObjectValue.of(Quantifier.constant(), CONSTANT_ID, LONG_TYPE);
    }

    /**
     * A context binding {@code bindings} under the constant alias. A key mapped to {@code null} is bound to SQL
     * {@code NULL}; a key that is absent carries no value at all.
     */
    @Nonnull
    private static EvaluationContext contextBinding(@Nonnull final Map<String, Object> bindings) {
        return EvaluationContext.newBuilder()
                .setConstant(Quantifier.constant(), bindings)
                .build(TypeRepository.empty());
    }

    @Nonnull
    private static ValueEquivalence.ConstantValueEquivalence equivalenceUnder(@Nonnull final EvaluationContext context) {
        return new ValueEquivalence.ConstantValueEquivalence(context);
    }

    @Nonnull
    private static Map<String, Object> mapWithNull(@Nonnull final String key) {
        // Map.of rejects null values, and a null value is exactly what "bound to SQL NULL" means here.
        final var bindings = new HashMap<String, Object>();
        bindings.put(key, null);
        return bindings;
    }

    @Nonnull
    private static LiteralValue<?> literal(@Nullable final Object value) {
        return new LiteralValue<>(LONG_TYPE, value);
    }

    /**
     * The constant is bound to a value equal to the literal, so the two are equal under a constraint pinning that
     * value.
     */
    @Test
    void boundConstantIsEqualToAMatchingLiteral() {
        final var equivalence = equivalenceUnder(contextBinding(Map.of(CONSTANT_ID, 42L)));

        assertThat(equivalence.isDefinedEqual(constant(), literal(42L)).isTrue()).isTrue();
        assertThat(equivalence.isDefinedEqual(constant(), literal(7L)).isTrue()).isFalse();
    }

    /**
     * A constant bound to {@code NULL} is equal to a {@code NULL} literal — both are the value {@code NULL}.
     */
    @Test
    void constantBoundToNullIsEqualToANullLiteral() {
        final var equivalence = equivalenceUnder(contextBinding(mapWithNull(CONSTANT_ID)));

        assertThat(equivalence.isDefinedEqual(constant(), literal(null)).isTrue()).isTrue();
    }

    /**
     * A constant with no binding is not equal to a {@code NULL} literal. Its absence means "no value at all", not the
     * value {@code NULL} — and mistaking one for the other would attach an {@code IS_NULL} constraint to the plan that
     * no non-null value could satisfy, making the plan unreachable.
     */
    @Test
    void valueFreeConstantIsNotEqualToANullLiteral() {
        // Another constant is bound, so the constant map exists and only this constant is missing from it — the shape a
        // query mixing literals with a value-free parameter produces.
        final var equivalence = equivalenceUnder(contextBinding(Map.of("c2", 7L)));

        assertThat(equivalence.isDefinedEqual(constant(), literal(null)).isTrue()).isFalse();
        assertThat(equivalence.isDefinedEqual(constant(), literal(42L)).isTrue()).isFalse();
    }

    /**
     * The same with no constants bound at all, which is how a plan is generated when no value is known for any of them.
     */
    @Test
    void valueFreeConstantIsNotEqualToANullLiteralWithoutAnyBindings() {
        final var equivalence = equivalenceUnder(EvaluationContext.forTypeRepository(TypeRepository.empty()));

        assertThat(equivalence.isDefinedEqual(constant(), literal(null)).isTrue()).isFalse();
    }
}
