/*
 * CompilableSqlFunctionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Tests for {@link CompiledSqlFunction}.
 */
class CompilableSqlFunctionTest {

    @Test
    void toProtoThrowsRecordCoreException() {
        final var function = createTestFunction();

        final var exception = Assertions.assertThrows(RecordCoreException.class, function::toProto);

        Assertions.assertNotNull(exception);
        Assertions.assertEquals("attempt to serialize compiled SQL function", exception.getMessage());
    }

    @Test
    void auxiliaryLiteralsCarryValueFreeLiterals() {
        // A typed signature parameter warmed with no value rides in the function's literal table as a value-free
        // literal: it reserves the constant id and declares the type, but contributes no binding.
        final var function = new CompiledSqlFunction("testFunction", ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), createDummyBody(), literalsWithValueFreeParameter());

        final var carried = function.getAuxiliaryLiterals();
        final var valueFree = carried.getOrderedLiterals().stream()
                .filter(OrderedLiteral::isValueFree)
                .collect(ImmutableList.toImmutableList());
        Assertions.assertEquals(1, valueFree.size());
        Assertions.assertEquals("param_b", valueFree.get(0).getParameterName());
        final var valueFreeConstantId = valueFree.get(0).getConstantId();
        Assertions.assertTrue(carried.isValueFree(valueFreeConstantId));
        // The value-free literal contributes no binding, so it is absent from the constant map, while the
        // value-bearing literal beside it does bind.
        Assertions.assertFalse(carried.asBindings().containsKey(valueFreeConstantId));
        Assertions.assertEquals(1, carried.asBindings().size());
    }

    /**
     * Creates a literal table holding one value-bearing named parameter and one value-free one.
     */
    @Nonnull
    private static Literals literalsWithValueFreeParameter() {
        final var builder = Literals.newBuilder();
        builder.addLiteral(Type.primitiveType(Type.TypeCode.STRING), "bound", null, "param_a", 1);
        builder.addValueFreeLiteral(Type.primitiveType(Type.TypeCode.LONG, false), "param_b", 2);
        return builder.build();
    }

    /**
     * Creates a simple test function with basic parameters.
     */
    @Nonnull
    private CompiledSqlFunction createTestFunction() {
        return new CompiledSqlFunction(
                "testFunction",
                ImmutableList.of("param1", "param2"),
                ImmutableList.of(
                        com.apple.foundationdb.record.query.plan.cascades.typing.Type.primitiveType(
                                com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode.INT),
                        com.apple.foundationdb.record.query.plan.cascades.typing.Type.primitiveType(
                                com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode.STRING)
                ),
                ImmutableList.of(Optional.empty(), Optional.empty()),
                Optional.of(com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier.of("test")),
                createDummyBody(),
                Literals.empty()
        );
    }

    /**
     * Creates a dummy relational expression to use as function body.
     */
    @Nonnull
    private RelationalExpression createDummyBody() {
        // Create a minimal SelectExpression as the function body
        return new SelectExpression(
                com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue.ofUnnamed(ImmutableList.of()),
                ImmutableList.of(),
                ImmutableList.of()
        );
    }
}
