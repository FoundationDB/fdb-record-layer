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
import com.apple.foundationdb.relational.recordlayer.query.Literals;
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
    void getAuxiliaryConstantObjectValuesReturnsProvidedList() {
        // empty when none are provided
        Assertions.assertTrue(createTestFunction().getAuxiliaryConstantObjectValues().isEmpty());

        // round-trips the value-free constant object values passed to the constructor
        final var cov = com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue.of(
                com.apple.foundationdb.record.query.plan.cascades.Quantifier.constant(), "c0",
                com.apple.foundationdb.record.query.plan.cascades.typing.Type.primitiveType(
                        com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode.LONG));
        final var function = new CompiledSqlFunction("testFunction", ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), createDummyBody(), Literals.empty(), ImmutableList.of(cov));
        Assertions.assertEquals(ImmutableList.of(cov), function.getAuxiliaryConstantObjectValues());
    }

    @Test
    void withPlanGenerationSideEffectsDefaultsToEmptyConstantObjectValues() {
        // an implementation that provides only literals inherits an empty value-free COV list by default.
        final WithPlanGenerationSideEffects sideEffects = Literals::empty;
        Assertions.assertTrue(sideEffects.getAuxiliaryConstantObjectValues().isEmpty());
    }

    @Test
    void finalStepBuilderDefaultSetUnboundConstantObjectValuesIsNoOp() {
        final UserDefinedFunctionBuilder.FinalStepBuilder builder = new UserDefinedFunctionBuilder.FinalStepBuilder() {
            @Override
            public UserDefinedFunctionBuilder.FinalStepBuilder setLiterals(@Nonnull final Literals literals) {
                return this;
            }

            @Override
            public com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction build() {
                return null;
            }
        };
        // the default implementation ignores its argument and returns the same builder for chaining.
        Assertions.assertSame(builder, builder.setUnboundConstantObjectValues(ImmutableList.of()));
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
                Literals.empty(),
                ImmutableList.of()
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
