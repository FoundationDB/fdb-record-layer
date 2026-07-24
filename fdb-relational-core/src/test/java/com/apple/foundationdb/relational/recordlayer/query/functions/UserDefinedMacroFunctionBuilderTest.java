/*
 * UserDefinedMacroFunctionBuilderTest.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedMacroFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link UserDefinedMacroFunctionBuilder}, exercised through the public {@link UserDefinedFunctionBuilder}.
 */
final class UserDefinedMacroFunctionBuilderTest {

    @Nonnull
    private static final Type STRING_TYPE = Type.primitiveType(Type.TypeCode.STRING);

    @Test
    void buildFunctionWithNoParameters() {
        final var functionBodyValue = LiteralValue.ofScalar(42);
        final var userDefinedMacroFunctionBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName("functionWithNoArgs")
                .seal()
                .withBodyValue(functionBodyValue);

        final var function = userDefinedMacroFunctionBuilder.build();

        assertThat(function).isInstanceOf(UserDefinedMacroFunction.class);
        assertThat(function.getFunctionName()).isEqualTo("functionWithNoArgs");
        assertThat(function.getParameterTypes()).isEmpty();
        assertThat(function.encapsulate(CallSiteArguments.ofPositional())).isEqualTo(functionBodyValue);
    }

    @Test
    void buildFunctionWithSingleStringParameter() {
        final var functionBodyStepBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName("identityFunction")
                .addParameter(stringParameter("input"))
                .seal();
        final var parametersQuantifier = functionBodyStepBuilder.getParametersCorrelation();
        assertThat(parametersQuantifier).isPresent();
        final var functionBodyValue = FieldValue.ofFieldName(
                parametersQuantifier.get().getFlowedObjectValue(), "input");
        final var userDefinedMacroFunctionBuilder = functionBodyStepBuilder
                .withBodyValue(functionBodyValue);

        final var macroIdentityFunction = userDefinedMacroFunctionBuilder.build();

        assertThat(macroIdentityFunction).isInstanceOf(UserDefinedMacroFunction.class);
        assertThat(macroIdentityFunction.getFunctionName()).isEqualTo("identityFunction");
        assertThat(macroIdentityFunction.getParameterTypes()).singleElement().isEqualTo(STRING_TYPE);

        final var arguments = ImmutableList.of(LiteralValue.ofScalar("hello"));
        assertThat(macroIdentityFunction.encapsulate(CallSiteArguments.ofPositional(arguments)))
                .isEqualTo(arguments.get(0));
    }

    @Test
    void buildFunctionWithMultipleStringParameters() {
        final var functionBodyStepBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName("recordConstructor")
                .addParameter(stringParameter("input1"))
                .addParameter(stringParameter("input2"))
                .seal();
        final var parametersQuantifier = functionBodyStepBuilder.getParametersCorrelation();
        assertThat(parametersQuantifier).isPresent();
        final var functionBodyValue = RecordConstructorValue.ofUnnamed(
                ImmutableList.of(
                        FieldValue.ofFieldName(
                                parametersQuantifier.get().getFlowedObjectValue(), "input1"),
                        FieldValue.ofFieldName(
                                parametersQuantifier.get().getFlowedObjectValue(), "input2")));
        final var userDefinedMacroFunctionBuilder = functionBodyStepBuilder
                .withBodyValue(functionBodyValue);

        final var macroIdentityFunction = userDefinedMacroFunctionBuilder.build();
        assertThat(macroIdentityFunction).isInstanceOf(UserDefinedMacroFunction.class);
        assertThat(macroIdentityFunction.getFunctionName()).isEqualTo("recordConstructor");
        assertThat(macroIdentityFunction.getParameterTypes()).hasSize(2)
                .containsExactly(STRING_TYPE, STRING_TYPE);

        final var arguments = ImmutableList.of(
                LiteralValue.ofScalar("one"), LiteralValue.ofScalar("two"));
        assertThat(macroIdentityFunction.encapsulate(CallSiteArguments.ofPositional(arguments))).isInstanceOf(RecordConstructorValue.class)
                .satisfies(rcv ->
                        ((RecordConstructorValue)rcv).semanticEquals(
                                RecordConstructorValue.ofUnnamed(arguments), AliasMap.emptyMap()));
    }

    @Test
    void buildWithExplicitReturnTypeInjectsPromotion() {
        final var userDefinedMacroFunctionBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName("promoting")
                .setReturnType(DataType.Primitives.LONG.type())
                .seal()
                .withBodyValue(LiteralValue.ofScalar(1));

        final var function = userDefinedMacroFunctionBuilder.build();

        assertThat(function).isInstanceOf(UserDefinedMacroFunction.class);
        assertThat(function.getFunctionName()).isEqualTo("promoting");
        assertThat(function.getParameterTypes()).isEmpty();

        assertThat(function.encapsulate(CallSiteArguments.ofPositional())).isInstanceOf(PromoteValue.class).satisfies(
                v -> assertThat(v.getResultType().getTypeCode()).isEqualTo(Type.TypeCode.LONG)
        );
    }

    @Test
    void setLiteralsThrowsUnsupportedOperationException() {
        final var functionBodyValue = LiteralValue.ofScalar(42);
        final var userDefinedMacroFunctionBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName("functionWithNoArgs")
                .seal()
                .withBodyValue(functionBodyValue);


        assertThatThrownBy(() -> userDefinedMacroFunctionBuilder.setLiterals(Literals.empty()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("macro functions don't support processed literals");
    }

    /**
     * Creates a named string parameter with no default value, mirroring how the DDL visitor models a parameter
     * declaration without a {@code DEFAULT} clause.
     */
    @Nonnull
    private static Expression stringParameter(@Nonnull final String name) {
        return Expression.of(new ThrowsValue(STRING_TYPE), Identifier.of(name));
    }
}
