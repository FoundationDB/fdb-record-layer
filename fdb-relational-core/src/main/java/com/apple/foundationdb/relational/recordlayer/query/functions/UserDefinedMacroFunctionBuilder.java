/*
 * UserDefinedMacroFunctionBuilder.java
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

import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedMacroFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;


/**
 * The {@link UserDefinedFunctionBuilder.FinalStepBuilder} that instantiates a {@link UserDefinedMacroFunction}.
 */
final class UserDefinedMacroFunctionBuilder implements UserDefinedFunctionBuilder.FinalStepBuilder {
    @Nonnull
    private final String name;
    @Nonnull
    private final Value bodyValue;
    @Nonnull
    private final Expressions parameters;
    @Nonnull
    private final List<Optional<Value>> parameterDefaults;
    @Nullable
    private final Quantifier.ForEach parametersQuantifier;
    @Nonnull
    private final List<QuantifiedObjectValue> parametersQuantifiedObjectValues;
    @Nullable
    private final Type returnType;

    /**
     * Creates a builder for a macro function.
     * @param name the function name.
     * @param bodyValue the scalar body of the function, correlated to {@code parametersQuantifier}.
     * @param parameters the function parameters, providing their names and types.
     * @param parameterDefaults the default value of each parameter, with {@link Optional#empty()} marking a
     * parameter that has no default.
     * @param parametersQuantifier the quantifier the body's parameter references are correlated to, or {@code null}
     * when the function has no parameters.
     */
    UserDefinedMacroFunctionBuilder(@Nonnull final String name,
                                    @Nonnull final Value bodyValue,
                                    @Nonnull final Expressions parameters,
                                    @Nonnull final List<Optional<Value>> parameterDefaults,
                                    @Nullable final Quantifier.ForEach parametersQuantifier,
                                    @Nullable final Type returnType) {
        this.name = name;
        this.bodyValue = bodyValue;
        this.parameters = parameters;
        this.parameterDefaults = parameterDefaults;
        this.parametersQuantifier = parametersQuantifier;
        this.parametersQuantifiedObjectValues = computeParameterQuantifiedObjectValues();
        this.returnType = returnType;
    }

    /**
     * Always throws: macro functions do not support processed literals.
     * @param literals ignored.
     * @throws UnsupportedOperationException always.
     */
    @Override
    public UserDefinedFunctionBuilder.FinalStepBuilder setLiterals(@Nonnull final Literals literals) {
        throw new UnsupportedOperationException("macro functions don't support processed literals");
    }

    /**
     * Builds the {@link UserDefinedMacroFunction}.
     * <p>
     * Before instantiating the  {@link UserDefinedMacroFunction}, the function's body {@link Value} is rewritten so
     * it is correlated to a per-parameter {@link QuantifiedObjectValue}, and if an explicit return type was set to the
     * function, a {@link PromoteValue} is injected around the body value.
     * </p>
     * @return the constructed macro function.
     */
    @Nonnull
    @Override
    public UserDefinedFunction build() {
        Assert.notNullUnchecked(name);
        Assert.thatUnchecked(parametersQuantifiedObjectValues.size() == parameters.size());
        Assert.thatUnchecked(parameters.size() == parameterDefaults.size());
        final var bodyValue = translateBodyValueParametersCorrelations(parametersQuantifiedObjectValues);
        if (returnType == null || !PromoteValue.isPromotionNeeded(bodyValue.getResultType(), returnType)) {
            return new UserDefinedMacroFunction(
                    name, parametersQuantifiedObjectValues, parameters.argumentNames(), parameterDefaults, bodyValue);
        }
        return new UserDefinedMacroFunction(
                name,
                parametersQuantifiedObjectValues,
                parameters.argumentNames(),
                parameterDefaults,
                PromoteValue.inject(bodyValue, returnType));
    }

    /**
     * Creates one {@link QuantifiedObjectValue} with a unique alias per parameter to serve as the function's
     * parameters; the unique aliases makes it possible to bind the parameters independently during encapsulation.
     * @return a quantified object value for each parameter, in declaration order.
     */
    @Nonnull
    private List<QuantifiedObjectValue> computeParameterQuantifiedObjectValues() {
        ImmutableList.Builder<QuantifiedObjectValue> quantifiedObjectValueBuilder = ImmutableList.builder();
        for (var i = 0; i < parameters.size(); i++) {
            quantifiedObjectValueBuilder.add(
                    QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(),
                            DataTypeUtils.toRecordLayerType(parameters.asList().get(i).getDataType())));
        }
        return quantifiedObjectValueBuilder.build();
    }

    /**
     * Translate the named parameters correlations in the body value to be in terms of the provided
     * quantified object values.
     * <p>
     * For example, if the body value is {@code RCV(q0.X, q0.Y)}, and the provided value list is
     * {@code [QOV(alias1), QOV(alias2)]} it is going to be transformed to be {@code RCV(QOV(alias1), QOV(alias2))}.
     * This is done to simplify encapsulation of user-provided arguments when this macro function is called.
     * </p>
     * @return a {@code Value} that is correlated to the provided quantified object values instead of
     *         {@code this.parameterQuantifier}.
     */
    private Value translateBodyValueParametersCorrelations(@Nonnull final List<QuantifiedObjectValue> parametersQuantifiedObjectValues) {
        if (parametersQuantifiedObjectValues.isEmpty()) {
            return bodyValue;
        }

        ImmutableList.Builder<Column<? extends Value>> columnBuilder = ImmutableList.builder();
        for (var i = 0; i < parameters.size(); i++) {
            columnBuilder.add(Column.of(parameters.asList().get(i).getName().map(Identifier::getName),
                    parametersQuantifiedObjectValues.get(i)));
        }
        RegularTranslationMap translationMap = RegularTranslationMap.builder()
                .when(Assert.notNullUnchecked(parametersQuantifier).getAlias())
                .then((sourceAlias, leafValue) ->
                        RecordConstructorValue.ofColumns(columnBuilder.build()))
                .build();
        return bodyValue.translateCorrelations(translationMap, true);
    }
}
