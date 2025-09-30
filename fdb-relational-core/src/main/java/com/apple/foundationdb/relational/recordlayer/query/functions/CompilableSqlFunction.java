/*
 * SqlFunction.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This represents a compilable SQL function. If the function is a table function, it is modeled as a join between
 * the function body as defined by the user, and a constant row representing the list of arguments as given in the
 * call site.
 * <br>
 * Note that a SQL function is only compiled once, upon invocation, the call site is created representing the compiled
 * function plan as a leg of a binary join, where
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class CompilableSqlFunction extends UserDefinedFunction implements WithPlanGenerationSideEffects {

    @Nonnull
    private final RelationalExpression body;

    @Nonnull
    private final Optional<CorrelationIdentifier> parametersCorrelation;

    @Nonnull
    private final Literals literals;

    protected CompilableSqlFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                                    @Nonnull final List<Type> parameterTypes,
                                    @Nonnull final List<Optional<? extends Typed>> parameterDefaults,
                                    @Nonnull final Optional<CorrelationIdentifier> parametersCorrelation,
                                    @Nonnull final RelationalExpression body,
                                    @Nonnull final Literals literals) {
        super(functionName, parameterNames, parameterTypes, parameterDefaults);
        this.parametersCorrelation = parametersCorrelation;
        this.body = body;
        this.literals = literals;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.PUserDefinedFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("attempt to serialize compiled SQL function");
    }

    @Nonnull
    @Override
    public RelationalExpression encapsulate(@Nonnull final List<? extends Typed> arguments) {
        if (parametersCorrelation.isEmpty()) {
            // this should never happen.
            Assert.thatUnchecked(arguments.isEmpty(), ErrorCode.INTERNAL_ERROR,
                    "unexpected parameterless function invocation with non-zero arguments");
            return body;
        }
        final var parametersCount = getParameterNames().size();
        Assert.thatUnchecked(arguments.size() <= parametersCount, ErrorCode.UNDEFINED_FUNCTION,
                () -> "could not find function matching the provided arguments");
        for (var missingArgIndex = arguments.size(); missingArgIndex < parametersCount; missingArgIndex++) {
            Assert.thatUnchecked(hasDefaultValue(missingArgIndex), ErrorCode.UNDEFINED_FUNCTION,
                    () -> "could not find function matching the provided arguments");
        }
        final var resultBuilder = GraphExpansion.builder();
        for (var paramIdx = 0; paramIdx < parametersCount; paramIdx++) {
            Value argumentValue;
            if (paramIdx >= arguments.size()) {
                argumentValue = Assert.castUnchecked(Assert.optionalUnchecked(getDefaultValue(paramIdx)), Value.class);
            } else {
                final var providedArgValue = Assert.castUnchecked(arguments.get(paramIdx), Value.class);
                final var isPromotionNeeded = PromoteValue.isPromotionNeeded(providedArgValue.getResultType(), computeParameterType(paramIdx));
                Assert.thatUnchecked(!isPromotionNeeded || PromoteValue.isPromotable(providedArgValue.getResultType(), computeParameterType(paramIdx)),
                        ErrorCode.UNDEFINED_FUNCTION, () -> "could not find function matching the provided arguments");
                argumentValue = PromoteValue.inject(providedArgValue, computeParameterType(paramIdx));
            }
            resultBuilder.addResultColumn(Column.of(Optional.of(getParameterName(paramIdx)), argumentValue));
        }
        final var qun = Quantifier.forEach(Reference.initialOf(resultBuilder.addQuantifier(rangeOfOnePlan()).build().buildSelect()),
                parametersCorrelation.get());
        final var bodyQun = Quantifier.forEach(Reference.initialOf(body));
        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(bodyQun)
                .addQuantifier(qun);
        bodyQun.computeFlowedColumns().forEach(selectBuilder::addResultColumn);
        return selectBuilder.build().buildSelect();
    }

    @Nonnull
    @Override
    public RelationalExpression encapsulate(@Nonnull final Map<String, ? extends Typed> namedArguments) {
        if (parametersCorrelation.isEmpty()) {
            // this should never happen.
            Assert.thatUnchecked(namedArguments.isEmpty(), ErrorCode.INTERNAL_ERROR,
                    "unexpected parameterless function invocation with non-zero arguments");
            return body;
        }
        Assert.thatUnchecked(hasNamedParameters(), ErrorCode.INTERNAL_ERROR,
                "unexpected invocation of function with named arguments");
        final var resultBuilder = GraphExpansion.builder();
        for (final var name : getParameterNames()) {
            Value argumentValue;
            if (namedArguments.containsKey(name)) {
                argumentValue = Assert.castUnchecked(namedArguments.get(name), Value.class);
            } else {
                argumentValue = Assert.castUnchecked(Assert.optionalUnchecked(getDefaultValue(name), ErrorCode.UNDEFINED_FUNCTION,
                        () -> "could not find function matching the provided arguments"), Value.class);
            }
            final var isPromotionNeeded = PromoteValue.isPromotionNeeded(argumentValue.getResultType(), computeParameterType(name));
            Assert.thatUnchecked(!isPromotionNeeded || PromoteValue.isPromotable(argumentValue.getResultType(), computeParameterType(name)),
                    ErrorCode.UNDEFINED_FUNCTION, () -> "could not find function matching the provided arguments");
            final var maybePromotedArgument = PromoteValue.inject(argumentValue, computeParameterType(name));
            resultBuilder.addResultColumn(Column.of(Optional.of(name), maybePromotedArgument));
        }
        final var qun = Quantifier.forEach(Reference.initialOf(resultBuilder.addQuantifier(rangeOfOnePlan()).build().buildSelect()),
                parametersCorrelation.get());
        final var bodyQun = Quantifier.forEach(Reference.initialOf(body));
        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(bodyQun)
                .addQuantifier(qun);
        bodyQun.computeFlowedColumns().forEach(selectBuilder::addResultColumn);
        return selectBuilder.build().buildSelect();
    }

    @Nonnull
    @Override
    public Literals getAuxiliaryLiterals() {
        return literals;
    }

    /**
     * Creates a quantifier over a logical expression that is {@code range(0,1]}.
     *
     * @return a quantifier over a logical expression that is {@code range(0,1]}.
     */
    @Nonnull
    private static Quantifier rangeOfOnePlan() {
        final var rangeFunction = new RangeValue.RangeFn();
        final var rangeValue = Assert.castUnchecked(rangeFunction.encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L))),
                StreamingValue.class);
        final var tableFunctionExpression = new TableFunctionExpression(rangeValue);
        return Quantifier.forEach(Reference.initialOf(tableFunctionExpression));
    }

    /**
     * Creates a new builder of {@link SqlFunctionCatalog}.
     *
     * @return new builder of {@link SqlFunctionCatalog}.
     */
    @Nonnull
    public static StepBuilder newBuilder() {
        return new StepBuilder();
    }

    public static final class StepBuilder {
        private final ImmutableList.Builder<Expression> parametersBuilder;
        private String name;

        private StepBuilder() {
            this.parametersBuilder = ImmutableList.builder();
        }

        public static final class FinalBuilder {
            @Nonnull
            private final StepBuilder outerBuilder;
            private RelationalExpression body;
            private Quantifier.ForEach qun;
            private final Expressions parameters;
            private Literals literals;

            private FinalBuilder(@Nonnull final StepBuilder outerBuilder, @Nonnull final Expressions parameters) {
                this.outerBuilder = outerBuilder;
                this.parameters = parameters;
                this.literals = Literals.empty();
            }

            @Nonnull
            public Optional<Quantifier> getParametersCorrelation() {
                if (parameters.isEmpty()) {
                    return Optional.empty();
                }
                if (qun == null) {
                    final var select = GraphExpansion.builder()
                            .addAllResultColumns(parameters.underlyingAsColumns())
                            .build().buildSelect();
                    qun = Quantifier.forEach(Reference.initialOf(select));
                }
                return Optional.of(qun);
            }

            @Nonnull
            public FinalBuilder setBody(@Nonnull final RelationalExpression body) {
                this.body = body;
                return this;
            }

            @Nonnull
            public FinalBuilder setLiterals(@Nonnull final Literals literals) {
                this.literals = literals;
                return this;
            }

            @Nonnull
            public CompilableSqlFunction build() {
                final List<Optional<? extends Typed>> defaultsValuesList = Streams.stream(parameters.underlying())
                        .map(v -> v instanceof ThrowsValue ? Optional.<Value>empty() : Optional.of(v))
                        .collect(ImmutableList.toImmutableList());
                return new CompilableSqlFunction(outerBuilder.name, parameters.argumentNames(), parameters.underlyingTypes(),
                        defaultsValuesList, getParametersCorrelation().map(Quantifier::getAlias), body, literals);
            }
        }

        @Nonnull
        public StepBuilder setReturnType(@Nonnull final Type returnType) {
            // todo: if return type is defined, use it to perform necessary explicit promotions, if any.
            throw new UnsupportedOperationException("unsupported explicit return type");
        }

        @Nonnull
        public StepBuilder addParameter(@Nonnull final Expression expression) {
            parametersBuilder.add(expression);
            return this;
        }

        @Nonnull
        public StepBuilder addAllParameters(@Nonnull final Expressions expressions) {
            parametersBuilder.addAll(expressions);
            return this;
        }

        @Nonnull
        public StepBuilder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public FinalBuilder seal() {
            Assert.notNullUnchecked(name);
            final var parameters = Expressions.of(parametersBuilder.build());
            // todo: support unnamed parameters.
            Assert.thatUnchecked(parameters.allNamedArguments() /*|| parameters.allUnnamed()*/, ErrorCode.UNSUPPORTED_OPERATION,
                    "unnamed arguments is not supported");
            return new FinalBuilder(this, parameters);
        }
    }
}
