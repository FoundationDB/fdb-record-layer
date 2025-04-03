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
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SqlFunction extends UserDefinedFunction<Value> {

    @Nonnull
    private final RelationalExpression body;

    private final Optional<CorrelationIdentifier> parametersCorrelation;

    public SqlFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                       @Nonnull final List<Type> parameterTypes,
                       @Nonnull final List<Optional<Value>> parameterDefaults,
                       @Nonnull final Optional<CorrelationIdentifier> parametersCorrelation,
                       @Nonnull final RelationalExpression body) {
        super(functionName, parameterNames, parameterTypes, parameterDefaults);
        this.parametersCorrelation = parametersCorrelation;
        this.body = body;
    }

    @Nonnull
    public RecordMetaDataProto.PUserDefinedFunction toProto(@Nonnull PlanSerializationContext serializationContext) {
        throw new RecordCoreException("attempt to serialize expandable SQL function");
    }

    @Nonnull
    @Override
    public RelationalExpression encapsulate(@Nonnull final List<? extends Typed> arguments) {
        throw new UnsupportedOperationException("this method is not implemented yet");
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
                argumentValue = Assert.optionalUnchecked(getDefaultValue(name), ErrorCode.UNDEFINED_FUNCTION,
                        () -> "could not find function matching the provided arguments");
            }
            final var maybePromotedArgument = PromoteValue.inject(argumentValue, getParameterType(name));
            resultBuilder.addResultColumn(Column.of(Optional.of(name), maybePromotedArgument));
        }
        final var qun = Quantifier.forEach(Reference.of(resultBuilder.addQuantifier(rangeOneFunc()).build().buildSelect()),
                parametersCorrelation.get());
        final var bodyQun = Quantifier.forEach(Reference.of(body));
        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(bodyQun)
                .addQuantifier(qun);
        bodyQun.computeFlowedColumns().forEach(selectBuilder::addResultColumn);
        return selectBuilder.build().buildSelect();
    }

    @Nonnull
    private static Quantifier rangeOneFunc() {
        final var rangeFunction = new RangeValue.RangeFn();
        final var rangeValue = Assert.castUnchecked(rangeFunction.encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L))), StreamingValue.class);
        final var tableFunctionExpression = new TableFunctionExpression(rangeValue);
        return Quantifier.forEach(Reference.of(tableFunctionExpression));
    }

    @Nonnull
    public static StepBuilder newBuilder() {
        return new StepBuilder();
    }

    public static final class StepBuilder {
        @Nullable
        private Type returnType;
        private final ImmutableList.Builder<Expression> parametersBuilder;
        private boolean isDeterministic;
        private String name;

        private StepBuilder() {
            isDeterministic = false;
            this.parametersBuilder = ImmutableList.builder();
        }

        public static final class FinalBuilder {
            @Nonnull
            private final StepBuilder outerBuilder;
            private RelationalExpression body;
            private Quantifier.ForEach qun;
            private final Expressions parameters;

            private FinalBuilder(@Nonnull final StepBuilder outerBuilder, @Nonnull final Expressions parameters) {
                this.outerBuilder = outerBuilder;
                this.parameters = parameters;
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
                    qun = Quantifier.forEach(Reference.of(select));
                }
                return Optional.of(qun);
            }

            @Nonnull
            public FinalBuilder setBody(@Nonnull final RelationalExpression body) {
                this.body = body;
                return this;
            }

            @Nonnull
            public SqlFunction build() {
                final List<Optional<Value>> defaultsValuesList = Streams.stream(parameters.underlying())
                        .map(v -> v instanceof ThrowsValue ? Optional.<Value>empty() : Optional.of(v))
                        .collect(ImmutableList.toImmutableList());
                return new SqlFunction(outerBuilder.name, parameters.names(), parameters.underlyingTypes(),
                        defaultsValuesList, getParametersCorrelation().map(Quantifier::getAlias), body);
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
        public StepBuilder setDeterministic(final boolean isDeterministic) {
            this.isDeterministic = isDeterministic;
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
            Assert.thatUnchecked(parameters.allNamed() /*|| parameters.allUnnamed()*/, ErrorCode.UNSUPPORTED_OPERATION,
                    "unnamed arguments is not supported");
            return new FinalBuilder(this, parameters);

            // final var qun = getParametersCorrelation();

//            LogicalOperator bodyOp = body;
//            if (qun.isPresent()) {
//                final var selectBuilder = GraphExpansion.builder()
//                        .addQuantifier(body.getQuantifier())
//                        .addQuantifier(qun.get())
//                        .addAllResultValues(body.getQuantifier().getFlowedValues()).build();
//                final var topQun = Quantifier.forEach(Reference.of(selectBuilder.buildSelect()));
//                final var pulledUpValues = body.getOutput().rewireQov(topQun.getFlowedObjectValue());
//                bodyOp = LogicalOperator.newOperatorWithPreservedExpressionNames(pulledUpValues, topQun);
//            }

//            final var rlType = DataTypeUtils.toRecordLayerType(type);
//            var body = visit(ctx.routineBody());
//            Assert.thatUnchecked(body instanceof Value || body instanceof LogicalOperator);
//            if (body instanceof Value) {
//                final var value = (Value)body;
//                final var requiresPromotion = PromoteValue.isPromotionNeeded(value.getResultType(), rlType);
//                if (requiresPromotion) {
//                    body = PromoteValue.inject(value, rlType);
//                }
//            } else {
//                final var logicalOperator = (LogicalOperator)body;
//
//            }
//            return new SqlFunction(isDeterministic, parametersBuilder, name, getParametersCorrelation().map(Quantifier::getAlias), body.getQuantifier());
        }
    }
}
