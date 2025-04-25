/*
 * CompilableSqlFunction.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.CompilableRoutine;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Parameter;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class CompilableSqlTableFunction implements CompilableRoutine<Value, LogicalOperator> {

    private final boolean isDeterministic;

    @Nonnull
    private final List<RecordLayerParameter> parameters;

    // might not be needed.
    @Nonnull
    private final String name;

    @Nonnull
    private final Quantifier body;

    @Nonnull
    private final Optional<CorrelationIdentifier> parametersCorrelation;

    public CompilableSqlTableFunction(final boolean isDeterministic,
                                      @Nonnull final List<RecordLayerParameter> parameters,
                                      @Nonnull final String name,
                                      @Nonnull final Optional<CorrelationIdentifier> parametersCorrelation,
                                      @Nonnull final Quantifier body) {
        this.isDeterministic = isDeterministic;
        this.parameters = ImmutableList.copyOf(parameters);
        this.name = name;
        this.parametersCorrelation = parametersCorrelation;
        this.body = body;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return isDeterministic;
    }

    @Override
    public boolean returnsNullOnNullInput() {
        return false;
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return DataTypeUtils.toRelationalType(body.getFlowedObjectType());
    }

    @Nonnull
    @Override
    public List<? extends Parameter> getParameters() {
        return parameters;
    }

    @Nonnull
    @Override
    public Parameter getParameter(final int index) {
        return parameters.get(index);
    }

    @Nonnull
    @Override
    public Parameter getParameter(@Nonnull final String name) {
        return parameters.stream().filter(p -> p.isNamed() && p.getName().equals(name))
                .findFirst().orElseThrow();
    }

    @Nonnull
    @Override
    public Language getLanguage() {
        return Language.SQL;
    }

    @Nonnull
    @Override
    public LogicalOperator compile(@Nonnull final Map<String, Value> namedArgMap) {
        if (parametersCorrelation.isEmpty()) {
            return LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(body), body);
        }
        final var select = GraphExpansion.builder();
        for (final var parameter : parameters) {
            Value argumentValue;
            if (parameter.isNamed()) {
                final var name = parameter.getName();
                if (namedArgMap.containsKey(name)) {
                    argumentValue = namedArgMap.get(name);
                } else {
                    Assert.thatUnchecked(parameter.hasDefaultValue(), ErrorCode.UNDEFINED_FUNCTION,
                            "could not find function matching the provided arguments");
                    argumentValue = parameter.getDefaultValue();
                }
            } else {
                Assert.thatUnchecked(parameter.hasDefaultValue(), ErrorCode.UNDEFINED_FUNCTION,
                        "could not find function matching the provided arguments");
                argumentValue = parameter.getDefaultValue();
            }
            select.addResultColumn(Column.of(Optional.of(parameter.getName()), argumentValue));
        }
        final var qun = Quantifier.forEach(Reference.of(select.build().buildSelect()), parametersCorrelation.get());
        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(body)
                .addQuantifier(qun)
                .addAllResultValues(body.getFlowedValues()).build();
        final var topQun = Quantifier.forEach(Reference.of(selectBuilder.buildSelect()));
        return LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(topQun), topQun);
    }

    @Nonnull
    @Override
    public LogicalOperator compile(@Nonnull final List<Optional<Value>> args) {
        // no var-arg, overloading support yet.
        Assert.thatUnchecked(args.size() == parameters.size(), ErrorCode.UNDEFINED_FUNCTION,
                "could not find function matching the provided arguments");
        if (parametersCorrelation.isEmpty()) {
            return LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(body), body);
        }
        final var select = GraphExpansion.builder();
        for (int i = 0; i < args.size(); i++) {
            final var argument = args.get(i);
            final var parameter = parameters.get(i);
            Value argumentValue;
            if (argument.isPresent()) {
                argumentValue = argument.get();
            } else {
                Assert.thatUnchecked(parameter.hasDefaultValue(), ErrorCode.UNDEFINED_FUNCTION,
                        "could not find function matching the provided arguments");
                argumentValue = parameter.getDefaultValue();
            }
            select.addResultColumn(Column.of(Optional.of(parameter.getName()), argumentValue));
        }
        final var qun = Quantifier.forEach(Reference.of(select.build().buildSelect()), parametersCorrelation.get());
        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(body)
                .addQuantifier(qun)
                .addAllResultValues(body.getFlowedValues()).build();
        final var topQun = Quantifier.forEach(Reference.of(selectBuilder.buildSelect()));
        return LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(topQun), topQun);
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        @Nullable
        private Type returnType;
        private final List<RecordLayerParameter> parameters;
        private boolean isDeterministic;
        private String name;
        private LogicalOperator body;
        private Quantifier.ForEach qun;

        private Builder() {
            isDeterministic = false;
            this.parameters = new ArrayList<>();
        }

        @Nonnull
        public Builder setReturnType(@Nonnull final Type returnType) {
            this.returnType = returnType;
            return this;
        }

        @Nonnull
        public Builder addParameter(@Nonnull final RecordLayerParameter parameter) {
            parameters.add(parameter);
            return this;
        }

        @Nonnull
        public Builder addParameters(@Nonnull final List<RecordLayerParameter> parameters) {
            parameters.forEach(this::addParameter);
            return this;
        }

        @Nonnull
        public Optional<Quantifier> getParametersCorrelation() {
            if (parameters.isEmpty()) {
                return Optional.empty();
            }
            if (qun == null) {
                final var select = GraphExpansion.builder();
                for (final var parameter : parameters) {
                    Verify.verify(parameter.isNamed(), "unnamed parameters are not yet supported");
                    select.addResultColumn(Column.of(Optional.of(parameter.getName()), parameter.hasDefaultValue()
                                                                                       ? parameter.getDefaultValue()
                                                                                       : new ThrowsValue(DataTypeUtils.toRecordLayerType(parameter.getDataType()))));
                }
                qun = Quantifier.forEach(Reference.of(select.build().buildSelect()));
            }
            return Optional.of(qun);
        }

        @Nonnull
        public Builder setDeterministic(final boolean isDeterministic) {
            this.isDeterministic = isDeterministic;
            return this;
        }

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setBody(@Nonnull final LogicalOperator body) {
            this.body = body;
            return this;
        }

        @Nonnull
        public CompilableSqlTableFunction build() {
            Assert.notNullUnchecked(name);
            if (returnType != null) {
                throw new UnsupportedOperationException("not supported");
            }
            return new CompilableSqlTableFunction(isDeterministic, parameters, name,
                    getParametersCorrelation().map(Quantifier::getAlias), body.getQuantifier());
        }
    }
}
