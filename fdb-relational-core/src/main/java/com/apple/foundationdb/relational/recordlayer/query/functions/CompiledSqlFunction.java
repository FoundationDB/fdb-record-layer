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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.References;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.Iterables;
import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.Quantifier.ForEach;

/**
 * This represents a compilable SQL function. If the function is a table function, it is modeled as a join between
 * the function body as defined by the user, and a constant row representing the list of arguments as given in the
 * call site.
 * <br>
 * Note that a SQL function is only compiled once, upon invocation, the call site is created representing the compiled
 * function plan as a leg of a binary join, where
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class CompiledSqlFunction extends UserDefinedFunction implements WithPlanGenerationSideEffects {

    private final RelationalExpression body;

    private final Optional<CorrelationIdentifier> parametersCorrelation;

    private final Literals literals;

    protected CompiledSqlFunction(final String functionName, final List<String> parameterNames,
                                  final List<Type> parameterTypes,
                                  final List<Optional<Value>> parameterDefaults,
                                  final Optional<CorrelationIdentifier> parametersCorrelation,
                                  final RelationalExpression body,
                                  final Literals literals) {
        super(functionName, parameterNames, parameterTypes, parameterDefaults);
        this.parametersCorrelation = parametersCorrelation;
        this.body = body;
        this.literals = literals;
    }

    @Override
    public RecordMetaDataProto.PUserDefinedFunction toProto() {
        throw new RecordCoreException("attempt to serialize compiled SQL function");
    }

    @Override
    public RelationalExpression encapsulate(final CallSiteArguments arguments) {
        if (parametersCorrelation.isEmpty()) {
            // this should never happen.
            Assert.thatUnchecked(arguments.isEmpty(), ErrorCode.INTERNAL_ERROR,
                    "unexpected parameterless function invocation with non-zero arguments");
            return body;
        }
        if (arguments.isNamed()) {
            return encapsulateFromArgumentValues(
                    resolveParameterValuesFromArguments(arguments.asNamedArguments().namedArguments()));
        }
        return encapsulateFromArgumentValues(resolveParameterValuesFromArguments(arguments.getArgumentsList()));
    }

    private RelationalExpression encapsulateFromArgumentValues(final List<Value> resolvedArgumentValues) {
        final var resultBuilder = GraphExpansion.builder();
        for (var paramIdx = 0; paramIdx < getParameterNames().size(); paramIdx++) {
            resultBuilder.addResultColumn(Column.of(
                    Type.Record.Field.of(
                            computeParameterType(paramIdx),
                            Optional.of(getParameterName(paramIdx))),
                    resolvedArgumentValues.get(paramIdx)));
        }
        final var argumentsExpression = resultBuilder.addQuantifier(rangeOfOnePlan()).build().buildSelect();
        return constructTableFunctionExpression(argumentsExpression);
    }

    private RelationalExpression constructTableFunctionExpression(final RelationalExpression argumentsExpression) {
        final var aliasMap = new ToUniqueAliasesTranslationMap();

        final var bodyRef = Reference.initialOf(body);
        final var translatedBodyRef = Iterables.getOnlyElement(References.rebaseGraphs(List.of(bodyRef),
                Memoizer.noMemoization(PlannerStage.INITIAL), aliasMap, false));
        final var bodyQun = Quantifier.forEach(translatedBodyRef);

        final var translatedParamCorrelation = aliasMap.getSnapshotAliasMap()
                .getTargetOrDefault(Assert.optionalUnchecked(parametersCorrelation),
                        Quantifier.uniqueId());
        final var parametersQun = Quantifier.forEach(Reference.initialOf(argumentsExpression), translatedParamCorrelation);

        final var selectBuilder = GraphExpansion.builder()
                .addQuantifier(bodyQun)
                .addQuantifier(parametersQun);
        bodyQun.computeFlowedColumns().forEach(selectBuilder::addResultColumn);
        return selectBuilder.build().buildSelect();
    }

    @Override
    public Literals getAuxiliaryLiterals() {
        return literals;
    }

    /**
     * Creates a quantifier over a logical expression that is {@code range(0,1]}.
     *
     * @return a quantifier over a logical expression that is {@code range(0,1]}.
     */
    private static Quantifier rangeOfOnePlan() {
        final var rangeFunction = new RangeValue.RangeFn();
        final var rangeValue = Assert.castUnchecked(rangeFunction.encapsulate(CallSiteArguments.ofPositional(LiteralValue.ofScalar(1L))),
                StreamingValue.class);
        final var tableFunctionExpression = new TableFunctionExpression(rangeValue);
        return Quantifier.forEach(Reference.initialOf(tableFunctionExpression));
    }

    /**
     * The {@link UserDefinedFunctionBuilder.FinalStepBuilder} that instantiates a {@link CompiledSqlFunction}.
     */
    static final class CompiledSQLFunctionStepBuilder implements UserDefinedFunctionBuilder.FinalStepBuilder {
        private final String name;
        private final RelationalExpression body;
        private final Expressions parameters;
        private final List<Optional<Value>> parameterDefaults;
        private final ForEach parametersQuantifier;
        private Literals literals;

        CompiledSQLFunctionStepBuilder(final String name,
                                       final RelationalExpression body,
                                       final Expressions parameters,
                                       final List<Optional<Value>> parameterDefaults,
                                       @Nullable final ForEach parametersQuantifier,
                                       @Nullable final Type returnType) {
            Assert.isNullUnchecked(returnType, "unsupported explicit return type for compiled SQL function");

            this.name = name;
            this.body = body;
            this.parameters = parameters;
            this.parameterDefaults = parameterDefaults;
            this.parametersQuantifier = parametersQuantifier;
            this.literals = Literals.empty();
        }

        @Override
        public UserDefinedFunctionBuilder.FinalStepBuilder setLiterals(final Literals literals) {
            this.literals = literals;
            return this;
        }

        @Override
        public UserDefinedFunction build() {
            Assert.notNullUnchecked(name);
            Assert.notNullUnchecked(parameterDefaults);
            Assert.notNullUnchecked(parameters);
            return new CompiledSqlFunction(name, parameters.argumentNames(), parameters.underlyingTypes(),
                    parameterDefaults, Optional.ofNullable(parametersQuantifier).map(Quantifier::getAlias), body, literals);
        }
    }
}
