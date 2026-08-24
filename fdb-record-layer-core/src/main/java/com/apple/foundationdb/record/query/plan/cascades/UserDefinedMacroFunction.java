/*
 * UserDefinedMacroFunction.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.planprotos.PUserDefinedFunctionArgumentDefaultValue;
import com.apple.foundationdb.record.planprotos.PUserDefinedMacroFunction;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * UserDefinedMacroFunction that expands a body (referring to parameters) into a {@link Value} (through encapsulation) call site.
 */
public class UserDefinedMacroFunction extends UserDefinedFunction {
    @Nonnull
    private final List<CorrelationIdentifier> parameterIdentifiers;
    @Nonnull
    private final Value bodyValue;

    public UserDefinedMacroFunction(@Nonnull final String functionName,
                                    @Nonnull final List<QuantifiedObjectValue> parameters,
                                    @Nonnull final Value bodyValue) {
        super(functionName, parameters.stream().map(QuantifiedObjectValue::getResultType).collect(ImmutableList.toImmutableList()));
        this.parameterIdentifiers = parameters.stream().map(QuantifiedObjectValue::getAlias).collect(ImmutableList.toImmutableList());
        this.bodyValue = bodyValue;
    }

    public UserDefinedMacroFunction(@Nonnull final String functionName,
                                    @Nonnull final List<QuantifiedObjectValue> parameters,
                                    @Nonnull final List<String> parameterNames,
                                    @Nonnull final List<Optional<Value>> parameterDefaults,
                                    @Nonnull final Value bodyValue) {
        super(functionName, parameterNames,
                parameters.stream().map(QuantifiedObjectValue::getResultType).collect(ImmutableList.toImmutableList()),
                parameterDefaults);
        this.parameterIdentifiers = parameters.stream().map(QuantifiedObjectValue::getAlias).collect(ImmutableList.toImmutableList());
        this.bodyValue = bodyValue;
    }

    @Nonnull
    @Override
    public Value encapsulate(@Nonnull final CallSiteArguments arguments) {
        if (arguments.isNamed()) {
            return encapsulateFromArgumentValues(
                    resolveParameterValuesFromArguments(arguments.asNamedArguments().namedArguments()));
        }
        return encapsulateFromArgumentValues(resolveParameterValuesFromArguments(arguments.getArgumentsList()));
    }

    private Value encapsulateFromArgumentValues(@Nonnull List<Value> resolvedArgumentValues) {
        final RegularTranslationMap.Builder translationMapBuilder = TranslationMap.regularBuilder();
        for (var paramIdx = 0; paramIdx < resolvedArgumentValues.size(); paramIdx++) {
            final int finalParamIdx = paramIdx;
            translationMapBuilder.when(parameterIdentifiers.get(paramIdx))
                    .then((sourceAlias, leafValue) -> resolvedArgumentValues.get(finalParamIdx));
        }
        return bodyValue.translateCorrelations(translationMapBuilder.build());
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.PUserDefinedFunction toProto() {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        PUserDefinedMacroFunction.Builder builder = PUserDefinedMacroFunction.newBuilder();
        for (int i = 0; i < parameterTypes.size(); i++) {
            builder.addArguments(
                    QuantifiedObjectValue.of(parameterIdentifiers.get(i), parameterTypes.get(i))
                            .toValueProto(serializationContext));
            if (!hasNamedParameters()) {
                continue;
            }

            builder.addArgumentNames(getParameterName(i));
            final var defaultValueForArgument = getDefaultValue(i);
            final var defaultArgumentBuilder = PUserDefinedFunctionArgumentDefaultValue.newBuilder()
                    .setIsProvided(defaultValueForArgument.isPresent());
            defaultValueForArgument.ifPresent(defaultValue ->
                    defaultArgumentBuilder.setValue(defaultValue.toValueProto(serializationContext)));
            builder.addDefaultArgumentValues(defaultArgumentBuilder);

        }
        return RecordMetaDataProto.PUserDefinedFunction.newBuilder()
                .setUserDefinedMacroFunction(builder
                        .setFunctionName(functionName)
                        .setBody(bodyValue.toValueProto(serializationContext)))
                .build();
    }

    @Nonnull
    public static UserDefinedMacroFunction fromProto(@Nonnull final PUserDefinedMacroFunction function) {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final var parametersQuantifiedObjectValues = function.getArgumentsList().stream()
                .map(pvalue -> ((QuantifiedObjectValue)Value.fromValueProto(serializationContext, pvalue)))
                .collect(ImmutableList.toImmutableList());
        final var bodyValue = Value.fromValueProto(serializationContext, function.getBody());
        final List<String> parameterNames = function.getArgumentNamesList();
        if (parameterNames.isEmpty()) {
            return new UserDefinedMacroFunction(function.getFunctionName(), parametersQuantifiedObjectValues, bodyValue);
        }
        final List<Optional<Value>> parameterDefaults = function.getDefaultArgumentValuesList().stream().map(
                pDefaultArgumentValue ->
                        !pDefaultArgumentValue.getIsProvided() ? Optional.<Value>empty() :
                        Optional.of(Value.fromValueProto(serializationContext, pDefaultArgumentValue.getValue())))
                .collect(ImmutableList.toImmutableList());
        return new UserDefinedMacroFunction(function.getFunctionName(), parametersQuantifiedObjectValues, parameterNames, parameterDefaults, bodyValue);
    }
}
