/*
 * MacroFunction.java
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

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.SerializableFunction;
import com.apple.foundationdb.record.planprotos.PMacroFunctionValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MacroFunction that expands a body (referring to parameters) into a {@link Value} (through encapsulation) call site.
 */
public class MacroFunction implements SerializableFunction {
    @Nonnull
    private final String functionName;

    @Nonnull
    private final List<Type> parameterTypes;
    @Nonnull
    private final Value bodyValue;
    private final List<CorrelationIdentifier> parameterIdentifiers;

    public MacroFunction(@Nonnull final String functionName, @Nonnull final List<QuantifiedObjectValue> parameters, @Nonnull final Value bodyValue) {
        this.functionName = functionName;
        this.parameterTypes = parameters.stream().map(QuantifiedObjectValue::getResultType).collect(Collectors.toUnmodifiableList());
        this.parameterIdentifiers = parameters.stream().map(QuantifiedObjectValue::getAlias).collect(Collectors.toList());
        this.bodyValue = bodyValue;
    }

    @Nonnull
    public Value encapsulate(@Nonnull List<? extends Typed> arguments) {
        // replace the QuantifiedObjectValue in body with arguments
        SemanticException.check(arguments.size() == parameterTypes.size(), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES, "argument length doesn't match with function definition");
        TranslationMap.Builder translationMapBuilder = new TranslationMap.Builder();
        for (int i = 0; i < arguments.size(); i++) {
            // check that arguments[i] type matches with parameterTypes[i]
            final int finalI = i;
            SemanticException.check(arguments.get(finalI).getResultType().equals(parameterTypes.get(i)), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES, "argument type doesn't match with function definition");
            translationMapBuilder.when(parameterIdentifiers.get(finalI)).then((sourceAlias, leafValue) -> (Value)arguments.get(finalI));
        }
        return bodyValue.translateCorrelations(translationMapBuilder.build());
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.SerializableFunction toProto() {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        PMacroFunctionValue.Builder builder = PMacroFunctionValue.newBuilder();
        for (int i = 0; i < parameterTypes.size(); i++) {
            builder.addArguments(QuantifiedObjectValue.of(parameterIdentifiers.get(i), parameterTypes.get(i)).toValueProto(serializationContext));
        }
        return RecordMetaDataProto.SerializableFunction.newBuilder()
                .setMacroFunction(builder
                        .setFunctionName(functionName)
                        .setBody(bodyValue.toValueProto(serializationContext)))
                .build();

    }

    @Nonnull
    public static MacroFunction fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PMacroFunctionValue functionValue) {
        return new MacroFunction(
                functionValue.getFunctionName(),
                functionValue.getArgumentsList().stream().map(pvalue -> ((QuantifiedObjectValue)Value.fromValueProto(serializationContext, pvalue))).collect(Collectors.toList()),
                Value.fromValueProto(serializationContext, functionValue.getBody()));
    }
}
