/*
 * MacroFunctionValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PMacroFunctionValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The class defines a function.
 * The function takes in a list of arguments as QuantifiedObjectValue, of which alias = uniqueId, resultType = argument type.
 * and a body value, executing the function is by replacing the QuantifiedObjectValue in the body with the actual argument.
 */
public class MacroFunctionValue extends AbstractValue{

    @Nonnull
    private final List<Value> argList;

    @Nonnull
    private final Value body;

    private MacroFunctionValue(@Nonnull final List<Value> argList, @Nonnull final Value underlying) {
        this.argList = argList;
        this.body = underlying;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, argList, body);
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return body.getResultType();
    }

    @Nonnull
    public Value getBody() {
        return body;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return body.eval(store, context);
    }

    @Nonnull
    public static MacroFunctionValue of(@Nonnull final List<Value> argList, @Nonnull final Value body) {
        return new MacroFunctionValue(argList, body);
    }

    @Nullable
    public Value call(@Nonnull List<? extends Typed> arguments) {
        // replace the QuantifiedObjectValue in body with arguments
        SemanticException.check(arguments.size() == argList.size(), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES, "argument length doesn't match with function definition");
        TranslationMap.Builder translationMapBuilder = new TranslationMap.Builder();
        for (int i = 0; i < arguments.size(); i++) {
            // check that arguments[i] type matches with argList[i] type
            final int finalI = i;
            SemanticException.check(arguments.get(finalI).getResultType().equals(argList.get(i).getResultType()), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES, "argument type doesn't match with function definition");
            translationMapBuilder.when(((QuantifiedObjectValue) argList.get(i)).getAlias()).then((sourceAlias, leafValue) -> (Value)arguments.get(finalI));
        }
        return body.translateCorrelations(translationMapBuilder.build());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, this.getClass().getCanonicalName());
    }

    @Nonnull
    @Override
    public PMacroFunctionValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        PMacroFunctionValue.Builder builder = PMacroFunctionValue.newBuilder();
        argList.forEach(arg -> builder.addArguments(arg.toValueProto(serializationContext)));
        return builder.setBody(body.toValueProto(serializationContext)).build();
    }


    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setMacroFunctionValue(specificValueProto).build();
    }

    @Nonnull
    public static MacroFunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PMacroFunctionValue functionValue) {
        return new MacroFunctionValue(functionValue.getArgumentsList().stream().map(pvalue -> Value.fromValueProto(serializationContext, pvalue)).collect(Collectors.toList()), Value.fromValueProto(serializationContext, functionValue.getBody()));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        return this;
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PMacroFunctionValue, MacroFunctionValue> {
        @Nonnull
        @Override
        public Class<PMacroFunctionValue> getProtoMessageClass() {
            return PMacroFunctionValue.class;
        }

        @Nonnull
        @Override
        public MacroFunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PMacroFunctionValue functionValue) {
            return MacroFunctionValue.fromProto(serializationContext, functionValue);
        }
    }
}
