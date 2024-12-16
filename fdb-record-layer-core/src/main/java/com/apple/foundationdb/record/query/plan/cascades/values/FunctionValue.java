/*
 * FunctionValue.java
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
import com.apple.foundationdb.record.planprotos.PFunctionValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FunctionValue extends AbstractValue{
    @Nonnull
    private final String functionName;

    @Nonnull
    private final Iterable<? extends Value> children; // QuantifiedObjectValue

    @Nonnull
    private final Value underlying;

    @Nonnull
    private final Type resultType;

    private FunctionValue(@Nonnull String functionName, @Nonnull final Iterable<? extends Value> children, @Nonnull final Value underlying, @Nonnull final Type resultType) {
        this.functionName = functionName;
        this.children = children;
        this.underlying = underlying;
        this.resultType = resultType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, this.getClass().getCanonicalName(), children);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    public Value getUnderlying() {
        return underlying;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return call(StreamSupport.stream(children.spliterator(), false).map(c -> c.eval(store, context)).collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public Value withChildren(Iterable<? extends Value> newChildren) {
        return new FunctionValue(functionName, newChildren, underlying, resultType);
    }

    @Nonnull
    public static FunctionValue of(@Nonnull String functionName, @Nonnull final Iterable<? extends Value> children, @Nonnull final Value underlying, @Nonnull final Type resultType) {
        return new FunctionValue(functionName, children, underlying, resultType);
    }

    @Nullable
    public Value call(@Nonnull List<Object> arguments) {
        // replace the children in underlying with arguments
        /*
        TranslationMap.Builder translationMapBuilder = new TranslationMap.Builder();
        // assert arguments size = children size
        var argumentIter = arguments.iterator();
        for (Value v: getChildren()) {
            translationMapBuilder.when(((QuantifiedObjectValue)v).getAlias()).then((sourceAlias, leafValue) -> (Value)argumentIter.next());
        }
        return translateCorrelations(translationMapBuilder.build());
         */
        return underlying.replace((v) -> {
            if ((v instanceof QuantifiedObjectValue) && (v.getResultType().equals(resultType))) {
                return (Value)arguments.get(0);
            } else {
                return v;
            }
        });
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, this.getClass().getCanonicalName());
    }

    @Nonnull
    @Override
    public PFunctionValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        List<PValue> childValueList = new LinkedList<>();
        children.forEach(c -> childValueList.add(c.toValueProto(serializationContext)));
        PFunctionValue.Builder builder = PFunctionValue.newBuilder();
        builder.setFunctionName(functionName);
        builder.setResultType(resultType.toTypeProto(serializationContext));
        builder.addAllChildren(childValueList);
        builder.setUnderlying(underlying.toValueProto(serializationContext));
        return builder.build();
    }


    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var specificValueProto = toProto(serializationContext);
        return PValue.newBuilder().setFunctionValue(specificValueProto).build();
    }

    @Nonnull
    public static FunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PFunctionValue functionValue) {
        List<Value> children = new ArrayList<>();
        for (PValue pValue: functionValue.getChildrenList()) {
            children.add(Value.fromValueProto(serializationContext, pValue));
        }
        return new FunctionValue(functionValue.getFunctionName(), children, Value.fromValueProto(serializationContext, functionValue.getUnderlying()), Type.fromTypeProto(serializationContext, functionValue.getResultType()));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PFunctionValue, FunctionValue> {
        @Nonnull
        @Override
        public Class<PFunctionValue> getProtoMessageClass() {
            return PFunctionValue.class;
        }

        @Nonnull
        @Override
        public FunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PFunctionValue functionValue) {
            return FunctionValue.fromProto(serializationContext, functionValue);
        }
    }
}

