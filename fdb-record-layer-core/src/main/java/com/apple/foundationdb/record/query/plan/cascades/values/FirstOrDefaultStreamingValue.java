/*
 * FirstOrDefaultValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PFirstOrDefaultStreamingValue;
import com.apple.foundationdb.record.planprotos.PFirstOrDefaultValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that returns the first element of the streaming {@link Value} that is passed in. If the streaming value returns
 * and empty stream or the first item is null, a default value of the same type is returned.
 *
 * @see RecordCursor#first() semantics for more information.
 */
@API(API.Status.EXPERIMENTAL)
public class FirstOrDefaultStreamingValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("First-Or-Default-Streaming-Value");

    @Nonnull
    private final StreamingValue childValue;
    @Nonnull
    private final Value onEmptyResultValue;
    @Nonnull
    private final Supplier<List<Value>> childrenSupplier;
    @Nonnull
    private final Type resultType;

    public FirstOrDefaultStreamingValue(@Nonnull final StreamingValue childValue, @Nonnull final Value onEmptyResultValue) {
        this.childValue = childValue;
        this.onEmptyResultValue = onEmptyResultValue;
        this.childrenSupplier = () -> ImmutableList.of(childValue, onEmptyResultValue);
        this.resultType = Objects.requireNonNull(childValue.getResultType());
    }

    @Nonnull
    @Override
    public List<? extends Value> computeChildren() {
        return childrenSupplier.get();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenList = ImmutableList.copyOf(newChildren);
        Verify.verify(newChildrenList.size() == 2);
        Verify.verify(newChildrenList.get(0) instanceof StreamingValue);
        return new FirstOrDefaultStreamingValue((StreamingValue)newChildrenList.get(0), newChildrenList.get(1));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    public Value getOnEmptyResultValue() {
        return onEmptyResultValue;
    }

    @Override
    @SpotBugsSuppressWarnings({"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", "NP_NONNULL_PARAM_VIOLATION"})
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var childResult = childValue.evalAsStream(store, context, null, null).first().join();
        if (childResult.isPresent()) {
            return childResult.get();
        } else {
            return onEmptyResultValue.eval(store, context);
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, childValue, onEmptyResultValue);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("firstOrDefault",
                Value.explainFunctionArguments(explainSuppliers)));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PFirstOrDefaultValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PFirstOrDefaultValue.newBuilder()
                .setChildValue(childValue.toValueProto(serializationContext))
                .setOnEmptyResultValue(onEmptyResultValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setFirstOrDefaultValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static FirstOrDefaultStreamingValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PFirstOrDefaultStreamingValue firstOrDefaultStreamingValueProto) {
        final var value = Value.fromValueProto(serializationContext, Objects.requireNonNull(firstOrDefaultStreamingValueProto.getChildValue()));
        if (!(value instanceof StreamingValue)) {
            throw new RecordCoreException("invalid value, expecting streaming value").addLogInfo(LogMessageKeys.VALUE, value);
        }
        return new FirstOrDefaultStreamingValue((StreamingValue)value,
                Value.fromValueProto(serializationContext, Objects.requireNonNull(firstOrDefaultStreamingValueProto.getOnEmptyResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PFirstOrDefaultStreamingValue, FirstOrDefaultStreamingValue> {
        @Nonnull
        @Override
        public Class<PFirstOrDefaultStreamingValue> getProtoMessageClass() {
            return PFirstOrDefaultStreamingValue.class;
        }

        @Nonnull
        @Override
        public FirstOrDefaultStreamingValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PFirstOrDefaultStreamingValue firstOrDefaultValueStreamingProto) {
            return FirstOrDefaultStreamingValue.fromProto(serializationContext, firstOrDefaultValueStreamingProto);
        }
    }
}
