/*
 * CurrentTimestampValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PCurrentTimestampValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * A value that evaluates to the evaluation timestamp of the current query, as the number of milliseconds since the
 * Unix epoch.
 * <p>
 * The timestamp is not read from the system clock by this value, instead it is retrieved directly from the child
 * value, which is a {@link ConstantObjectValue} bound to a constant in the {@link EvaluationContext},
 * which represents the evaluation timestamp used across the query.
 */
@API(API.Status.EXPERIMENTAL)
public class CurrentTimestampValue extends AbstractValue implements Value.RangeMatchableValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Current-Timestamp-Value");

    @Nonnull
    private final ConstantObjectValue timestampConstantObjectValue;

    public CurrentTimestampValue(@Nonnull ConstantObjectValue timestampConstantObjectValue) {
        this.timestampConstantObjectValue = timestampConstantObjectValue;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        return timestampConstantObjectValue.eval(store, context);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return timestampConstantObjectValue.getResultType();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(timestampConstantObjectValue);
    }

    @Nonnull
    @Override
    public CurrentTimestampValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        final var child = Iterables.getOnlyElement(newChildren);
        Verify.verify(child instanceof ConstantObjectValue);
        return CurrentTimestampValue.of((ConstantObjectValue)child);
    }

    public static CurrentTimestampValue of(final ConstantObjectValue timestampValue) {
        return new CurrentTimestampValue(timestampValue);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var childTokens = Iterables.getOnlyElement(explainSuppliers).get();
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addFunctionCall("current_timestamp", childTokens.getExplainTokens()));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, timestampConstantObjectValue);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
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
    public PCurrentTimestampValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PCurrentTimestampValue.newBuilder()
                .setChildValue(timestampConstantObjectValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCurrentTimestampValue(toProto(serializationContext)).build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static CurrentTimestampValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PCurrentTimestampValue currentTimestampValueProto) {
        final Value child = Value.fromValueProto(serializationContext, currentTimestampValueProto.getChildValue());
        Verify.verify(child instanceof ConstantObjectValue);
        return new CurrentTimestampValue((ConstantObjectValue)child);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCurrentTimestampValue, CurrentTimestampValue> {
        @Nonnull
        @Override
        public Class<PCurrentTimestampValue> getProtoMessageClass() {
            return PCurrentTimestampValue.class;
        }

        @Nonnull
        @Override
        public CurrentTimestampValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PCurrentTimestampValue currentTimestampValueProto) {
            return CurrentTimestampValue.fromProto(serializationContext, currentTimestampValueProto);
        }
    }
}
