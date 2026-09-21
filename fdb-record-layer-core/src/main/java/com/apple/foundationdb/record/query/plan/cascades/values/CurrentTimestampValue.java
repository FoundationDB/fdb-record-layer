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
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

/**
 * A value that evaluates to the evaluation timestamp of the current query, as the number of milliseconds since the
 * Unix epoch.
 * <p>
 * The timestamp is not read from the system clock by this value, instead it is retrieved from the
 * {@link EvaluationContext}, via the method {@link EvaluationContext#getEvaluationTimestamp()} to ensure that every
 * evaluation within the same query returns the same value. The actual timestamp value is not stored as part of this
 * value to ensure that cached plans are reusable regardless of the evaluation timestamp.
 */
@API(API.Status.EXPERIMENTAL)
public class CurrentTimestampValue extends AbstractValue implements LeafValue, Value.RangeMatchableValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Current-Timestamp-Value");

    public CurrentTimestampValue() {
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var evaluationTimestamp = context.getEvaluationTimestamp();
        Verify.verify(evaluationTimestamp != null);
        return evaluationTimestamp.toEpochMilli();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return List.of();
    }

    @Nonnull
    @Override
    public CurrentTimestampValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        // No children, so just return this
        return this;
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return this;
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("current_timestamp"));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
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
        return PCurrentTimestampValue.newBuilder().build();
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
        return new CurrentTimestampValue();
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

    /**
     * The {@code current_timestamp} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class CurrentTimestampFn extends BuiltInFunction<Value> {
        public CurrentTimestampFn() {
            super("current_timestamp",
                    List.of(),
                    (ignored, arguments) -> new CurrentTimestampValue());
        }
    }
}
