/*
 * NotProvidedValue.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PNotProvidedValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A placeholder for a field that a scan does not provide, for example a column an index entry does not carry, in a
 * value describing what the scan provides (such as the record a covering scan produces).
 *
 * <p>Its value is unknown, not {@code null}: it is equal to nothing but itself, so a value tree containing one can never
 * be equal to another tree. {@link com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap}
 * relies on that to discard such candidate subtrees up front instead of searching for a match it can never find.
 *
 * <p>It can be serialized as part of a plan that describes what it provides, but it cannot be evaluated. Values that are
 * evaluated, such as the index-entry-to-record value of
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan}, describe what the decoder writes,
 * an unset field reading as {@code null}, and use a {@link NullValue} instead (see
 * {@link com.apple.foundationdb.record.query.plan.cascades.IndexEntryToRecordValueHelper}).
 */
@API(API.Status.EXPERIMENTAL)
public class NotProvidedValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Provided-Value");

    @Nonnull
    private final Type resultType;

    /**
     * Creates a placeholder for a field that is not provided.
     *
     * @param resultType the declared type of the field, as is
     */
    public NotProvidedValue(@Nonnull final Type resultType) {
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        throw new RecordCoreException("a field that is not provided cannot be evaluated");
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        // only ever equal to itself, which Value#semanticEquals settles by identity before getting here
        return ConstrainedBoolean.falseValue();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, resultType);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addToString("■"));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return this == other;
    }

    @Nonnull
    @Override
    public PNotProvidedValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PNotProvidedValue.newBuilder()
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setNotProvidedValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static NotProvidedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PNotProvidedValue notProvidedValueProto) {
        return new NotProvidedValue(Type.fromTypeProto(serializationContext,
                Objects.requireNonNull(notProvidedValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PNotProvidedValue, NotProvidedValue> {
        @Nonnull
        @Override
        public Class<PNotProvidedValue> getProtoMessageClass() {
            return PNotProvidedValue.class;
        }

        @Nonnull
        @Override
        public NotProvidedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PNotProvidedValue notProvidedValueProto) {
            return NotProvidedValue.fromProto(serializationContext, notProvidedValueProto);
        }
    }
}
