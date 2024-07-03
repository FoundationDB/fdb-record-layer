/*
 * ThrowsValue.java
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
import com.apple.foundationdb.record.planprotos.PThrowsValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A value that throws an exception if it gets executed.
 */
@API(API.Status.EXPERIMENTAL)
public class ThrowsValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Throws-Value");
    @Nonnull
    private final Type resultType;

    public ThrowsValue(@Nonnull final Type resultType) {
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

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new RecordCoreException("evaluation of throws()");
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> resultType.equals(((ThrowsValue)other).resultType));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public String toString() {
        return "throws()";
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "throws()";
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
    public PThrowsValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PThrowsValue.newBuilder()
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setThrowsValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ThrowsValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PThrowsValue throwsValueProto) {
        return new ThrowsValue(Type.fromTypeProto(serializationContext, Objects.requireNonNull(throwsValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PThrowsValue, ThrowsValue> {
        @Nonnull
        @Override
        public Class<PThrowsValue> getProtoMessageClass() {
            return PThrowsValue.class;
        }

        @Nonnull
        @Override
        public ThrowsValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PThrowsValue throwsValueProto) {
            return ThrowsValue.fromProto(serializationContext, throwsValueProto);
        }
    }
}
