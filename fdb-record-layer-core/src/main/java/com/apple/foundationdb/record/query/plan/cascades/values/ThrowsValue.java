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
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that throws an exception if it gets executed.
 */
@API(API.Status.EXPERIMENTAL)
public class ThrowsValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Throws-Value");
    private final Type resultType;

    public ThrowsValue(final Type resultType) {
        this.resultType = resultType;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        throw new RecordCoreException("evaluation of throws()");
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> resultType.equals(((ThrowsValue)other).resultType));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("throws"));
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

    @Override
    public PThrowsValue toProto(final PlanSerializationContext serializationContext) {
        return PThrowsValue.newBuilder()
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setThrowsValue(toProto(serializationContext)).build();
    }

    public static ThrowsValue fromProto(final PlanSerializationContext serializationContext, final PThrowsValue throwsValueProto) {
        return new ThrowsValue(Type.fromTypeProto(serializationContext, Objects.requireNonNull(throwsValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PThrowsValue, ThrowsValue> {
        @Override
        public Class<PThrowsValue> getProtoMessageClass() {
            return PThrowsValue.class;
        }

        @Override
        public ThrowsValue fromProto(final PlanSerializationContext serializationContext,
                                     final PThrowsValue throwsValueProto) {
            return ThrowsValue.fromProto(serializationContext, throwsValueProto);
        }
    }
}
