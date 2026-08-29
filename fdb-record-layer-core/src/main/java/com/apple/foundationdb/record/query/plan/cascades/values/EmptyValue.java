/*
 * EmptyValue.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.planprotos.PEmptyValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.function.Supplier;

/**
 * A value that evaluates to empty.
 */
@API(API.Status.EXPERIMENTAL)
public class EmptyValue extends AbstractValue implements LeafValue {
    private static final EmptyValue EMPTY = new EmptyValue();
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Empty-Value");

    private EmptyValue() {
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return Key.Evaluated.EMPTY;
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return true;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSupliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("empty"));
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

    /**
     * Get an instance representing an empty value.
     *
     * @return an instance of {@link EmptyValue}
     */
    public static EmptyValue empty() {
        return EMPTY;
    }

    @Override
    public PEmptyValue toProto(final PlanSerializationContext serializationContext) {
        return PEmptyValue.newBuilder().build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setEmptyValue(toProto(serializationContext)).build();
    }

    @SuppressWarnings("unused")
    public static EmptyValue fromProto(final PlanSerializationContext serializationContext,
                                       final PEmptyValue emptyValueProto) {
        return new EmptyValue();
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PEmptyValue, EmptyValue> {
        @Override
        public Class<PEmptyValue> getProtoMessageClass() {
            return PEmptyValue.class;
        }

        @Override
        public EmptyValue fromProto(final PlanSerializationContext serializationContext,
                                    final PEmptyValue emptyValueProto) {
            return EmptyValue.fromProto(serializationContext, emptyValueProto);
        }
    }
}
