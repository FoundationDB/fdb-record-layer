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
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return Key.Evaluated.EMPTY;
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return this;
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return true;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "empty()";
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
    @Nonnull
    public static EmptyValue empty() {
        return EMPTY;
    }

    @Nonnull
    @Override
    public PEmptyValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PEmptyValue.newBuilder().build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setEmptyValue(toProto(serializationContext)).build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static EmptyValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PEmptyValue emptyValueProto) {
        return new EmptyValue();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PEmptyValue, EmptyValue> {
        @Nonnull
        @Override
        public Class<PEmptyValue> getProtoMessageClass() {
            return PEmptyValue.class;
        }

        @Nonnull
        @Override
        public EmptyValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PEmptyValue emptyValueProto) {
            return EmptyValue.fromProto(serializationContext, emptyValueProto);
        }
    }
}
