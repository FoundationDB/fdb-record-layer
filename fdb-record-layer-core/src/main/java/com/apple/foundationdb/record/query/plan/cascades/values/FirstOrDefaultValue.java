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
import com.apple.foundationdb.record.planprotos.PFirstOrDefaultValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that returns the first element of the input array passed in. If there is no such element, that is the array
 * is empty, a default value of the same type is returned.
 */
@API(API.Status.EXPERIMENTAL)
public class FirstOrDefaultValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("First-Or-Default-Value");

    @Nonnull
    private final Value childValue;
    @Nonnull
    private final Value onEmptyResultValue;
    @Nonnull
    private final Supplier<List<Value>> childrenSupplier;
    @Nonnull
    private final Type resultType;

    public FirstOrDefaultValue(@Nonnull final Value childValue, @Nonnull final Value onEmptyResultValue) {
        this.childValue = childValue;
        this.onEmptyResultValue = onEmptyResultValue;
        this.childrenSupplier = () -> ImmutableList.of(childValue, onEmptyResultValue);
        final var childResultType = childValue.getResultType();
        Verify.verify(childResultType.isArray());
        final var elementType = Objects.requireNonNull(((Type.Array)childResultType).getElementType());
        Verify.verify(elementType.equals(onEmptyResultValue.getResultType()));
        this.resultType = Objects.requireNonNull(elementType);
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
        return new FirstOrDefaultValue(newChildrenList.get(0), newChildrenList.get(1));
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
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var childResult = childValue.eval(store, context);
        if (childResult == null) {
            return null;
        }

        final var childrenObjects = (List<?>)childResult;
        if (childrenObjects.isEmpty()) {
            return onEmptyResultValue.eval(store, context);
        }
        return childrenObjects.get(0);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, childValue, onEmptyResultValue);
    }

    @Override
    public String toString() {
        return "firstOrDefault(" + childValue + ", " + onEmptyResultValue + ")";
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "firstOrDefault(" + childValue.explain(formatter) + ", " +
                onEmptyResultValue.explain(formatter) + ")";
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
    public static FirstOrDefaultValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PFirstOrDefaultValue firstOrDefaultValueProto) {
        return new FirstOrDefaultValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(firstOrDefaultValueProto.getChildValue())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(firstOrDefaultValueProto.getOnEmptyResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PFirstOrDefaultValue, FirstOrDefaultValue> {
        @Nonnull
        @Override
        public Class<PFirstOrDefaultValue> getProtoMessageClass() {
            return PFirstOrDefaultValue.class;
        }

        @Nonnull
        @Override
        public FirstOrDefaultValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PFirstOrDefaultValue firstOrDefaultValueProto) {
            return FirstOrDefaultValue.fromProto(serializationContext, firstOrDefaultValueProto);
        }
    }
}
