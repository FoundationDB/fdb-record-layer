/*
 * OfTypeValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.POfTypeValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Checks whether a {@link Value}'s evaluation conforms to its result type.
 */
public class OfTypeValue extends AbstractValue implements Value.RangeMatchableValue, ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Of-Type-Value");

    @Nonnull
    private final Value child;
    @Nonnull
    private final Type expectedType;

    private OfTypeValue(@Nonnull final Value child, @Nonnull final Type expectedType) {
        this.child = child;
        this.expectedType = expectedType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, expectedType, child);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    public Type getExpectedType() {
        return expectedType;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new OfTypeValue(rebasedChild, expectedType);
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings(value = {"NP_NONNULL_PARAM_VIOLATION"}, justification = "compile-time evaluations take their value from the context only")
    public Object compileTimeEval(@Nonnull final EvaluationContext context) {
        return eval(null, context);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final var value = child.eval(store, context);
        if (value == null) {
            return expectedType.isNullable();
        }
        if (value instanceof DynamicMessage) {
            return expectedType.isRecord();
        }
        final var type = Type.fromObject(value);
        final var promotionNeeded = PromoteValue.isPromotionNeeded(type, expectedType);
        if (!promotionNeeded) {
            return true;
        }
        return PromoteValue.resolvePhysicalOperator(type, expectedType) != null;
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> expectedType.equals(((OfTypeValue)other).getExpectedType()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, expectedType);
    }

    @Override
    public String toString() {
        return child + " ofType " + expectedType;
    }

    @Nonnull
    @Override
    public POfTypeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return POfTypeValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setExpectedType(expectedType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setOfTypeValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static OfTypeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final POfTypeValue ofTypeValueProto) {
        return new OfTypeValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(ofTypeValueProto.getChild())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(ofTypeValueProto.getExpectedType())));
    }

    @Nonnull
    public static OfTypeValue of(@Nonnull final Value value, @Nonnull final Type type) {
        return new OfTypeValue(value, type);
    }

    /**
     * Derives a {@link OfTypeValue} object from a given {@link ConstantObjectValue}. It does this by constructing a new
     * {@link ConstantObjectValue} as a child requiring it to have a type that conforms to the type of the passed
     * {@link ConstantObjectValue}.
     * @param value The {@link ConstantObjectValue} object we want to derive from.
     * @return new {@link OfTypeValue} that checks whether the underlying child have a type conforming to the type of
     *         the {@link ConstantObjectValue}.
     */
    @Nonnull
    public static OfTypeValue from(@Nonnull final ConstantObjectValue value) {
        return new OfTypeValue(ConstantObjectValue.of(value.getAlias(), value.getConstantId(), Type.any()), value.getResultType());
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<POfTypeValue, OfTypeValue> {
        @Nonnull
        @Override
        public Class<POfTypeValue> getProtoMessageClass() {
            return POfTypeValue.class;
        }

        @Nonnull
        @Override
        public OfTypeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final POfTypeValue ofTypeValueProto) {
            return OfTypeValue.fromProto(serializationContext, ofTypeValueProto);
        }
    }
}
