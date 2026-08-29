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
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Checks whether a {@link Value}'s evaluation conforms to its result type.
 */
public class OfTypeValue extends AbstractValue implements Value.RangeMatchableValue, ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Of-Type-Value");

    private final Value child;
    private final Type expectedType;

    private OfTypeValue(final Value child, final Type expectedType) {
        this.child = child;
        this.expectedType = expectedType;
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, expectedType, child);
    }

    @Override
    public Value getChild() {
        return child;
    }

    public Type getExpectedType() {
        return expectedType;
    }

    @Override
    public ValueWithChild withNewChild(final Value rebasedChild) {
        return new OfTypeValue(rebasedChild, expectedType);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nullable final FDBRecordStoreBase<M> store,
                                            final EvaluationContext context) {
        final var value = child.eval(store, context);
        if (value == null) {
            return expectedType.isNullable();
        }
        if (value instanceof DynamicMessage) {
            return expectedType.isRecord();
        }
        final var type = Type.fromObject(value);

        if (type.isPrimitive() && expectedType.isPrimitive() && type.getTypeCode() != Type.TypeCode.NULL) {
            return type.nullable().equals(expectedType.nullable());
        }

        final var promotionNeeded = PromoteValue.isPromotionNeeded(type, expectedType);
        if (!promotionNeeded) {
            return true;
        }

        return PromoteValue.resolvePhysicalOperator(type, expectedType) != null;
    }

    @Nullable
    @Override
    public Boolean evalWithoutStore(final EvaluationContext context) {
        return eval(null, context);
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
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
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var child = Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens();
        return ExplainTokensWithPrecedence.of(ExplainTokensWithPrecedence.Precedence.ALWAYS_PARENS,
                child.addWhitespace().addIdentifier("OF").addWhitespace().addKeyword("TYPE")
                        .addWhitespace().addNested(expectedType.describe()));
    }

    @Override
    public POfTypeValue toProto(final PlanSerializationContext serializationContext) {
        return POfTypeValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setExpectedType(expectedType.toTypeProto(serializationContext))
                .build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setOfTypeValue(toProto(serializationContext)).build();
    }

    public static OfTypeValue fromProto(final PlanSerializationContext serializationContext,
                                        final POfTypeValue ofTypeValueProto) {
        return new OfTypeValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(ofTypeValueProto.getChild())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(ofTypeValueProto.getExpectedType())));
    }

    public static OfTypeValue of(final Value value, final Type type) {
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
    public static OfTypeValue from(final ConstantObjectValue value) {
        return new OfTypeValue(ConstantObjectValue.of(value.getAlias(), value.getConstantId(), Type.any()), value.getResultType());
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<POfTypeValue, OfTypeValue> {
        @Override
        public Class<POfTypeValue> getProtoMessageClass() {
            return POfTypeValue.class;
        }

        @Override
        public OfTypeValue fromProto(final PlanSerializationContext serializationContext,
                                     final POfTypeValue ofTypeValueProto) {
            return OfTypeValue.fromProto(serializationContext, ofTypeValueProto);
        }
    }
}
