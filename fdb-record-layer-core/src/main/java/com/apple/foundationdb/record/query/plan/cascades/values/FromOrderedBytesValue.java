/*
 * FromOrderedBytesValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PFromOrderedBytesValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.apple.foundationdb.tuple.TupleOrdering.Direction;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A value that produces a binary encoding that is comparable according to certain modes gives by
 * {@link Direction}.
 */
@API(API.Status.EXPERIMENTAL)
public class FromOrderedBytesValue extends AbstractValue implements ValueWithChild {
    /**
     * The hash value of this expression.
     */
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("From-Ordered-Bytes-Value");

    /**
     * The child expression.
     */
    @Nonnull
    private final Value child;

    @Nonnull
    private final Direction direction;

    @Nonnull
    private final Type resultType;

    /**
     * Constructs a new {@link FromOrderedBytesValue} instance.
     * @param child The child expression.
     */
    public FromOrderedBytesValue(@Nonnull final Value child,
                                 @Nonnull final Direction direction,
                                 @Nonnull final Type resultType) {
        this.child = child;
        this.direction = direction;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    public Direction getDirection() {
        return direction;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new FromOrderedBytesValue(rebasedChild, direction, resultType);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final var childResult = (ByteString)Objects.requireNonNull(child.eval(store, context));
        final Object result = TupleOrdering.unpack(childResult.toByteArray(), direction).get(0);
        Verify.verify(Type.fromObject(result).nullable().equals(resultType.nullable()));
        return result;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, direction, resultType);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, child, direction);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "from_ordered_bytes(" + child.explain(formatter) + ", " + getDirection() + ", " + getResultType() + ")";
    }

    @Override
    public String toString() {
        return "from_ordered_bytes(" + child + ", " + getDirection() + ", " + getResultType() + ")";
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
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> direction == ((FromOrderedBytesValue)other).getDirection())
                .filter(ignored -> resultType.equals(other.getResultType()));
    }

    @Nonnull
    @Override
    public PFromOrderedBytesValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PFromOrderedBytesValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setDirection(OrderedBytesHelpers.toDirectionProto(direction))
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setFromOrderedBytesValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static FromOrderedBytesValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PFromOrderedBytesValue fromOrderedBytesValueProto) {
        return new FromOrderedBytesValue(
                Value.fromValueProto(serializationContext, Objects.requireNonNull(fromOrderedBytesValueProto.getChild())),
                OrderedBytesHelpers.fromDirectionProto(Objects.requireNonNull(fromOrderedBytesValueProto.getDirection())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(fromOrderedBytesValueProto.getResultType())));
    }

    /**
     * The {@code FROM_ORDERED_BYTES_ASC_NULLS_FIRST} function. Note that this method's result is of type {@code ANY}.
     * The caller should ensure that the result is cast into an appropriate more specific type.
     */
    @AutoService(BuiltInFunction.class)
    public static class FromOrderedBytesAscNullsFirstFn extends BuiltInFunction<Value> {
        public FromOrderedBytesAscNullsFirstFn() {
            super("FROM_ORDERED_BYTES_" + Direction.ASC_NULLS_FIRST,
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.BYTES)),
                    (builtInFunction, arguments) -> new FromOrderedBytesValue((Value)arguments.get(0), Direction.ASC_NULLS_FIRST, Type.any()));
        }
    }

    /**
     * The {@code FROM_ORDERED_BYTES_ASC_NULLS_LAST} function. Note that this method's result is of type {@code ANY}.
     * The caller should ensure that the result is cast into an appropriate more specific type.
     */
    @AutoService(BuiltInFunction.class)
    public static class FromOrderedBytesAscNullsLastFn extends BuiltInFunction<Value> {
        public FromOrderedBytesAscNullsLastFn() {
            super("FROM_ORDERED_BYTES_" + Direction.ASC_NULLS_LAST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new FromOrderedBytesValue((Value)arguments.get(0), Direction.ASC_NULLS_LAST, Type.any()));
        }
    }

    /**
     * The {@code FROM_ORDERED_BYTES_DESC_NULLS_FIRST} function. Note that this method's result is of type {@code ANY}.
     * The caller should ensure that the result is cast into an appropriate more specific type.
     */
    @AutoService(BuiltInFunction.class)
    public static class FromOrderedBytesDescNullsFirstFn extends BuiltInFunction<Value> {
        public FromOrderedBytesDescNullsFirstFn() {
            super("FROM_ORDERED_BYTES_" + Direction.DESC_NULLS_FIRST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new FromOrderedBytesValue((Value)arguments.get(0), Direction.DESC_NULLS_FIRST, Type.any()));
        }
    }

    /**
     * The {@code FROM_ORDERED_BYTES_DESC_NULLS_LAST} function. Note that this method's result is of type {@code ANY}.
     * The caller should ensure that the result is cast into an appropriate more specific type.
     */
    @AutoService(BuiltInFunction.class)
    public static class FromOrderedBytesDescNullsLastFn extends BuiltInFunction<Value> {
        public FromOrderedBytesDescNullsLastFn() {
            super("FROM_ORDERED_BYTES_" + Direction.DESC_NULLS_LAST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new FromOrderedBytesValue((Value)arguments.get(0), Direction.DESC_NULLS_LAST, Type.any()));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PFromOrderedBytesValue, FromOrderedBytesValue> {
        @Nonnull
        @Override
        public Class<PFromOrderedBytesValue> getProtoMessageClass() {
            return PFromOrderedBytesValue.class;
        }

        @Nonnull
        @Override
        public FromOrderedBytesValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PFromOrderedBytesValue fromOrderedBytesValueProto) {
            return FromOrderedBytesValue.fromProto(serializationContext, fromOrderedBytesValueProto);
        }
    }
}
