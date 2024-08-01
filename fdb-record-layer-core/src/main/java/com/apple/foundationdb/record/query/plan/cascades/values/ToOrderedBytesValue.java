/*
 * ToOrderedBytesValue.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.planprotos.PToOrderedBytesValue;
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
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A value that produces a binary encoding that is comparable according to certain modes gives by
 * {@link Direction}.
 */
@API(API.Status.EXPERIMENTAL)
public class ToOrderedBytesValue extends AbstractValue implements ValueWithChild {
    /**
     * The hash value of this expression.
     */
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("To-Ordered-Bytes-Value");

    /**
     * The child expression.
     */
    @Nonnull
    private final Value child;

    @Nonnull
    private final Direction direction;

    /**
     * Constructs a new {@link ToOrderedBytesValue} instance.
     * @param child The child expression.
     */
    public ToOrderedBytesValue(@Nonnull final Value child, @Nonnull final Direction direction) {
        this.child = child;
        this.direction = direction;
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
        return Type.primitiveType(Type.TypeCode.BYTES);
    }

    @Nonnull
    public Direction getDirection() {
        return direction;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new ToOrderedBytesValue(rebasedChild, direction);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final Object result = child.eval(store, context);
        return ZeroCopyByteString.wrap(TupleOrdering.pack(Key.Evaluated.scalar(result).toTuple(), direction));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, direction);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, child, direction);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "to_ordered_bytes(" + child.explain(formatter) + ", " + getDirection() + ")";
    }

    @Override
    public String toString() {
        return "to_ordered_bytes(" + child + ", " + getDirection() + ")";
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
                .filter(ignored -> direction == ((ToOrderedBytesValue)other).getDirection());
    }

    @Nonnull
    @Override
    public PToOrderedBytesValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PToOrderedBytesValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setDirection(OrderedBytesHelpers.toDirectionProto(direction))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setToOrderedBytesValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ToOrderedBytesValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PToOrderedBytesValue toOrderedBytesValueProto) {
        return new ToOrderedBytesValue(
                Value.fromValueProto(serializationContext, Objects.requireNonNull(toOrderedBytesValueProto.getChild())),
                OrderedBytesHelpers.fromDirectionProto(Objects.requireNonNull(toOrderedBytesValueProto.getDirection())));
    }

    /**
     * The {@code TO_ORDERED_BYTES_ASC_NULLS_FIRST} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ToOrderedBytesAscNullsFirstFn extends BuiltInFunction<Value> {
        public ToOrderedBytesAscNullsFirstFn() {
            super("TO_ORDERED_BYTES_" + Direction.ASC_NULLS_FIRST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new ToOrderedBytesValue((Value)arguments.get(0), Direction.ASC_NULLS_FIRST));
        }
    }

    /**
     * The {@code TO_ORDERED_BYTES_ASC_NULLS_LAST} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ToOrderedBytesAscNullsLastFn extends BuiltInFunction<Value> {
        public ToOrderedBytesAscNullsLastFn() {
            super("TO_ORDERED_BYTES_" + Direction.ASC_NULLS_LAST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new ToOrderedBytesValue((Value)arguments.get(0), Direction.ASC_NULLS_LAST));
        }
    }

    /**
     * The {@code TO_ORDERED_BYTES_DESC_NULLS_FIRST} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ToOrderedBytesDescNullsFirstFn extends BuiltInFunction<Value> {
        public ToOrderedBytesDescNullsFirstFn() {
            super("TO_ORDERED_BYTES_" + Direction.DESC_NULLS_FIRST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new ToOrderedBytesValue((Value)arguments.get(0), Direction.DESC_NULLS_FIRST));
        }
    }

    /**
     * The {@code TO_ORDERED_BYTES_DESC_NULLS_LAST} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ToOrderedBytesDescNullsLastFn extends BuiltInFunction<Value> {
        public ToOrderedBytesDescNullsLastFn() {
            super("TO_ORDERED_BYTES_" + Direction.DESC_NULLS_LAST,
                    ImmutableList.of(Type.any()),
                    (builtInFunction, arguments) -> new ToOrderedBytesValue((Value)arguments.get(0), Direction.DESC_NULLS_LAST));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PToOrderedBytesValue, ToOrderedBytesValue> {
        @Nonnull
        @Override
        public Class<PToOrderedBytesValue> getProtoMessageClass() {
            return PToOrderedBytesValue.class;
        }

        @Nonnull
        @Override
        public ToOrderedBytesValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PToOrderedBytesValue toOrderedBytesValueProto) {
            return ToOrderedBytesValue.fromProto(serializationContext, toOrderedBytesValueProto);
        }
    }
}
