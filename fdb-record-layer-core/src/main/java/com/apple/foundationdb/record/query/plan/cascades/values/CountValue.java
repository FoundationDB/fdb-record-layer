/*
 * CountValue.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.planprotos.PCountValue;
import com.apple.foundationdb.record.planprotos.PCountValue.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * A counting aggregate value.
 */
@API(API.Status.EXPERIMENTAL)
public class CountValue extends AbstractValue implements AggregateValue, StreamableAggregateValue, IndexableAggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Count-Value");

    @Nonnull
    protected final PhysicalOperator operator;
    @Nullable
    private final Value child;

    @Nonnull
    private final String indexTypeName;

    public CountValue(@Nonnull final Value child) {
        this(isCountStar(child), child);
    }

    public CountValue(boolean isCountStar, @Nonnull final Value child) {
        this(isCountStar ? PhysicalOperator.COUNT_STAR : PhysicalOperator.COUNT, child);
    }

    public CountValue(@Nonnull PhysicalOperator operator, @Nullable Value child) {
        this(operator, child, operator == PhysicalOperator.COUNT ? IndexTypes.COUNT_NOT_NULL : IndexTypes.COUNT);
    }

    public CountValue(@Nonnull PhysicalOperator operator,
                      @Nullable Value child,
                      @Nonnull String indexTypeName) {
        this.operator = operator;
        this.child = child;
        this.indexTypeName = indexTypeName;
    }

    private static boolean isCountStar(@Nonnull Typed valueType) {
        // todo: we should dispatch on the right function depending on whether the child
        // value type is nullable or not. The '*' in count(*) must be guaranteed to be not-null
        // during plan generation.
        return !valueType.getResultType().isPrimitive();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        if (child != null) {
            return operator.evalInitialToPartial(child.eval(store, context));
        } else {
            return operator.evalInitialToPartial(null);
        }
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(final @Nonnull TypeRepository typeRepository) {
        return new SumAccumulator(operator);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        if (child != null) {
            return operator.name().toLowerCase(Locale.ROOT) + "(" + child.explain(formatter) + ")";
        } else {
            return operator.name().toLowerCase(Locale.ROOT) + "()";
        }
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        if (child != null) {
            return ImmutableList.of(child);
        } else {
            return ImmutableList.of();
        }
    }

    @Nonnull
    @Override
    public CountValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new CountValue(this.operator, Iterables.get(newChildren, 0));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, child);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + child + ")";
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
    public String getIndexTypeName() {
        return indexTypeName;
    }

    @Nonnull
    @Override
    public PCountValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder =  PCountValue.newBuilder()
                .setOperator(operator.toProto(serializationContext));
        if (child != null) {
            builder.setChild(child.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCountValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static CountValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PCountValue countValueProto) {
        final Value child;
        if (countValueProto.hasChild()) {
            child = Value.fromValueProto(serializationContext, countValueProto.getChild());
        } else {
            child = null;
        }
        return new CountValue(PhysicalOperator.fromProto(serializationContext,
                Objects.requireNonNull(countValueProto.getOperator())), child);
    }

    /**
     * The {@code count(x)} function.
     */
    @AutoService(BuiltInFunction.class)
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public static class CountFn extends BuiltInFunction<AggregateValue> {
        public CountFn() {
            super("COUNT",
                    ImmutableList.of(new Type.Any()), CountFn::encapsulate);
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<? extends Typed> arguments) {
            final Typed arg0 = arguments.get(0);
            return new CountValue((Value)arg0);
        }
    }

    /**
     * The counting argument type.
     */
    public enum PhysicalOperator {
        COUNT(TypeCode.LONG, v -> v == null ? 0L : 1L, (s, v) -> Math.addExact((long)s, (long)v), UnaryOperator.identity()),
        COUNT_STAR(TypeCode.LONG, v -> 1L, (s, v) -> Math.addExact((long)s, (long)v), UnaryOperator.identity());

        @Nonnull
        private final TypeCode resultType;

        @Nonnull
        private final UnaryOperator<Object> initialToPartialFunction;

        @Nonnull
        private final BinaryOperator<Object> partialToPartialFunction;

        @Nonnull
        private final UnaryOperator<Object> partialToFinalFunction;

        PhysicalOperator(@Nonnull final TypeCode resultType,
                         @Nonnull final UnaryOperator<Object> initialToPartialFunction,
                         @Nonnull final BinaryOperator<Object> partialToPartialFunction,
                         @Nonnull final UnaryOperator<Object> partialToFinalFunction) {
            this.resultType = resultType;
            this.initialToPartialFunction = initialToPartialFunction;
            this.partialToPartialFunction = partialToPartialFunction;
            this.partialToFinalFunction = partialToFinalFunction;
        }

        @Nonnull
        public TypeCode getResultTypeCode() {
            return resultType;
        }

        @Nonnull
        public UnaryOperator<Object> getInitialToPartialFunction() {
            return initialToPartialFunction;
        }

        @Nonnull
        public BinaryOperator<Object> getPartialToPartialFunction() {
            return partialToPartialFunction;
        }

        @Nonnull
        public UnaryOperator<Object> getPartialToFinalFunction() {
            return partialToFinalFunction;
        }

        @Nullable
        public Object evalInitialToPartial(@Nullable Object object) {
            return initialToPartialFunction.apply(object);
        }

        @Nullable
        public Object evalPartialToPartial(@Nullable Object object1, @Nullable Object object2) {
            return partialToPartialFunction.apply(object1 == null ? 0L : object1, object2 == null ? 0L : object2);
        }

        @Nullable
        public Object evalPartialToFinal(@Nullable Object object) {
            if (object == null) {
                return 0L;
            }
            return partialToFinalFunction.apply(object);
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case COUNT:
                    return PPhysicalOperator.COUNT;
                case COUNT_STAR:
                    return PPhysicalOperator.COUNT_STAR;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PPhysicalOperator physicalOperatorProto) {
            switch (physicalOperatorProto) {
                case COUNT:
                    return COUNT;
                case COUNT_STAR:
                    return COUNT_STAR;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }
    }

    /**
     * Sum partial counts in the appropriate mode.
     */
    public static class SumAccumulator implements Accumulator {
        private final PhysicalOperator physicalOperator;
        Object state = null;

        public SumAccumulator(@Nonnull final PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public void accumulate(@Nullable final Object currentObject) {
            this.state = physicalOperator.evalPartialToPartial(state, currentObject);
        }

        @Nullable
        @Override
        public Object finish() {
            return physicalOperator.evalPartialToFinal(state);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCountValue, CountValue> {
        @Nonnull
        @Override
        public Class<PCountValue> getProtoMessageClass() {
            return PCountValue.class;
        }

        @Nonnull
        @Override
        public CountValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PCountValue countValueProto) {
            return CountValue.fromProto(serializationContext, countValueProto);
        }
    }
}
