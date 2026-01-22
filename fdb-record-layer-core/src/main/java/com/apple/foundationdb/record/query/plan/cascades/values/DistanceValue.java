/*
 * DistanceValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PDistanceValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A {@link Value} that applies a distance metric operation on vector expressions.
 * <p>
 * This class handles vector distance calculations such as Euclidean distance, cosine distance,
 * Manhattan distance, and dot product distance. All operations expect to work with vector types
 * ({@link TypeCode#VECTOR}).
 * <p>
 * Distance operations are commonly used in similarity search and nearest neighbor queries,
 * particularly with vector similarity indexes (such as HNSW indexes).
 */
@API(API.Status.EXPERIMENTAL)
public class DistanceValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Distance-Value");

    @Nonnull
    private final DistanceOperator operator;
    @Nonnull
    private final Value leftChild;
    @Nonnull
    private final Value rightChild;

    /**
     * Constructs a new instance of {@link DistanceValue}.
     * @param operator The distance operation.
     * @param leftChild The left child (typically a vector field).
     * @param rightChild The right child (typically a query vector).
     */
    public DistanceValue(@Nonnull DistanceOperator operator,
                         @Nonnull Value leftChild,
                         @Nonnull Value rightChild) {
        this.operator = operator;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final RealVector left = (RealVector)leftChild.eval(store, context);
        final RealVector right = (RealVector)rightChild.eval(store, context);
        if (left == null || right == null) {
            throw new RecordCoreException("Vectors cannot be null");
        }
        return operator.eval((RealVector)leftChild.eval(store, context), (RealVector)rightChild.eval(store, context));
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSupplier) {
        final var left = Iterables.get(explainSupplier, 0).get();
        final var right = Iterables.get(explainSupplier, 1).get();
        final var precedence = operator.getPrecedence();
        return ExplainTokensWithPrecedence.of(precedence,
                precedence.parenthesizeChild(left).addWhitespace()
                        .addToString(operator.getInfixNotation()).addWhitespace()
                        .addNested(precedence.parenthesizeChild(right)));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(TypeCode.DOUBLE);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(leftChild, rightChild);
    }

    @Nonnull
    @Override
    public DistanceValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new DistanceValue(this.operator,
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, leftChild, rightChild);
    }

    @Nonnull
    public DistanceOperator getOperator() {
        return operator;
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other).filter(ignored -> {
            DistanceValue otherDistance = (DistanceValue)other;
            return operator.equals(otherDistance.operator);
        });
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
    public PDistanceValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PDistanceValue.newBuilder()
                .setOperator(operator.toProto(serializationContext))
                .setLeftChild(leftChild.toValueProto(serializationContext))
                .setRightChild(rightChild.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setDistanceValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static DistanceValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PDistanceValue distanceValueProto) {
        return new DistanceValue(DistanceOperator.fromProto(serializationContext, Objects.requireNonNull(distanceValueProto.getOperator())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(distanceValueProto.getLeftChild())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(distanceValueProto.getRightChild())));
    }

    @Nonnull
    private static Value encapsulateInternal(@Nonnull BuiltInFunction<Value> builtInFunction,
                                             @Nonnull final List<? extends Typed> arguments) {
        return encapsulate(builtInFunction.getFunctionName(), arguments);
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final String functionName, @Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        final Typed arg0 = arguments.get(0);
        final Type type0 = arg0.getResultType();
        final Typed arg1 = arguments.get(1);
        final Type type1 = arg1.getResultType();

        // Distance functions expect vector types
        SemanticException.check(type0.getTypeCode() == TypeCode.VECTOR || type1.getTypeCode() == TypeCode.VECTOR,
                SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE,
                "Distance functions require at least one vector argument");

        final Optional<DistanceOperator> operatorOptional = Enums.getIfPresent(DistanceOperator.class, functionName.toUpperCase(Locale.ROOT)).toJavaUtil();
        Verify.verify(operatorOptional.isPresent());
        final DistanceOperator logicalOperator = operatorOptional.get();

        return new DistanceValue(logicalOperator, (Value)arg0, (Value)arg1);
    }


    /**
     * Operators for various distance metric functors.
     * All distance operations work on vector types and return a double representing the distance.
     */
    public enum DistanceOperator {
        EUCLIDEAN_DISTANCE("euclidean_distance", Precedence.NEVER_PARENS, ((l, r) -> new Metric.EuclideanMetric().distance(l.getData(), r.getData()))),
        EUCLIDEAN_SQUARE_DISTANCE("euclidean_square_distance", Precedence.NEVER_PARENS, (l, r) -> new Metric.EuclideanSquareMetric().distance(l.getData(), r.getData())),
        MANHATTAN_DISTANCE("manhattan_distance", Precedence.NEVER_PARENS, ((l, r) -> new Metric.ManhattanMetric().distance(l.getData(), r.getData()))),
        COSINE_DISTANCE("cosine_distance", Precedence.NEVER_PARENS, ((l, r) -> new Metric.CosineMetric().distance(l.getData(), r.getData()))),
        DOT_PRODUCT_DISTANCE("dot_product_distance", Precedence.NEVER_PARENS, ((l, r) -> new Metric.DotProductMetric().distance(l.getData(), r.getData())))
        ;

        @Nonnull
        private static final Supplier<BiMap<DistanceOperator, PDistanceValue.PDistanceOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(DistanceOperator.class,
                        PDistanceValue.PDistanceOperator.class));

        @Nonnull
        private final String infixNotation;
        @Nonnull
        private final Precedence precedence;
        @Nonnull
        private final BiFunction<RealVector, RealVector, Double> evaluateFunction;

        DistanceOperator(@Nonnull final String infixNotation, @Nonnull final Precedence precedence,
                         @Nonnull final BiFunction<RealVector, RealVector, Double> evaluateFunction) {
            this.infixNotation = infixNotation;
            this.precedence = precedence;
            this.evaluateFunction = evaluateFunction;
        }

        @Nonnull
        public String getInfixNotation() {
            return infixNotation;
        }

        @Nonnull
        public Precedence getPrecedence() {
            return precedence;
        }

        @Nullable
        public Object eval(@Nullable final RealVector arg1, @Nullable final RealVector arg2) {
            return evaluateFunction.apply(arg1, arg2);
        }

        @Nonnull
        private static BiMap<DistanceOperator, PDistanceValue.PDistanceOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PDistanceValue.PDistanceOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static DistanceOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PDistanceValue.PDistanceOperator physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PDistanceValue, DistanceValue> {
        @Nonnull
        @Override
        public Class<PDistanceValue> getProtoMessageClass() {
            return PDistanceValue.class;
        }

        @Nonnull
        @Override
        public DistanceValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PDistanceValue distanceValueProto) {
            return DistanceValue.fromProto(serializationContext, distanceValueProto);
        }
    }

    /**
     * Euclidean distance function.
     */
    @AutoService(BuiltInFunction.class)
    public static class EuclideanDistanceFn extends BuiltInFunction<Value> {
        public EuclideanDistanceFn() {
            super("euclidean_distance",
                    ImmutableList.of(Type.any(), Type.any()), DistanceValue::encapsulateInternal);
        }
    }

    /**
     * Euclidean square distance function.
     */
    @AutoService(BuiltInFunction.class)
    public static class EuclideanSquareDistanceFn extends BuiltInFunction<Value> {
        public EuclideanSquareDistanceFn() {
            super("euclidean_square_distance",
                    ImmutableList.of(Type.any(), Type.any()), DistanceValue::encapsulateInternal);
        }
    }

    /**
     * Manhattan distance function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ManhattanDistanceFn extends BuiltInFunction<Value> {
        public ManhattanDistanceFn() {
            super("manhattan_distance",
                    ImmutableList.of(Type.any(), Type.any()), DistanceValue::encapsulateInternal);
        }
    }

    /**
     * Cosine distance function.
     */
    @AutoService(BuiltInFunction.class)
    public static class CosineDistanceFn extends BuiltInFunction<Value> {
        public CosineDistanceFn() {
            super("cosine_distance",
                    ImmutableList.of(Type.any(), Type.any()), DistanceValue::encapsulateInternal);
        }
    }

    /**
     * Dot product distance function.
     */
    @AutoService(BuiltInFunction.class)
    public static class DotProductDistanceFn extends BuiltInFunction<Value> {
        public DotProductDistanceFn() {
            super("dot_product_distance",
                    ImmutableList.of(Type.any(), Type.any()), DistanceValue::encapsulateInternal);
        }
    }
}
