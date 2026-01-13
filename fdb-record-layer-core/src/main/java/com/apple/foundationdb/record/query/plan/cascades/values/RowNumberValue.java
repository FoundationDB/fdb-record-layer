/*
 * RowNumberValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.RuntimeOption;
import com.apple.foundationdb.record.provider.foundationdb.RuntimeOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue.LogicalOperator.EUCLIDEAN_DISTANCE;

public class RowNumberValue extends WindowedValue implements Value.IndexOnlyValue {

    @Nonnull
    private static final String NAME = "ROW_NUMBER";

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    @Nonnull
    private final RuntimeOptions runtimeOptions;

    public RowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final PRowNumberValue rowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(rowNumberValueProto.getSuper()));
        this.runtimeOptions = RuntimeOptions.fromProto(serializationContext,
                rowNumberValueProto.getRuntimeOptionsList());
    }

    public RowNumberValue(@Nonnull Iterable<? extends Value> partitioningValues,
                          @Nonnull Iterable<? extends Value> argumentValues,
                          @Nonnull RuntimeOptions runtimeOptions) {
        super(partitioningValues, argumentValues);
        this.runtimeOptions = runtimeOptions;
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return basePlanHash(mode, BASE_HASH, runtimeOptions);
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RowNumberValue(childrenPair.getKey(), childrenPair.getValue(), runtimeOptions);
    }

    /**
     * Attempts to adjust a comparison predicate and transform it into a specialized distance rank comparison
     * that can leverage vector similarity indexes for more efficient query execution.
     * <p>
     * This method performs a pattern-matching transformation on queries involving {@code ROW_NUMBER()} window
     * functions ordered by distance metrics (Euclidean or Cosine distance). When the pattern matches, it converts
     * the comparison into a {@link Comparisons.DistanceRankValueComparison} wrapped in either an
     * {@link EuclideanDistanceRowNumberValue} or {@link CosineDistanceRowNumberValue}, which enables the query
     * planner to use specialized vector similarity search indexes (such as HNSW indexes).
     * </p>
     * <p>
     * <strong>Matched Pattern:</strong>
     * <pre>
     *                      Comparison (<=, <, or =)
     *                      /                      \
     *                     /                        \
     *                    /                          \
     *   RowNumber(Partitions,                     Constant
     *     [Distance(Field, Vector)])
     * </pre>
     * Where:
     * <ul>
     *   <li><strong>Distance</strong> = {@link ArithmeticValue} with {@code EUCLIDEAN_DISTANCE} or {@code COSINE_DISTANCE} operator</li>
     *   <li><strong>Field</strong> = {@link FieldValue} representing the indexed vector field to search</li>
     *   <li><strong>Vector</strong> = Constant value ({@link ConstantObjectValue} or {@link LiteralValue}) representing the query vector</li>
     *   <li><strong>Constant</strong> = Constant value representing the k in "top-k" nearest neighbor search</li>
     * </ul>
     * </p>
     * <p>
     * The method accepts distance arguments in either order: {@code Distance(Field, Vector)} or {@code Distance(Vector, Field)}.
     * </p>
     * <p>
     * <strong>Example Queries:</strong><br>
     * <pre>
     * -- Euclidean distance example:
     * ROW_NUMBER() OVER (
     *   PARTITION BY category
     *   ORDER BY EUCLIDEAN_DISTANCE(embedding, [0.1, 0.2, 0.3])
     * ) <= 10
     *
     * -- Cosine distance example:
     * ROW_NUMBER() OVER (
     *   ORDER BY COSINE_DISTANCE(text_embedding, [0.5, 0.3, 0.2])
     * ) < 5
     * </pre>
     * These would be transformed to find the k nearest neighbors using appropriate distance metrics.
     * </p>
     * <p>
     * <strong>Transformation Output:</strong><br>
     * The method transforms the comparison into:
     * <pre>
     * ValuePredicate(
     *   DistanceRowNumberValue(Partitions, IndexField),
     *   DistanceRankValueComparison(mappedComparisonType, QueryVector, k, runtimeOptions)
     * )
     * </pre>
     * Where {@code DistanceRowNumberValue} is either {@link EuclideanDistanceRowNumberValue} or
     * {@link CosineDistanceRowNumberValue} depending on the distance metric used.
     * </p>
     * <p>
     * <strong>Supported Comparison Types:</strong>
     * <ul>
     *   <li>{@code EQUALS} → {@code DISTANCE_RANK_EQUALS}</li>
     *   <li>{@code LESS_THAN} → {@code DISTANCE_RANK_LESS_THAN}</li>
     *   <li>{@code LESS_THAN_OR_EQUALS} → {@code DISTANCE_RANK_LESS_THAN_OR_EQUAL}</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Supported Distance Metrics:</strong>
     * <ul>
     *   <li>{@link ArithmeticValue.LogicalOperator#EUCLIDEAN_DISTANCE}</li>
     *   <li>{@link ArithmeticValue.LogicalOperator#COSINE_DISTANCE}</li>
     * </ul>
     * Future support may include {@code EUCLIDEAN_SQUARE_DISTANCE}, {@code MANHATTAN_DISTANCE}, and {@code DOT_PRODUCT_DISTANCE}.
     * </p>
     * <p>
     * <strong>Requirements for Transformation:</strong>
     * <ul>
     *   <li>Window function must have exactly one argument value</li>
     *   <li>Argument must be an {@link ArithmeticValue} with a supported distance operator</li>
     *   <li>Distance function must have one {@link FieldValue} and one constant value argument</li>
     *   <li>Comparison must be against a constant value</li>
     *   <li>Comparison type must be one of: {@code =}, {@code <}, or {@code <=}</li>
     * </ul>
     * </p>
     *
     * @param comparisonType the type of comparison being performed (must be {@code =}, {@code <}, or {@code <=})
     * @param comparand the value being compared against (must be a constant representing the k in top-k search)
     * @return an {@link Optional} containing the transformed {@link QueryPredicate} if the pattern matches and
     *         transformation is applicable, or {@link Optional#empty()} if the transformation cannot be applied
     */
    @Nonnull
    @Override
    public Optional<QueryPredicate> adjustComparison(@Nonnull final Comparisons.Type comparisonType, @Nonnull final Value comparand) {
        if (getArgumentValues().size() > 1) {
            // window definition is too complicated for adjustment, bailout.
            return Optional.empty();
        }

        Verify.verify(!getArgumentValues().isEmpty());
        final var argument = getArgumentValues().get(0);
        if (!(argument instanceof ArithmeticValue)) {
            return Optional.empty();
        }

        final Comparisons.Type distanceRankComparisonType;
        switch (comparisonType) {
            case EQUALS:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_EQUALS;
                break;
            case LESS_THAN:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_LESS_THAN;
                break;
            case LESS_THAN_OR_EQUALS:
                distanceRankComparisonType = Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL;
                break;
            default:
                return Optional.empty();
        }

        WindowedValue windowedValue;
        Comparisons.DistanceRankValueComparison distanceRankComparison;
        final var arithmeticValue = (ArithmeticValue)argument;
        switch (arithmeticValue.getLogicalOperator()) {
            case EUCLIDEAN_DISTANCE:
            // TODO enable once supported by the index:
            // case EUCLIDEAN_SQUARE_DISTANCE:
            // case MANHATTAN_DISTANCE:
            // case DOT_PRODUCT_DISTANCE:
            case COSINE_DISTANCE:
                Value queryVector;
                Value indexVector;
                final var arithmeticValueArgs = ImmutableList.copyOf(arithmeticValue.getChildren());
                final var firstArg = arithmeticValueArgs.get(0);
                final var secondArg = arithmeticValueArgs.get(1);
                if (firstArg instanceof FieldValue && secondArg.isConstant()) {
                    indexVector = firstArg;
                    queryVector = secondArg;
                } else if (firstArg.isConstant() && secondArg instanceof FieldValue) {
                    indexVector = secondArg;
                    queryVector = firstArg;
                } else {
                    return Optional.empty();
                }
                distanceRankComparison = new Comparisons.DistanceRankValueComparison(distanceRankComparisonType, queryVector,
                        comparand, runtimeOptions);
                if (arithmeticValue.getLogicalOperator() == EUCLIDEAN_DISTANCE) {
                    windowedValue = new EuclideanDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                } else {
                    windowedValue = new CosineDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                }
                break;
            default:
                return Optional.empty();
        }
        return Optional.of(new ValuePredicate(windowedValue, distanceRankComparison));
    }

    @Nonnull
    @Override
    public PRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRowNumberValue.newBuilder()
                .setSuper(toWindowedValueProto(serializationContext))
                .addAllRuntimeOptions(runtimeOptions.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRowNumberValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PRowNumberValue rowNumberValueProto) {
        return new RowNumberValue(serializationContext, rowNumberValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRowNumberValue, RowNumberValue> {
        @Nonnull
        @Override
        public Class<PRowNumberValue> getProtoMessageClass() {
            return PRowNumberValue.class;
        }

        @Nonnull
        @Override
        public RowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PRowNumberValue rowNumberValueProto) {
            return RowNumberValue.fromProto(serializationContext, rowNumberValueProto);
        }
    }

    /**
     * The {@code row_number} window function.
     */
    @AutoService(BuiltInFunction.class)
    public static class RowNumberFn extends BuiltInFunction<Value> {

        public RowNumberFn() {
            super("row_number", ImmutableList.of(Type.any(), Type.any()), RowNumberFn::encapsulateInternal);
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static RowNumberValue encapsulateInternal(@Nonnull BuiltInFunction<Value> builtInFunction,
                                                          @Nonnull final List<? extends Typed> arguments) {
            SemanticException.check(arguments.size() >= 2,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            final var partitioningValuesList = (AbstractArrayConstructorValue)arguments.get(0);
            final var argumentValuesList = (AbstractArrayConstructorValue)arguments.get(1);
            final var runtimeOptionsBuilder = ImmutableSet.<RuntimeOption>builder();
            if (arguments.size() > 2) {
                final var runtimeOptionNames = new HashSet<String>();
                for (int i = 2; i < arguments.size(); i++) {
                    final var argument = arguments.get(i);
                    // TODO: use better error messages
                    SemanticException.check(argument instanceof RuntimeOption,
                            SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                    final var runtimeOption = (RuntimeOption)argument;
                    final var optionName = runtimeOption.getName();
                    SemanticException.check(!(runtimeOptionNames.contains(optionName)),
                            SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                    runtimeOptionNames.add(optionName);
                    runtimeOptionsBuilder.add(runtimeOption);
                }
            }
            final var runtimeOptions = runtimeOptionsBuilder.build();
            return new RowNumberValue(partitioningValuesList.getChildren(), argumentValuesList.getChildren(),
                    new RuntimeOptions(runtimeOptions));
        }
    }
}
