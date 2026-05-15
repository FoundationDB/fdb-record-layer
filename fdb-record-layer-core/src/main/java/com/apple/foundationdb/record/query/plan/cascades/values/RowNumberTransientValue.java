/*
 * RowNumberValue.java
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

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRowNumberTransientValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RowNumberTransientValue extends TransientWindowValue implements Value.IndexOnlyValue {

    @Nonnull
    private static final String NAME = "ROW_NUMBER_WINDOW";

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    @Nullable
    private final Integer efSearch;

    @Nullable
    private final Boolean isReturningVectors;

    public RowNumberTransientValue(@Nonnull final PlanSerializationContext serializationContext,
                                @Nonnull final PRowNumberTransientValue rowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(rowNumberValueProto.getSuper()));
        this.efSearch = rowNumberValueProto.hasEfSearch() ? rowNumberValueProto.getEfSearch() : null;
        this.isReturningVectors = rowNumberValueProto.hasIsReturningVectors() ? rowNumberValueProto.getIsReturningVectors() : null;
    }

    public RowNumberTransientValue(@Nonnull final Iterable<? extends Value> partitioningValues,
                                @Nonnull final Iterable<WindowOrderingPart> orderingParts,
                                @Nonnull final WindowFrameSpecification windowFrameSpecification,
                                @Nullable final Integer efSearch,
                                @Nullable final Boolean isReturningVectors) {
        super(ImmutableList.of(), partitioningValues, orderingParts, windowFrameSpecification);
        this.efSearch = efSearch;
        this.isReturningVectors = isReturningVectors;
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public RowNumberTransientValue withOrderingParts(final @Nonnull List<WindowOrderingPart> newOrderingParts) {
        return new RowNumberTransientValue(getPartitioningValues(), newOrderingParts, getWindowFrameSpecification(),
                efSearch, isReturningVectors);
    }

    @Nonnull
    @Override
    public WindowValue toWindowValue() {
        return new RowNumberValue(getWindowFrameSpecification());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return basePlanHash(mode, BASE_HASH, efSearch, isReturningVectors);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        Verify.verify(childrenPair.getValue().isEmpty());
        return new RowNumberTransientValue(childrenPair.getKey(), splitNewOrderingParts(newChildren), getWindowFrameSpecification(), efSearch,
                isReturningVectors);
    }

    /**
     * Attempts to adjust a comparison predicate and transform it into a specialized distance rank comparison
     * that can leverage vector similarity indexes for more efficient query execution.
     * <p>
     * This method performs a pattern-matching transformation on queries involving {@code ROW_NUMBER()} window
     * functions ordered by distance metrics (Euclidean, Euclidean square, Cosine, or Dot product distance). When
     * the pattern matches, it converts the comparison into a {@link Comparisons.DistanceRankValueComparison} wrapped
     * in the appropriate distance-specific {@code RowNumberValue} ({@link EuclideanDistanceRowNumberValue},
     * {@link EuclideanSquareDistanceRowNumberValue}, {@link CosineDistanceRowNumberValue}, or
     * {@link DotProductDistanceRowNumberValue}), which enables the query planner to use specialized vector
     * similarity search indexes (such as HNSW indexes).
     * </p>
     * <p>
     * <strong>Matched Pattern:</strong>
     * <pre>
     *                      Comparison (&lt;=, &lt;, or =)
     *                      /                      \
     *                     /                        \
     *                    /                          \
     *   RowNumber(Partitions,                     Constant
     *     [Distance(Field, Vector)])
     * </pre>
     * Where:
     * <ul>
     *   <li><strong>Distance</strong> = {@link ArithmeticValue} with {@code EUCLIDEAN_DISTANCE}, {@code EUCLIDEAN_SQUARE_DISTANCE}, {@code COSINE_DISTANCE}, or {@code DOT_PRODUCT_DISTANCE} operator</li>
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
     * ) &lt;= 10
     *
     * -- Cosine distance example:
     * ROW_NUMBER() OVER (
     *   ORDER BY COSINE_DISTANCE(text_embedding, [0.5, 0.3, 0.2])
     * ) &lt; 5
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
     *   <li>{@link com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator#EUCLIDEAN_DISTANCE}
     *   <li>{@link com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator#EUCLIDEAN_SQUARE_DISTANCE}</li>
     *   <li>{@link com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator#COSINE_DISTANCE}</li>
     *   <li>{@link com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator#DOT_PRODUCT_DISTANCE}</li>
     * </ul>
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
    public Optional<QueryPredicate> transformComparisonMaybe(@Nonnull final Comparisons.Type comparisonType, @Nonnull final Value comparand) {
        Verify.verify(getArgumentValues().isEmpty());
        if (getOrderingParts().size() > 1) {
            // window definition is too complicated for adjustment, bailout.
            return Optional.empty();
        }

        Verify.verify(!getOrderingParts().isEmpty());
        final var orderingPart = getOrderingParts().get(0);

        if (!(orderingPart.getSortOrder().isAnyAscending())) {
            return Optional.empty();
        }

        if (!(orderingPart.getValue() instanceof final DistanceValue distanceValue)) {
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

        Comparisons.DistanceRankValueComparison distanceRankComparison;
        final var distanceValueArgs = ImmutableList.copyOf(distanceValue.getChildren());
        final var indexVector = distanceValueArgs.get(0);
        final var queryVector = distanceValueArgs.get(1);
        final var operator = distanceValue.getOperator();
        distanceRankComparison = new Comparisons.DistanceRankValueComparison(distanceRankComparisonType, queryVector,
                comparand, efSearch, isReturningVectors);
        final TransientWindowValue windowValue;
        switch (operator) {
            case EUCLIDEAN_DISTANCE:
                windowValue = new EuclideanDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case EUCLIDEAN_SQUARE_DISTANCE:
                windowValue = new EuclideanSquareDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case COSINE_DISTANCE:
                windowValue = new CosineDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case DOT_PRODUCT_DISTANCE:
                windowValue = new DotProductDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            default:
                throw new RecordCoreException("unexpected distance function " + operator.name());
        }
        return Optional.of(new ValuePredicate(windowValue, distanceRankComparison));
    }

    @Nonnull
    @Override
    public PRowNumberTransientValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var rowNumberValueProtoBuilder = PRowNumberTransientValue.newBuilder()
                .setSuper(toWindowedValueProto(serializationContext));
        if (efSearch != null) {
            rowNumberValueProtoBuilder.setEfSearch(efSearch);
        }
        if (isReturningVectors != null) {
            rowNumberValueProtoBuilder.setIsReturningVectors(isReturningVectors);
        }
        return rowNumberValueProtoBuilder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRowNumberValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RowNumberTransientValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PRowNumberTransientValue rowNumberValueProto) {
        return new RowNumberTransientValue(serializationContext, rowNumberValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRowNumberTransientValue, RowNumberTransientValue> {
        @Nonnull
        @Override
        public Class<PRowNumberTransientValue> getProtoMessageClass() {
            return PRowNumberTransientValue.class;
        }

        @Nonnull
        @Override
        public RowNumberTransientValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PRowNumberTransientValue rowNumberValueProto) {
            return RowNumberTransientValue.fromProto(serializationContext, rowNumberValueProto);
        }
    }

    /**
     * The {@code row_number} window function.
     */
    @AutoService(BuiltInFunction.class)
    public static final class RowNumberValueFn extends BuiltInFunction<RowNumberTransientValue> {

        @Nonnull
        public static final String EF_SEARCH_ARGUMENT = VectorIndexScanOptions.HNSW_EF_SEARCH.getOptionName();

        @Nonnull
        public static final String INDEX_RETURNS_VECTORS_ARGUMENT = VectorIndexScanOptions.HNSW_RETURN_VECTORS.getOptionName();

        public RowNumberValueFn() {
            super("row_number", ImmutableList.of(Type.any()), Type.any(), (ignored, callSiteArguments) -> {
                final var namedArguments = callSiteArguments.getOptions();
                SemanticException.check(namedArguments.size() <= 2,
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                // Validate that namedArguments only contains EF_SEARCH_ARGUMENT or INDEX_RETURNS_VECTORS_ARGUMENT (or is empty)
                for (final String key : namedArguments.keySet()) {
                    SemanticException.check(EF_SEARCH_ARGUMENT.equals(key) || INDEX_RETURNS_VECTORS_ARGUMENT.equals(key),
                            SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                }

                @Nullable  final Integer efSearch = (Integer)namedArguments.getOrDefault(EF_SEARCH_ARGUMENT, null);
                @Nullable final Boolean indexReturnsVectorsValue = (Boolean)namedArguments.getOrDefault(INDEX_RETURNS_VECTORS_ARGUMENT, null);

                final var windowSpecification = callSiteArguments.getWindowSpecification();
                return new RowNumberTransientValue(windowSpecification.partitioningValues(),
                        windowSpecification.orderingParts(), windowSpecification.frameSpecification(), efSearch, indexReturnsVectorsValue);
            });
        }
    }
}
