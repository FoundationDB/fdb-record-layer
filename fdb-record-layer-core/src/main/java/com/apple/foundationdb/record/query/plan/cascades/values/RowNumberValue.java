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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator.COSINE_DISTANCE;
import static com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator.DOT_PRODUCT_DISTANCE;
import static com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator.EUCLIDEAN_DISTANCE;
import static com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue.DistanceOperator.EUCLIDEAN_SQUARE_DISTANCE;

/**
 * A windowed value representing the {@code ROW_NUMBER()} window function, which assigns sequential
 * integer row numbers to rows within partitions based on a specified ordering.
 * <p>
 * This class extends {@link WindowedValue} to provide window function semantics and implements
 * {@link Value.IndexOnlyValue} because row numbers are computed during index traversal for
 * vector similarity searches and cannot be reconstructed from base records alone.
 * </p>
 *
 * <h2>Primary Use Case: Vector Similarity Search</h2>
 * <p>
 * While {@code ROW_NUMBER()} is a standard SQL window function, this implementation is specifically
 * designed for K-nearest neighbor (K-NN) vector similarity searches using HNSW indexes. The function
 * assigns rank positions to vectors based on their distance from a query vector, enabling efficient
 * "top-K" result limiting through the {@code QUALIFY} clause.
 * </p>
 * <p>
 * <strong>Typical Query Pattern:</strong>
 * <pre>
 * SELECT docId, title
 * FROM documents
 * WHERE category = 'tech'
 * QUALIFY ROW_NUMBER() OVER (
 *   PARTITION BY category
 *   ORDER BY euclidean_distance(embedding, [0.1, 0.2, 0.3])
 *   OPTIONS ef_search = 100
 * ) &lt;= 10
 * </pre>
 * This query finds the 10 nearest documents in the 'tech' category to the query vector {@code [0.1, 0.2, 0.3]}.
 * </p>
 *
 * <h2>Configuration Parameters</h2>
 * <ul>
 *   <li><strong>efSearch</strong> ({@code Integer}, optional): Controls the HNSW index search quality.
 *       Higher values increase recall (accuracy) but decrease performance. Corresponds to the
 *       {@code ef_search} parameter in the {@code OPTIONS} clause. When {@code null}, the index's
 *       default value is used.</li>
 *   <li><strong>isReturningVectors</strong> ({@code Boolean}, optional): Determines whether the
 *       index scan should return actual vector values in addition to distances. When {@code true},
 *       vectors are returned; when {@code false} or {@code null}, only distances and document IDs
 *       are returned, reducing data transfer overhead.</li>
 * </ul>
 *
 * <h2>Comparison Transformation</h2>
 * <p>
 * The {@link #transformComparisonMaybe(Comparisons.Type, Value)} method performs critical pattern
 * matching to enable HNSW index usage. When a comparison like {@code ROW_NUMBER() <= 10} is detected,
 * it transforms the predicate into a {@link Comparisons.DistanceRankValueComparison} that the query
 * planner recognizes as a K-NN search pattern.
 * </p>
 * <p>
 * <strong>Transformation Example:</strong>
 * <pre>
 * // Original predicate
 * ROW_NUMBER() OVER (ORDER BY euclidean_distance(embedding, queryVec)) &lt;= 10
 *
 * // Transformed to
 * ValuePredicate(
 *   EuclideanDistanceRowNumberValue(partitions, indexField),
 *   DistanceRankValueComparison(queryVec, k=10, efSearch, isReturningVectors)
 * )
 * </pre>
 * This transformation allows {@link com.apple.foundationdb.record.query.plan.cascades.VectorIndexScanMatchCandidate}
 * to recognize and satisfy the pattern using an HNSW index scan.
 * </p>
 *
 * <h2>Class Hierarchy</h2>
 * <ul>
 *   <li><strong>{@link RowNumberValue}</strong> (this class) - Base window function implementation</li>
 *   <li><strong>{@link EuclideanDistanceRowNumberValue}</strong> - Specialized for Euclidean distance ordering</li>
 *   <li><strong>{@link CosineDistanceRowNumberValue}</strong> - Specialized for Cosine distance ordering</li>
 * </ul>
 * <p>
 * The specialized subclasses are created during comparison transformation to explicitly capture the
 * distance metric being used, enabling the query planner to match against appropriately-configured
 * HNSW indexes.
 * </p>
 *
 * <h2>Integration with Higher-Order Functions</h2>
 * <p>
 * {@code ROW_NUMBER()} is resolved as a higher-order function through {@link RowNumberHighOrderValue}.
 * The resolution process is:
 * <ol>
 *   <li>Parser encounters {@code ROW_NUMBER(ef_search: 100)}</li>
 *   <li>{@link RowNumberHighOrderFn} creates {@link RowNumberHighOrderValue} with configuration</li>
 *   <li>Higher-order value is evaluated to produce a curried function</li>
 *   <li>Curried function is applied with partition and ordering arguments</li>
 *   <li>Final {@code RowNumberValue} is produced with configuration baked in</li>
 * </ol>
 * This multi-stage resolution enables flexible syntax where configuration parameters can be specified
 * separately from the window specification.
 * </p>
 *
 * <h2>Index-Only Semantics</h2>
 * <p>
 * This class implements {@link Value.IndexOnlyValue} because row numbers are computed during the
 * HNSW index traversal based on distance rankings. These rankings cannot be reconstructed from the
 * base record data alone - they fundamentally depend on the index's graph structure and search
 * algorithm. This constraint ensures that:
 * <ul>
 *   <li>The query planner doesn't attempt to compute row numbers outside of index scans</li>
 *   <li>No covering index optimizations at the moment</li>
 *   <li>Plan validation catches attempts to use {@code ROW_NUMBER()} without an appropriate index</li>
 * </ul>
 * </p>
 *
 * @see WindowedValue for the window function base class
 * @see RowNumberHighOrderValue for the higher-order function wrapper
 * @see RowNumberHighOrderFn for the function definition and resolution
 * @see EuclideanDistanceRowNumberValue for Euclidean distance specialization
 * @see CosineDistanceRowNumberValue for Cosine distance specialization
 * @see Comparisons.DistanceRankValueComparison for the transformed comparison type
 * @see com.apple.foundationdb.record.query.plan.cascades.VectorIndexScanMatchCandidate for index matching
 */
public class RowNumberValue extends WindowedValue implements Value.IndexOnlyValue {

    @Nonnull
    private static final String NAME = "ROW_NUMBER";

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    @Nullable
    private final Integer efSearch;

    @Nullable
    private final Boolean isReturningVectors;

    public RowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final PRowNumberValue rowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(rowNumberValueProto.getSuper()));
        this.efSearch = rowNumberValueProto.hasEfSearch() ? rowNumberValueProto.getEfSearch() : null;
        this.isReturningVectors = rowNumberValueProto.hasIsReturningVectors() ? rowNumberValueProto.getIsReturningVectors() : null;
    }

    public RowNumberValue(@Nonnull Iterable<? extends Value> partitioningValues,
                          @Nonnull Iterable<? extends Value> argumentValues,
                          @Nullable final Integer efSearch,
                          @Nullable final Boolean isReturningVectors) {
        super(partitioningValues, argumentValues);
        this.efSearch = efSearch;
        this.isReturningVectors = isReturningVectors;
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
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
        return new RowNumberValue(childrenPair.getKey(), childrenPair.getValue(), efSearch, isReturningVectors);
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
        if (getArgumentValues().size() > 1) {
            // window definition is too complicated for adjustment, bailout.
            return Optional.empty();
        }

        Verify.verify(!getArgumentValues().isEmpty());
        final var argument = getArgumentValues().get(0);
        if (!(argument instanceof DistanceValue)) {
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
        final var distanceValue = (DistanceValue)argument;
        final var distanceValueArgs = ImmutableList.copyOf(distanceValue.getChildren());
        final var indexVector = distanceValueArgs.get(0);
        final var queryVector = distanceValueArgs.get(1);
        final var operator = distanceValue.getOperator();
        distanceRankComparison = new Comparisons.DistanceRankValueComparison(distanceRankComparisonType, queryVector,
                comparand, efSearch, isReturningVectors);
        final WindowedValue windowedValue;
        switch (operator) {
            case EUCLIDEAN_DISTANCE:
                windowedValue = new EuclideanDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case EUCLIDEAN_SQUARE_DISTANCE:
                windowedValue = new EuclideanSquareDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case COSINE_DISTANCE:
                windowedValue = new CosineDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            case DOT_PRODUCT_DISTANCE:
                windowedValue = new DotProductDistanceRowNumberValue(getPartitioningValues(), ImmutableList.of(indexVector));
                break;
            default:
                throw new RecordCoreException("unexpected distance function " + operator.name());
        }
        return Optional.of(new ValuePredicate(windowedValue, distanceRankComparison));
    }

    @Nonnull
    @Override
    public PRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var rowNumberValueProtoBuilder = PRowNumberValue.newBuilder()
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
    public static final class RowNumberHighOrderFn extends BuiltInFunction<RowNumberHighOrderValue> {

        @Nonnull
        public static final String EF_SEARCH_ARGUMENT = VectorIndexScanOptions.HNSW_EF_SEARCH.getOptionName();

        @Nonnull
        public static final String INDEX_RETURNS_VECTORS_ARGUMENT = VectorIndexScanOptions.HNSW_RETURN_VECTORS.getOptionName();

        public RowNumberHighOrderFn() {
            super("row_number", ImmutableList.of(EF_SEARCH_ARGUMENT, INDEX_RETURNS_VECTORS_ARGUMENT),
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.INT), Type.primitiveType(Type.TypeCode.BOOLEAN)),
                    ImmutableList.of(Optional.of(LiteralValue.ofScalar(null)), Optional.of(LiteralValue.ofScalar(null))),
                    RowNumberHighOrderFn::encapsulateInternal);
        }

        @Nonnull
        @Override
        public HighOrderValue encapsulate(@Nonnull final Map<String, ? extends Typed> namedArguments) {
            SemanticException.check(namedArguments.size() <= 2,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            // Validate that namedArguments only contains EF_SEARCH_ARGUMENT or INDEX_RETURNS_VECTORS_ARGUMENT (or is empty)
            for (final String key : namedArguments.keySet()) {
                SemanticException.check(EF_SEARCH_ARGUMENT.equals(key) || INDEX_RETURNS_VECTORS_ARGUMENT.equals(key),
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            }

            final Typed efSearchValue = namedArguments.getOrDefault(EF_SEARCH_ARGUMENT, null);
            final Integer efSearch = efSearchValue == null ? null : (Integer)Verify.verifyNotNull(((LiteralValue<?>)efSearchValue).evalWithoutStore(EvaluationContext.EMPTY));

            final Typed indexReturnsVectorsValue = namedArguments.getOrDefault(INDEX_RETURNS_VECTORS_ARGUMENT, null);
            final Boolean indexReturnsValue = indexReturnsVectorsValue == null ? null : (Boolean)Verify.verifyNotNull(((LiteralValue<?>)indexReturnsVectorsValue).evalWithoutStore(EvaluationContext.EMPTY));

            return new RowNumberHighOrderValue(efSearch, indexReturnsValue);
        }

        @Nonnull
        private static RowNumberHighOrderValue encapsulateInternal(@Nonnull final BuiltInFunction<RowNumberHighOrderValue> ignored,
                                                                   @Nonnull final List<? extends Typed> arguments) {
            SemanticException.check(arguments.size() <= 2,
                    SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

            if (arguments.isEmpty()) {
                return new RowNumberHighOrderValue(null, null);
            }

            if (arguments.size() == 1) {
                final Typed argument = arguments.get(0);
                SemanticException.check(argument instanceof LiteralValue, SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                final var argumentValue = (LiteralValue<?>)argument;
                final var argumentType = argumentValue.getResultType().getTypeCode();
                if (argumentType.equals(Type.TypeCode.BOOLEAN)) {
                    boolean indexReturnsValue = (boolean)Verify.verifyNotNull(argumentValue.evalWithoutStore(EvaluationContext.EMPTY));
                    return new RowNumberHighOrderValue(null, indexReturnsValue);
                } else if (argumentType.equals(Type.TypeCode.INT)) {
                    int efSearch = (int)Verify.verifyNotNull(argumentValue.evalWithoutStore(EvaluationContext.EMPTY));
                    return new RowNumberHighOrderValue(efSearch, null);
                }
                SemanticException.fail(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES, "function undefined for given argument " + argumentValue);
            }

            final Typed efSearchValue = arguments.get(0);
            SemanticException.check(efSearchValue instanceof LiteralValue, SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            final int efSearch = (int)Verify.verifyNotNull(((LiteralValue<?>)efSearchValue).evalWithoutStore(EvaluationContext.EMPTY));

            final Typed indexReturnsVectorsValue = arguments.get(1);
            SemanticException.check(indexReturnsVectorsValue instanceof LiteralValue, SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            final boolean indexReturnsValue = (boolean)Verify.verifyNotNull(((LiteralValue<?>)indexReturnsVectorsValue).evalWithoutStore(EvaluationContext.EMPTY));

            return new RowNumberHighOrderValue(efSearch, indexReturnsValue);
        }
    }
}
