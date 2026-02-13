/*
 * VectorIndexScanMatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueComputationRuleSet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A specialized match candidate for vector similarity search queries backed by vector similarity indexes
 * (such as HNSW - Hierarchical Navigable Small World indexes <a href="https://arxiv.org/abs/1603.09320">https://arxiv.org/abs/1603.09320</a>).
 * <p>
 * This class represents a potential query plan option that uses a vector index to efficiently perform
 * K-nearest neighbor (K-NN) searches and similarity-based filtering. It extends the standard index scan
 * matching framework to handle the unique characteristics of vector similarity searches, particularly
 * the notion of distance-based ranking and specialized comparison types.
 * </p>
 *
 * <h2>Vector Similarity Search Pattern</h2>
 * <p>
 * This match candidate is designed to recognize and optimize queries following the pattern:
 * <pre>
 * SELECT ... FROM table
 * WHERE partition_key = value
 * QUALIFY ROW_NUMBER() OVER (
 *   PARTITION BY partition_key
 *   ORDER BY distance_function(vector_field, query_vector)
 * ) &lt;= k
 * </pre>
 * The query planner transforms such queries into patterns involving {@link Comparisons.DistanceRankValueComparison}
 * predicates (via {@link com.apple.foundationdb.record.query.plan.cascades.values.RowNumberValue#transformComparisonMaybe}),
 * which this match candidate can then satisfy using the vector index.
 * </p>
 *
 * <h2>Comparison Handling</h2>
 * <p>
 * The class distinguishes between two types of comparisons:
 * <ul>
 *   <li><strong>Partition/Filter Comparisons:</strong> Standard equality and inequality comparisons
 *       on partition keys and other indexed fields (e.g., {@code zone = 'us-west'})</li>
 *   <li><strong>Distance Rank Comparisons:</strong> Specialized {@link Comparisons.DistanceRankValueComparison}
 *       predicates that specify the K-nearest neighbor constraint and query vector</li>
 * </ul>
 * The {@link #toVectorIndexScanComparisons} method separates these comparison types and constructs
 * appropriate {@link VectorIndexScanComparisons} that the execution engine can process.
 * </p>
 *
 * <h2>Constraints and Limitations</h2>
 * <ul>
 *   <li><strong>Single Distance Rank:</strong> Currently supports exactly one distance rank comparison
 *       per query (verified at line 269)</li>
 *   <li><strong>Partition Requirement:</strong> The index must be partitioned, and queries must provide
 *       partition key values to narrow the search space</li>
 *   <li><strong>Distance Function Matching:</strong> The distance function in the query must match
 *       the metric configured in the vector index</li>
 * </ul>
 *
 * @see VectorIndexScanComparisons for the scan comparison structure
 * @see Comparisons.DistanceRankValueComparison for distance-based ranking predicates
 * @see com.apple.foundationdb.record.query.plan.cascades.values.RowNumberValue for comparison transformation
 */
public class VectorIndexScanMatchCandidate implements WithPrimaryKeyMatchCandidate {
    /**
     * Index metadata structure.
     */
    @Nonnull
    private final Index index;

    /**
     * Record types this index is defined over.
     */
    private final List<RecordType> queriedRecordTypes;

    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    @Nonnull
    private final List<CorrelationIdentifier> orderingAliases;

    @Nonnull
    private final Set<CorrelationIdentifier> parametersRequiredForBinding;

    /**
     * Base type.
     */
    @Nonnull
    private final Type.Record baseType;

    /**
     * Base alias.
     */
    @Nonnull
    private final CorrelationIdentifier baseAlias;

    /**
     * Traversal object of the expanded index scan graph.
     */
    @Nonnull
    private final Traversal traversal;

    @Nonnull
    private final KeyExpression fullKeyExpression;

    @Nullable
    private final KeyExpression primaryKey;

    @Nonnull
    private final Supplier<Optional<List<Value>>> primaryKeyValuesOptionalSupplier;

    public VectorIndexScanMatchCandidate(@Nonnull final Index index,
                                         @Nonnull final Collection<RecordType> queriedRecordTypes,
                                         @Nonnull final Traversal traversal,
                                         @Nonnull final List<CorrelationIdentifier> parameters,
                                         @Nonnull final List<CorrelationIdentifier> orderingAliases,
                                         @Nonnull final Set<CorrelationIdentifier> parametersRequiredForBinding,
                                         @Nonnull final Type.Record baseType,
                                         @Nonnull final CorrelationIdentifier baseAlias,
                                         @Nonnull final KeyExpression fullKeyExpression,
                                         @Nullable final KeyExpression primaryKey) {
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.orderingAliases = ImmutableList.copyOf(orderingAliases);
        this.parametersRequiredForBinding = ImmutableSet.copyOf(parametersRequiredForBinding);
        this.baseType = baseType;
        this.baseAlias = baseAlias;
        this.fullKeyExpression = fullKeyExpression;
        this.primaryKey = primaryKey;
        this.primaryKeyValuesOptionalSupplier =
                Suppliers.memoize(() -> MatchCandidate.computePrimaryKeyValuesMaybe(primaryKey, baseType));
    }

    @Override
    public int getColumnSize() {
        return index.getColumnSize();
    }

    @Override
    public boolean isUnique() {
        return index.isUnique();
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return queriedRecordTypes;
    }

    @Nonnull
    @Override
    public Traversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return parameters;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getSargableAliasesRequiredForBinding() {
        return parametersRequiredForBinding;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return orderingAliases;
    }

    @Nonnull
    public Type.Record getBaseType() {
        return baseType;
    }

    @Nonnull
    public CorrelationIdentifier getBaseAlias() {
        return baseAlias;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return fullKeyExpression;
    }

    @Override
    public String toString() {
        return "vector[" + getName() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Nonnull
    @Override
    public List<OrderingPart.MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull final MatchInfo matchInfo, @Nonnull final List<CorrelationIdentifier> sortParameterIds, final boolean isReverse) {
        final var parameterBindingMap =
                matchInfo.getRegularMatchInfo().getParameterBindingMap();

        final var normalizedKeyExpressions =
                getFullKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<OrderingPart.MatchedOrderingPart>builder();
        final var candidateParameterIds = getOrderingAliases();
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            final var normalizedKeyExpression = normalizedKeyExpressions.get(ordinalInCandidate);

            Objects.requireNonNull(parameterId);
            Objects.requireNonNull(normalizedKeyExpression);
            @Nullable final var comparisonRange = parameterBindingMap.get(parameterId);

            if (normalizedKeyExpression.createsDuplicates()) {
                if (comparisonRange != null) {
                    if (comparisonRange.getRangeType() == ComparisonRange.Type.EQUALITY) {
                        continue;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            //
            // Compute a Value for this normalized key.
            //
            final var value =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());
            if (normalizedValues.add(value)) {
                final var matchedOrderingPart =
                        value.<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart>deriveOrderingPart(EvaluationContext.empty(),
                                AliasMap.emptyMap(), ImmutableSet.of(),
                                (v, sortOrder) ->
                                        OrderingPart.MatchedOrderingPart.of(parameterId, v, comparisonRange, sortOrder),
                                OrderingValueComputationRuleSet.usingMatchedOrderingParts());
                builder.add(matchedOrderingPart);
            }
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Ordering.Binding>builder();
        final var normalizedKeyExpressions = getFullKeyExpression().normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

        // We keep a set for normalized values in order to check for duplicate values in the index definition.
        // We correct here for the case where an index is defined over {a, a} since its order is still just {a}.
        final var seenValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                continue;
            }

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            final var simplifiedComparisonPairOptional =
                    MatchCandidate.simplifyComparisonMaybe(normalizedValue, comparison);
            if (simplifiedComparisonPairOptional.isEmpty()) {
                continue;
            }
            final var simplifiedComparisonPair = simplifiedComparisonPairOptional.get();
            bindingMapBuilder.put(simplifiedComparisonPair.getLeft(), Ordering.Binding.fixed(simplifiedComparisonPair.getRight()));
            seenValues.add(simplifiedComparisonPair.getLeft());
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (var i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                break;
            }

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            final var providedOrderingPart =
                    normalizedValue.deriveOrderingPart(EvaluationContext.empty(), AliasMap.emptyMap(),
                            ImmutableSet.of(), OrderingPart.ProvidedOrderingPart::new,
                            OrderingValueComputationRuleSet.usingProvidedOrderingParts());

            final var providedOrderingValue = providedOrderingPart.getValue();
            if (!seenValues.contains(providedOrderingValue)) {
                seenValues.add(providedOrderingValue);
                bindingMapBuilder.put(providedOrderingValue,
                        Ordering.Binding.sorted(providedOrderingPart.getSortOrder()
                                .flipIfReverse(isReverse)));
                orderingSequenceBuilder.add(providedOrderingValue);
            }
        }

        return Ordering.ofOrderingSequence(bindingMapBuilder.build(), orderingSequenceBuilder.build(), isDistinct);
    }

    @Nonnull
    @Override
    public Optional<List<Value>> getPrimaryKeyValuesMaybe() {
        return primaryKeyValuesOptionalSupplier.get();
    }

    @Nonnull
    @Override
    public RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final PlanContext planContext,
                                            @Nonnull final Memoizer memoizer,
                                            @Nonnull final List<ComparisonRange> comparisonRanges,
                                            final boolean reverseScanOrder) {
        final var matchInfo = partialMatch.getRegularMatchInfo();
        final var vectorIndexScanComparison = toVectorIndexScanComparisons(comparisonRanges);
        return new RecordQueryIndexPlan(index.getName(),
                primaryKey,
                vectorIndexScanComparison,
                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                reverseScanOrder,
                false,
                partialMatch.getMatchCandidate(),
                baseType,
                matchInfo.getConstraint());
    }

    @Nonnull
    private static VectorIndexScanComparisons toVectorIndexScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        final var scanRangesBuilder = ImmutableList.<ComparisonRange>builder();
        final var distanceRankComparisonsBuilder = ImmutableList.<Comparisons.DistanceRankValueComparison>builder();

        comparisonRanges.forEach(comparisonRange -> {
            if (comparisonRange.isEquality()
                    && comparisonRange.getEqualityComparison() instanceof Comparisons.DistanceRankValueComparison) {
                distanceRankComparisonsBuilder.add((Comparisons.DistanceRankValueComparison)comparisonRange.getEqualityComparison());
            } else if (comparisonRange.isInequality()
                    && comparisonRange.getInequalityComparisons()
                    .stream().allMatch(comp -> comp instanceof Comparisons.DistanceRankValueComparison)) {
                distanceRankComparisonsBuilder.addAll(comparisonRange.getInequalityComparisons().stream()
                        .map(comp -> (Comparisons.DistanceRankValueComparison)comp)
                        .collect(ImmutableList.toImmutableList()));
            } else {
                scanRangesBuilder.add(comparisonRange);
            }
        });
        final var rankComparisons = distanceRankComparisonsBuilder.build();
        // currently, exactly one distance rank comparison is supported by the index.
        Verify.verify(rankComparisons.size() == 1, "attempt to create vector scan comparison without any rank comparison");

        return VectorIndexScanComparisons.byDistance(toScanComparisons(scanRangesBuilder.build()),
                distanceRankComparisonsBuilder.build().get(0));
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }
}
