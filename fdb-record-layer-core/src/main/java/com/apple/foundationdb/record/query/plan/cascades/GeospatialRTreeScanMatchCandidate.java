/*
 * GeospatialRTreeScanMatchCandidate.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.DoubleValueOrParameter;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueComputationRuleSet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Preconditions;
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
 * Match candidate for a scan over an {@code GEOSPATIAL_RTREE} index. Structurally mirrors
 * {@link VectorIndexScanMatchCandidate}: both are non-covering scans that combine ordinary equality placeholders on a
 * leading segment with a single trailing composite predicate that carries query-side coordinates and radius through a
 * specialized {@link Comparisons.Comparison}.
 *
 * <p>The candidate's placeholder layout is:
 * <ul>
 *   <li>Zero or more <em>grouping</em> placeholders (one per grouping column of the index root's
 *       {@link com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression}, when present).</li>
 *   <li>A single trailing <em>coordinates</em> placeholder whose value is
 *       {@link com.apple.foundationdb.record.query.plan.cascades.values.HaversineDistanceValue}'s reduced 2-argument
 *       form and whose binding is a {@link Comparisons.WithinDistanceComparison} carrying the query-side
 *       {@code (centerLat, centerLon, radius)} triple.</li>
 * </ul>
 *
 * <p>No {@link MatchCandidate#computeBoundParameterPrefixMap} override is required: the coordinates placeholder is
 * last, {@code WITHIN_DISTANCE} is whitelisted in
 * {@link com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints.Builder#canBeUsedInScanPrefix}
 * as scan-prefix-eligible, and the default equality-then-stop-at-inequality walk naturally consumes every grouping
 * column plus the trailing within-distance range. The coordinates placeholder is registered as required for binding
 * in {@link GeospatialRTreeIndexExpansionVisitor} so that {@link com.apple.foundationdb.record.query.plan.cascades.rules.AbstractDataAccessRule}
 * skips any match in which the query did not supply a within-distance predicate; without that guard the planner
 * would spuriously promote leaf-level matches through {@link com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression#adjustMatch}
 * and try to build an R-tree scan with no coordinates or radius.
 *
 * <p>Only the grouping placeholders feed the candidate's ordering aliases -- the R-tree's Hilbert-curve traversal is
 * not a meaningful external sort, matching the legacy planner's {@code RecordQueryPlanner.planGeospatial} rejecting
 * any {@code sort != null}.
 *
 * @see GeospatialRTreeIndexExpansionVisitor for the expansion side that populates the placeholders.
 * @see GeospatialRTreeScanComparisons for the underlying scan-parameters type.
 * @see com.apple.foundationdb.record.query.plan.cascades.values.HaversineDistanceValue for the coordinates
 *      placeholder value.
 */
public class GeospatialRTreeScanMatchCandidate implements WithPrimaryKeyMatchCandidate, WithBaseQuantifierMatchCandidate {
    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> queriedRecordTypes;
    @Nonnull
    private final Traversal traversal;
    @Nonnull
    private final List<CorrelationIdentifier> parameters;
    @Nonnull
    private final List<CorrelationIdentifier> orderingAliases;
    @Nonnull
    private final Set<CorrelationIdentifier> parametersRequiredForBinding;
    @Nonnull
    private final Type.Record baseType;
    @Nonnull
    private final CorrelationIdentifier baseAlias;
    @Nonnull
    private final KeyExpression fullKeyExpression;
    @Nullable
    private final KeyExpression primaryKey;
    private final int prefixSize;
    @Nonnull
    private final Supplier<Optional<List<Value>>> primaryKeyValuesOptionalSupplier;

    public GeospatialRTreeScanMatchCandidate(@Nonnull final Index index,
                                             @Nonnull final Collection<RecordType> queriedRecordTypes,
                                             @Nonnull final Traversal traversal,
                                             @Nonnull final List<CorrelationIdentifier> parameters,
                                             @Nonnull final List<CorrelationIdentifier> orderingAliases,
                                             @Nonnull final Set<CorrelationIdentifier> parametersRequiredForBinding,
                                             @Nonnull final Type.Record baseType,
                                             @Nonnull final CorrelationIdentifier baseAlias,
                                             @Nonnull final KeyExpression fullKeyExpression,
                                             @Nullable final KeyExpression primaryKey,
                                             final int prefixSize) {
        Preconditions.checkArgument(prefixSize >= 0, "prefixSize must be non-negative");
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
        this.prefixSize = prefixSize;
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
    @Override
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

    public int getPrefixSize() {
        return prefixSize;
    }

    @Override
    public String toString() {
        return "geospatialRtree[" + getName() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Override
    public boolean isScopedToSingleType() {
        return queriedRecordTypes.size() == 1 || hasAndOrderedByRecordTypeKey();
    }

    @Nonnull
    @Override
    public Optional<List<Value>> getPrimaryKeyValuesMaybe() {
        return primaryKeyValuesOptionalSupplier.get();
    }

    @Nonnull
    @Override
    public List<OrderingPart.MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull final MatchInfo matchInfo,
                                                                              @Nonnull final List<CorrelationIdentifier> sortParameterIds,
                                                                              final boolean isReverse) {
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

            final var value =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());
            if (!normalizedValues.contains(value)) {
                final var matchedOrderingPart =
                        value.<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart>deriveOrderingPart(EvaluationContext.empty(),
                                AliasMap.emptyMap(), ImmutableSet.of(),
                                (v, sortOrder) ->
                                        OrderingPart.MatchedOrderingPart.of(parameterId, v, comparisonRange, sortOrder),
                                OrderingValueComputationRuleSet.usingMatchedOrderingParts());
                if (normalizedValues.add(matchedOrderingPart.getValue())) {
                    builder.add(matchedOrderingPart);
                }
            }
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons,
                                                       final boolean isReverse,
                                                       final boolean isDistinct) {
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Ordering.Binding>builder();
        final var normalizedKeyExpressions = getFullKeyExpression().normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

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
        // Only grouping-prefix columns advertise sort order; coordinate and primary-key columns
        // are laid out along the Hilbert curve, so claiming them sorted would let downstream
        // operators (ORDER BY, ordered union/intersection) rely on a false ordering.
        for (var i = scanComparisons.getEqualitySize(); i < prefixSize; i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                break;
            }

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
    public RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final PlanContext planContext,
                                            @Nonnull final Memoizer memoizer,
                                            @Nonnull final List<ComparisonRange> comparisonRanges,
                                            final boolean reverseScanOrder) {
        final var matchInfo = partialMatch.getRegularMatchInfo();
        Verify.verify(comparisonRanges.size() >= prefixSize + 1,
                "geospatial R-tree scan requires bindings for the full grouping prefix and the coordinates placeholder");

        final ScanComparisons prefixScanComparisons =
                ScanComparisons.fromComparisonRanges(comparisonRanges.subList(0, prefixSize));

        final Comparisons.WithinDistanceComparison withinDistance =
                extractWithinDistanceComparison(comparisonRanges.get(prefixSize));

        final GeospatialRTreeScanComparisons scanParameters = new GeospatialRTreeScanComparisons(
                prefixScanComparisons,
                DoubleValueOrParameter.valueExpression(withinDistance.getCenterLatitudeValue()),
                DoubleValueOrParameter.valueExpression(withinDistance.getCenterLongitudeValue()),
                DoubleValueOrParameter.valueExpression(withinDistance.getRadiusMetersValue()),
                ScanComparisons.EMPTY);

        return new RecordQueryIndexPlan(index.getName(),
                primaryKey,
                scanParameters,
                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                reverseScanOrder,
                false,
                partialMatch.getMatchCandidate(),
                baseType,
                matchInfo.getConstraint());
    }

    /**
     * Extract the single {@link Comparisons.WithinDistanceComparison} carried by the coordinates-placeholder
     * comparison range. The comparison is produced as an inequality by
     * {@link com.apple.foundationdb.record.query.plan.cascades.values.HaversineDistanceValue#transformComparisonMaybe},
     * so this expects an inequality range with exactly one comparison of that type; anything else indicates the
     * matcher accepted a shape the visitor cannot plan.
     */
    @Nonnull
    private static Comparisons.WithinDistanceComparison extractWithinDistanceComparison(@Nonnull final ComparisonRange comparisonRange) {
        if (!comparisonRange.isInequality()) {
            throw new RecordCoreException("geospatial R-tree coordinates placeholder must bind to an inequality range");
        }
        final List<Comparisons.Comparison> comparisons = comparisonRange.getInequalityComparisons();
        if (comparisons.size() != 1 || !(comparisons.get(0) instanceof Comparisons.WithinDistanceComparison)) {
            throw new RecordCoreException("geospatial R-tree coordinates placeholder must bind to exactly one WithinDistanceComparison");
        }
        return (Comparisons.WithinDistanceComparison) comparisons.get(0);
    }
}
