/*
 * MultidimensionalIndexScanMatchCandidate.java
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
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Match candidate for a scan over an {@code MULTIDIMENSIONAL} R-tree index. Mirrors
 * {@link VectorIndexScanMatchCandidate}'s shape (both are non-covering scans that carry independent per-column
 * placeholders); the notable divergences are:
 *
 * <ul>
 *   <li>The index columns are partitioned into three ordered segments -- <em>prefix</em>, <em>dimensions</em>,
 *       <em>suffix</em> -- whose sizes are threaded in via {@link #getPrefixSize()} and {@link #getDimensionsSize()}.
 *       {@link #toEquivalentPlan} splits the incoming flat {@link ComparisonRange} list positionally along these
 *       boundaries and feeds each segment into
 *       {@link MultidimensionalIndexScanComparisons#byValue(ScanComparisons, List, ScanComparisons)}.</li>
 *   <li>The default {@link MatchCandidate#computeBoundParameterPrefixMap} walk stops after the first non-equality
 *       binding -- correct for an ordinary index, wrong for R-tree, where every dimension carries an independent
 *       range simultaneously. The override in this class requires the whole prefix to be equality-bound and every
 *       dimension to have a non-empty range (returning an empty map otherwise so no partial-binding
 *       {@code toEquivalentPlan} is attempted), then applies the default equality-then-stop-at-inequality walk
 *       over the suffix. See {@link #computePrefixMap(List, Map, int, int)} for the segment-by-segment
 *       implementation.</li>
 * </ul>
 *
 * <p>Ordering aliases are restricted to the <em>prefix</em> columns only; the Hilbert-curve traversal that the
 * R-tree scan produces is not a meaningful external sort, so dimensions and suffix must not feed the candidate's
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression}. This mirrors the
 * legacy planner's {@code planGeospatial} rejecting any {@code sort != null}.</p>
 *
 * <p>Cascades matches placeholders by structural equality of their underlying {@link Value}, independent of the
 * query's AND-clause order. The legacy heuristic planner's {@code MultidimensionalAndWithThenPlanner} exists only
 * to work around {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}'s textual-order-sensitive
 * {@code Then} matcher; no analogue is needed here. A query supplying dimension filters in any AND-order will
 * match this candidate identically.</p>
 *
 * @see MultidimensionalIndexExpansionVisitor for the expansion side that populates the placeholders.
 * @see MultidimensionalIndexScanComparisons for the underlying scan-parameters type.
 */
public class MultidimensionalIndexScanMatchCandidate implements WithPrimaryKeyMatchCandidate, WithBaseQuantifierMatchCandidate {
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
    private final int dimensionsSize;
    @Nonnull
    private final Supplier<Optional<List<Value>>> primaryKeyValuesOptionalSupplier;

    public MultidimensionalIndexScanMatchCandidate(@Nonnull final Index index,
                                                   @Nonnull final Collection<RecordType> queriedRecordTypes,
                                                   @Nonnull final Traversal traversal,
                                                   @Nonnull final List<CorrelationIdentifier> parameters,
                                                   @Nonnull final List<CorrelationIdentifier> orderingAliases,
                                                   @Nonnull final Set<CorrelationIdentifier> parametersRequiredForBinding,
                                                   @Nonnull final Type.Record baseType,
                                                   @Nonnull final CorrelationIdentifier baseAlias,
                                                   @Nonnull final KeyExpression fullKeyExpression,
                                                   @Nullable final KeyExpression primaryKey,
                                                   final int prefixSize,
                                                   final int dimensionsSize) {
        Preconditions.checkArgument(prefixSize >= 0, "prefixSize must be non-negative");
        Preconditions.checkArgument(dimensionsSize > 0, "dimensionsSize must be positive");
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
        this.dimensionsSize = dimensionsSize;
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

    public int getDimensionsSize() {
        return dimensionsSize;
    }

    @Override
    public String toString() {
        return "multidimensional[" + getName() + "]";
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

    @Override
    public Map<CorrelationIdentifier, ComparisonRange> computeBoundParameterPrefixMap(@Nonnull final MatchInfo matchInfo) {
        return computePrefixMap(parameters,
                matchInfo.getRegularMatchInfo().getParameterBindingMap(),
                prefixSize,
                dimensionsSize);
    }

    /**
     * Package-private static core of {@link #computeBoundParameterPrefixMap} extracted for direct testability.
     *
     * <p>An R-tree scan is all-or-nothing at the prefix+dimensions boundary: the prefix must be fully equality-bound
     * (any unbound or inequality-bound prefix column disqualifies the scan) and every dimension must carry a non-empty
     * binding (an unbound dimension would collapse the hypercube's covering guarantee). When either invariant fails,
     * this returns an empty map so no partial-binding {@code toEquivalentPlan} is attempted. Otherwise, the prefix and
     * every dimension are placed into the map, followed by the ordinary equality-then-stop-at-inequality walk over
     * the suffix.</p>
     */
    @Nonnull
    static Map<CorrelationIdentifier, ComparisonRange> computePrefixMap(@Nonnull final List<CorrelationIdentifier> sargableAliases,
                                                                        @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                                                        final int prefixSize,
                                                                        final int dimensionsSize) {
        final HashMap<CorrelationIdentifier, ComparisonRange> prefixMap = Maps.newHashMap();

        // Prefix: must be entirely equality-bound, matching the legacy planner's
        // matchToMultidimensionalIndexScan requirement.
        for (int i = 0; i < prefixSize; i++) {
            final CorrelationIdentifier parameter = sargableAliases.get(i);
            @Nullable final ComparisonRange comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null || comparisonRange.getRangeType() != ComparisonRange.Type.EQUALITY) {
                return ImmutableMap.of();
            }
            prefixMap.put(parameter, comparisonRange);
        }

        // Dimensions: every alias must contribute a non-EMPTY range. Range kind (equality or inequality) is
        // admitted uniformly here; each dimension independently contributes whatever range it has.
        final int dimensionsEndExclusive = prefixSize + dimensionsSize;
        for (int i = prefixSize; i < dimensionsEndExclusive; i++) {
            final CorrelationIdentifier parameter = sargableAliases.get(i);
            @Nullable final ComparisonRange comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null || comparisonRange.getRangeType() == ComparisonRange.Type.EMPTY) {
                return ImmutableMap.of();
            }
            prefixMap.put(parameter, comparisonRange);
        }

        // Suffix: default equality-then-stop-at-inequality semantics.
        for (int i = dimensionsEndExclusive; i < sargableAliases.size(); i++) {
            final CorrelationIdentifier parameter = sargableAliases.get(i);
            @Nullable final ComparisonRange comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null) {
                return ImmutableMap.copyOf(prefixMap);
            }
            switch (comparisonRange.getRangeType()) {
                case EQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    break;
                case INEQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    return ImmutableMap.copyOf(prefixMap);
                case EMPTY:
                default:
                    return ImmutableMap.copyOf(prefixMap);
            }
        }

        return ImmutableMap.copyOf(prefixMap);
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
        // Only prefix columns advertise sort order; dimension, suffix, and primary-key columns
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
        Verify.verify(comparisonRanges.size() >= prefixSize + dimensionsSize,
                "multidimensional scan requires bindings for the full prefix and every dimension");

        final ScanComparisons prefixScanComparisons =
                ScanComparisons.fromComparisonRanges(comparisonRanges.subList(0, prefixSize));
        final ImmutableList.Builder<ScanComparisons> dimensionsScanComparisonsBuilder = ImmutableList.builder();
        for (int i = 0; i < dimensionsSize; i++) {
            dimensionsScanComparisonsBuilder.add(
                    ScanComparisons.fromComparisonRanges(ImmutableList.of(comparisonRanges.get(prefixSize + i))));
        }
        final ScanComparisons suffixScanComparisons =
                ScanComparisons.fromComparisonRanges(comparisonRanges.subList(prefixSize + dimensionsSize,
                        comparisonRanges.size()));

        final var scanParameters =
                MultidimensionalIndexScanComparisons.byValue(prefixScanComparisons,
                        dimensionsScanComparisonsBuilder.build(),
                        suffixScanComparisons);

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
}
