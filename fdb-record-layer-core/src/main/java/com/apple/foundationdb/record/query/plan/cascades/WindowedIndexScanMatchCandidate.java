/*
 * MatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueComputationRuleSet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
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
import java.util.function.Supplier;

/**
 * Case class to represent a match candidate that is backed by a windowed index such as a rank index.
 */
public class WindowedIndexScanMatchCandidate implements ScanWithFetchMatchCandidate, WithBaseQuantifierMatchCandidate {
    /**
     * Index metadata structure.
     */
    @Nonnull
    private final Index index;

    /**
     * Record types this index is defined over.
     */
    private final List<RecordType> queriedRecordTypes;

    /**
     * Base type.
     */
    @Nonnull
    private final Type baseType;

    /**
     * Base alias.
     */
    @Nonnull
    private final CorrelationIdentifier baseAlias;

    /**
     * Holds the grouping aliases for all groupings that can to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> groupingAliases;

    /**
     * Holds the alias for the score placeholder in the match candidate.
     */
    @Nonnull
    private final CorrelationIdentifier scoreAlias;

    /**
     * Holds the alias for the rank placeholder in the match candidate.
     */
    @Nonnull
    private final CorrelationIdentifier rankAlias;

    /**
     * Holds the grouping aliases for all primary keys.
     */
    @Nonnull
    private final List<CorrelationIdentifier> primaryKeyAliases;

    /**
     * List of values that represent the key parts of the index represented by the candidate in the expanded graph.
     */
    @Nonnull
    private final List<Value> indexKeyValues;

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
    private final Supplier<Optional<List<Value>>> primaryKeyValuesSupplier;

    @Nonnull
    private final Supplier<Optional<IndexEntryToLogicalRecord>> indexEntryToLogicalRecordOptionalSupplier;

    public WindowedIndexScanMatchCandidate(@Nonnull Index index,
                                           @Nonnull Collection<RecordType> queriedRecordTypes,
                                           @Nonnull final Traversal traversal,
                                           @Nonnull final Type baseType,
                                           @Nonnull final CorrelationIdentifier baseAlias,
                                           @Nonnull final List<CorrelationIdentifier> groupingAliases,
                                           @Nonnull final CorrelationIdentifier scoreAlias,
                                           @Nonnull final CorrelationIdentifier rankAlias,
                                           @Nonnull final List<CorrelationIdentifier> primaryKeyAliases,
                                           @Nonnull final List<Value> indexKeyValues,
                                           @Nonnull final KeyExpression fullKeyExpression,
                                           @Nullable final KeyExpression primaryKey) {
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.traversal = traversal;
        this.baseType = baseType;
        this.baseAlias = baseAlias;
        this.groupingAliases = ImmutableList.copyOf(groupingAliases);
        this.scoreAlias = scoreAlias;
        this.rankAlias = rankAlias;
        this.primaryKeyAliases = ImmutableList.copyOf(primaryKeyAliases);
        this.indexKeyValues = ImmutableList.copyOf(indexKeyValues);
        this.fullKeyExpression = fullKeyExpression;
        this.primaryKey = primaryKey;
        this.primaryKeyValuesSupplier = Suppliers.memoize(() -> MatchCandidate.computePrimaryKeyValuesMaybe(primaryKey, baseType));
        this.indexEntryToLogicalRecordOptionalSupplier =
                Suppliers.memoize(() -> ScanWithFetchMatchCandidate.computeIndexEntryToLogicalRecord(queriedRecordTypes,
                        baseAlias, baseType, indexKeyValues, ImmutableList.of()));
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
        return ImmutableList.<CorrelationIdentifier>builder().addAll(groupingAliases).add(rankAlias).build();
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return orderingAliases(groupingAliases, scoreAlias, primaryKeyAliases);
    }

    @Nonnull
    @Override
    public Type getBaseType() {
        return baseType;
    }

    @Nonnull
    public List<Value> getIndexKeyValues() {
        return indexKeyValues;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return fullKeyExpression;
    }

    @Override
    public String toString() {
        return "Windowed[" + getName() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Nonnull
    @Override
    public Optional<List<Value>> getPrimaryKeyValuesMaybe() {
        return primaryKeyValuesSupplier.get();
    }

    @Nonnull
    private Optional<IndexEntryToLogicalRecord> getIndexEntryToLogicalRecordMaybe() {
        return indexEntryToLogicalRecordOptionalSupplier.get();
    }

    @Nonnull
    @Override
    public List<MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull MatchInfo matchInfo,
                                                                 @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                                                 boolean isReverse) {
        final var parameterBindingMap =
                matchInfo.getRegularMatchInfo().getParameterBindingMap();

        final var normalizedKeyExpressions =
                getFullKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<MatchedOrderingPart>builder();
        final var candidateParameterIds = getOrderingAliases();
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            final var normalizedKeyExpression = normalizedKeyExpressions.get(ordinalInCandidate);
            Objects.requireNonNull(normalizedKeyExpression);
            Objects.requireNonNull(parameterId);
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

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            if (normalizedValues.add(normalizedValue)) {
                if (parameterId.equals(scoreAlias)) {
                    //
                    // This is the score field of the index which is returned at this ordinal position.
                    // Even though we may not have bound the score field itself via matching we may have bound the
                    // rank (we should have). If the rank is bound by equality, the score is also bound by equality.
                    // We need to record that.
                    //
                    @Nullable final var rankComparisonRange = parameterBindingMap.get(rankAlias);

                    final var matchedOrderingPart =
                            normalizedValue.<MatchedSortOrder, MatchedOrderingPart>deriveOrderingPart(EvaluationContext.empty(),
                                    AliasMap.emptyMap(), ImmutableSet.of(),
                                    (v, sortOrder) ->
                                            MatchedOrderingPart.of(rankAlias, v, rankComparisonRange, sortOrder),
                                    OrderingValueComputationRuleSet.usingMatchedOrderingParts());
                    builder.add(matchedOrderingPart);
                } else {
                    final var matchedOrderingPart =
                            normalizedValue.<MatchedSortOrder, MatchedOrderingPart>deriveOrderingPart(EvaluationContext.empty(),
                                    AliasMap.emptyMap(), ImmutableSet.of(),
                                    (v, sortOrder) ->
                                            MatchedOrderingPart.of(parameterId, v, comparisonRange, sortOrder),
                                    OrderingValueComputationRuleSet.usingMatchedOrderingParts());
                    builder.add(matchedOrderingPart);
                }
            }
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        final var normalizedKeyExpressions = getFullKeyExpression().normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();
        final var groupingExpression = (GroupingKeyExpression)index.getRootExpression();
        final var scoreOrdinal = groupingExpression.getGroupingCount();

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

            seenValues.add(normalizedValue);
            if (i == scoreOrdinal) {
                bindingMapBuilder.put(normalizedValue, Binding.fixed(new Comparisons.OpaqueEqualityComparison()));
                seenValues.add(normalizedValue);
            } else {
                final var simplifiedComparisonPairOptional =
                        MatchCandidate.simplifyComparisonMaybe(normalizedValue, comparison);
                if (simplifiedComparisonPairOptional.isEmpty()) {
                    continue;
                }
                final var simplifiedComparisonPair = simplifiedComparisonPairOptional.get();
                bindingMapBuilder.put(simplifiedComparisonPair.getLeft(), Binding.fixed(simplifiedComparisonPair.getRight()));
                seenValues.add(simplifiedComparisonPair.getLeft());
            }
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (int i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final KeyExpression normalizedKeyExpression = normalizedKeyExpressions.get(i);

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
                        Binding.sorted(providedOrderingPart.getSortOrder()
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
        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes));
        return tryFetchCoveringIndexScan(partialMatch, planContext, memoizer, comparisonRanges, reverseScanOrder, baseRecordType)
                .orElseGet(() ->
                        new RecordQueryIndexPlan(index.getName(),
                                primaryKey,
                                new IndexScanComparisons(IndexScanType.BY_RANK, toScanComparisons(comparisonRanges)),
                                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                reverseScanOrder,
                                false,
                                partialMatch.getMatchCandidate(),
                                baseRecordType,
                                QueryPlanConstraint.noConstraint()));
    }

    @Nonnull
    private Optional<RecordQueryPlan> tryFetchCoveringIndexScan(@Nonnull final PartialMatch partialMatch,
                                                                @Nonnull final PlanContext planContext,
                                                                @Nonnull final Memoizer memoizer,
                                                                @Nonnull final List<ComparisonRange> comparisonRanges,
                                                                final boolean isReverse,
                                                                @Nonnull final Type.Record baseRecordType) {
        final var indexEntryToLogicalRecordOptional = getIndexEntryToLogicalRecordMaybe();
        if (indexEntryToLogicalRecordOptional.isEmpty()) {
            return Optional.empty();
        }
        final var indexEntryToLogicalRecord = indexEntryToLogicalRecordOptional.get();
        final var scanParameters = new IndexScanComparisons(IndexScanType.BY_RANK, toScanComparisons(comparisonRanges));
        final var indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        primaryKey,
                        scanParameters,
                        planContext.getPlannerConfiguration().getIndexFetchMethod(),
                        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        isReverse,
                        false,
                        partialMatch.getMatchCandidate(),
                        baseRecordType,
                        QueryPlanConstraint.noConstraint());

        final var coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                indexEntryToLogicalRecord.getQueriedRecordType().getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                indexEntryToLogicalRecord.getIndexKeyValueToPartialRecord());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(memoizer.memoizePlan(coveringIndexPlan)),
                coveringIndexPlan::pushValueThroughFetch, baseRecordType, RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value toBePushedValue,
                                                 @Nonnull CorrelationIdentifier sourceAlias,
                                                 @Nonnull CorrelationIdentifier targetAlias) {
        final var indexEntryToLogicalRecord =
                getIndexEntryToLogicalRecordMaybe().orElseThrow(() -> new RecordCoreException("need index entry to logical record"));

        return ScanWithFetchMatchCandidate.pushValueThroughFetch(toBePushedValue,
                baseAlias,
                sourceAlias,
                targetAlias,
                indexEntryToLogicalRecord.getLogicalKeyValues());
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    @Nonnull
    public static List<CorrelationIdentifier> orderingAliases(@Nonnull final List<CorrelationIdentifier> groupingAliases,
                                                              @Nonnull final CorrelationIdentifier scoreAlias,
                                                              @Nonnull final List<CorrelationIdentifier> primaryKeyAliases) {
        return ImmutableList.<CorrelationIdentifier>builder().addAll(groupingAliases).add(scoreAlias).addAll(primaryKeyAliases).build();
    }
}
