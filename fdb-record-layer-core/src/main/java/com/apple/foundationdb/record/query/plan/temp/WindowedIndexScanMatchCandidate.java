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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Case class to represent a match candidate that is backed by a windowed index such as a rank index.
 */
public class WindowedIndexScanMatchCandidate implements ScanWithFetchMatchCandidate {
    /**
     * Index metadata structure.
     */
    @Nonnull
    private final Index index;

    /**
     * Record types this index is defined over.
     */
    private final List<RecordType> recordTypes;

    /**
     * Base Alias.
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
    private final ExpressionRefTraversal traversal;

    @Nonnull
    private final KeyExpression alternativeKeyExpression;

    public WindowedIndexScanMatchCandidate(@Nonnull Index index,
                                           @Nonnull Collection<RecordType> recordTypes,
                                           @Nonnull final ExpressionRefTraversal traversal,
                                           @Nonnull final CorrelationIdentifier baseAlias,
                                           @Nonnull final List<CorrelationIdentifier> groupingAliases,
                                           @Nonnull final CorrelationIdentifier scoreAlias,
                                           @Nonnull final CorrelationIdentifier rankAlias,
                                           @Nonnull final List<CorrelationIdentifier> primaryKeyAliases,
                                           @Nonnull final List<Value> indexKeyValues,
                                           @Nonnull final KeyExpression alternativeKeyExpression) {
        this.index = index;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
        this.traversal = traversal;
        this.baseAlias = baseAlias;
        this.groupingAliases = ImmutableList.copyOf(groupingAliases);
        this.scoreAlias = scoreAlias;
        this.rankAlias = rankAlias;
        this.primaryKeyAliases = ImmutableList.copyOf(primaryKeyAliases);
        this.indexKeyValues = ImmutableList.copyOf(indexKeyValues);
        this.alternativeKeyExpression = alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    public List<RecordType> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    @Override
    public ExpressionRefTraversal getTraversal() {
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
    public CorrelationIdentifier getBaseAlias() {
        return baseAlias;
    }

    @Nonnull
    public List<Value> getIndexKeyValues() {
        return indexKeyValues;
    }

    @Nonnull
    @Override
    public KeyExpression getAlternativeKeyExpression() {
        return alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public List<BoundKeyPart> computeBoundKeyParts(@Nonnull MatchInfo matchInfo,
                                                   @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                                   boolean isReverse) {
        final var parameterBindingMap = matchInfo.getParameterBindingMap();
        final var parameterBindingPredicateMap = matchInfo.getParameterPredicateMap();

        final var normalizedKeys =
                getAlternativeKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<BoundKeyPart>builder();
        final var candidateParameterIds = getOrderingAliases();

        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            final var normalizedKey = normalizedKeys.get(ordinalInCandidate);
            Objects.requireNonNull(normalizedKey);
            Objects.requireNonNull(parameterId);
            @Nullable final var comparisonRange = parameterBindingMap.get(parameterId);
            @Nullable final var queryPredicate = parameterBindingPredicateMap.get(parameterId);

            Verify.verify(comparisonRange == null || comparisonRange.getRangeType() == ComparisonRange.Type.EMPTY || queryPredicate != null);

            if (parameterId.equals(scoreAlias)) {
                //
                // This is the score field of the index which is returned at this ordinal position.
                // Even though we may not have bound the score field itself via matching we may have bound the
                // rank (we should have). If the rank is bound by equality, the score is also bound by equality.
                // We need to record that.
                //
                @Nullable final var rankComparisonRange = parameterBindingMap.get(rankAlias);
                @Nullable final var rankQueryPredicate = parameterBindingPredicateMap.get(rankAlias);

                builder.add(
                        BoundKeyPart.of(normalizedKey,
                                rankComparisonRange == null ? ComparisonRange.Type.EMPTY : rankComparisonRange.getRangeType(),
                                rankQueryPredicate,
                                isReverse));
            } else {
                builder.add(
                        BoundKeyPart.of(normalizedKey,
                                comparisonRange == null ? ComparisonRange.Type.EMPTY : comparisonRange.getRangeType(),
                                queryPredicate,
                                isReverse));
            }
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var equalityBoundKeyMapBuilder = ImmutableSetMultimap.<KeyExpression, Comparisons.Comparison>builder();
        final var normalizedKeyExpressions = getAlternativeKeyExpression().normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();
        final var groupingExpression = (GroupingKeyExpression)index.getRootExpression();
        final var scoreOrdinal = groupingExpression.getGroupingCount();

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            if (i == scoreOrdinal) {
                equalityBoundKeyMapBuilder.put(normalizedKeyExpression, new Comparisons.OpaqueEqualityComparison());
            } else {
                equalityBoundKeyMapBuilder.put(normalizedKeyExpression, comparison);
            }
        }

        final var result = ImmutableList.<KeyPart>builder();
        for (int i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final KeyExpression currentKeyExpression = normalizedKeyExpressions.get(i);

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            result.add(KeyPart.of(currentKeyExpression, isReverse));
        }

        return new Ordering(equalityBoundKeyMapBuilder.build(), result.build(), isDistinct);
    }

    @Nonnull
    @Override
    public RelationalExpression toEquivalentExpression(@Nonnull final PartialMatch partialMatch,
                                                       @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of scan"));

        return tryFetchCoveringIndexScan(partialMatch, comparisonRanges, reverseScanOrder)
                .orElseGet(() ->
                        new RecordQueryIndexPlan(index.getName(),
                                IndexScanComparisons.byValue(toScanComparisons(comparisonRanges)),
                                reverseScanOrder,
                                false,
                                (ScanWithFetchMatchCandidate)partialMatch.getMatchCandidate()));
    }

    @Nonnull
    private Optional<RelationalExpression> tryFetchCoveringIndexScan(@Nonnull final PartialMatch partialMatch,
                                                                     @Nonnull final List<ComparisonRange> comparisonRanges,
                                                                     final boolean isReverse) {
        if (recordTypes.size() > 1) {
            return Optional.empty();
        }

        final RecordType recordType = Iterables.getOnlyElement(recordTypes);
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        final Value baseObjectValue = QuantifiedObjectValue.of(baseAlias);
        for (int i = 0; i < indexKeyValues.size(); i++) {
            final Value keyValue = indexKeyValues.get(i);
            if (keyValue instanceof FieldValue && keyValue.isFunctionallyDependentOn(baseObjectValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, i);
                addCoveringField(builder, (FieldValue)keyValue, fieldData);
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        final IndexScanParameters scanParameters = new IndexScanComparisons(IndexScanType.BY_RANK, toScanComparisons(comparisonRanges));
        final RecordQueryPlanWithIndex indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        scanParameters,
                        isReverse,
                        false,
                        (WindowedIndexScanMatchCandidate)partialMatch.getMatchCandidate());

        final RecordQueryCoveringIndexPlan coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                recordType.getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                builder.build());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(coveringIndexPlan, coveringIndexPlan::pushValueThroughFetch));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                                 @Nonnull QuantifiedObjectValue indexRecordQuantifiedObjectValue) {

        final Set<Value> quantifiedObjectValues = ImmutableSet.copyOf(value.filter(v -> v instanceof QuantifiedObjectValue));

        // if this is a value that is referring to more than one value from its quantifier or two multiple quantifiers
        if (quantifiedObjectValues.size() != 1) {
            return Optional.empty();
        }

        final QuantifiedObjectValue quantifiedObjectValue = (QuantifiedObjectValue)Iterables.getOnlyElement(quantifiedObjectValues);

        // replace the quantified column value inside the given value with the quantified value in the match candidate
        final var baseObjectValue = QuantifiedObjectValue.of(baseAlias);
        final Optional<Value> translatedValueOptional =
                value.translate(ImmutableMap.of(quantifiedObjectValue, baseObjectValue));
        if (!translatedValueOptional.isPresent()) {
            return Optional.empty();
        }
        final Value translatedValue = translatedValueOptional.get();
        final AliasMap equivalenceMap = AliasMap.identitiesFor(ImmutableSet.of(baseAlias));

        for (final Value matchResultValue : Iterables.concat(ImmutableList.of(baseObjectValue), indexKeyValues)) {
            final Set<CorrelationIdentifier> resultValueCorrelatedTo = matchResultValue.getCorrelatedTo();
            if (resultValueCorrelatedTo.size() != 1) {
                continue;
            }
            if (translatedValue.semanticEquals(matchResultValue, equivalenceMap)) {
                return matchResultValue.translate(ImmutableMap.of(baseObjectValue, indexRecordQuantifiedObjectValue));
            }
        }

        return Optional.empty();
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    private static void addCoveringField(@Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                         @Nonnull FieldValue fieldValue,
                                         @Nonnull AvailableFields.FieldData fieldData) {
        for (final String fieldName : fieldValue.getFieldPrefix()) {
            builder = builder.getFieldBuilder(fieldName);
        }

        // TODO not sure what to do with the null standing requirement

        final String fieldName = fieldValue.getFieldName();
        if (!builder.hasField(fieldName)) {
            builder.addField(fieldName, fieldData.getSource(), fieldData.getIndex());
        }
    }

    @Nonnull
    public static List<CorrelationIdentifier> orderingAliases(@Nonnull final List<CorrelationIdentifier> groupingAliases,
                                                              @Nonnull final CorrelationIdentifier scoreAlias,
                                                              @Nonnull final List<CorrelationIdentifier> primaryKeyAliases) {
        return ImmutableList.<CorrelationIdentifier>builder().addAll(groupingAliases).add(scoreAlias).addAll(primaryKeyAliases).build();
    }
}
