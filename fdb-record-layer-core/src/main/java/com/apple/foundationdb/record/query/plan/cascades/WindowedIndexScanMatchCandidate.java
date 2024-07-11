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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.ImmutableIntArray;

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
    @Override
    public List<MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull MatchInfo matchInfo,
                                                                 @Nonnull List<CorrelationIdentifier> sortParameterIds,
                                                                 boolean isReverse) {
        final var parameterBindingMap = matchInfo.getParameterBindingMap();

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

                    builder.add(
                            MatchedOrderingPart.of(rankAlias, normalizedValue, rankComparisonRange, MatchedSortOrder.ASCENDING));
                } else {
                    builder.add(
                            MatchedOrderingPart.of(parameterId, normalizedValue, comparisonRange, MatchedSortOrder.ASCENDING));
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
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                continue;
            }

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            normalizedValues.add(normalizedValue);
            if (i == scoreOrdinal) {
                bindingMapBuilder.put(normalizedValue, Binding.fixed(new Comparisons.OpaqueEqualityComparison()));
            } else {
                bindingMapBuilder.put(normalizedValue, Binding.fixed(comparison));
            }
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (int i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final KeyExpression normalizedKeyExpression = normalizedKeyExpressions.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                break;
            }

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            getBaseType());

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //

            if (!normalizedValues.contains(normalizedValue)) {
                normalizedValues.add(normalizedValue);
                bindingMapBuilder.put(normalizedValue, Binding.sorted(isReverse));
                orderingSequenceBuilder.add(normalizedValue);
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
        final var baseRecordType = Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes));
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
                                QueryPlanConstraint.tautology()));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    private Optional<RecordQueryPlan> tryFetchCoveringIndexScan(@Nonnull final PartialMatch partialMatch,
                                                                @Nonnull final PlanContext planContext,
                                                                @Nonnull final Memoizer memoizer,
                                                                @Nonnull final List<ComparisonRange> comparisonRanges,
                                                                final boolean isReverse,
                                                                @Nonnull final Type.Record baseRecordType) {
        if (queriedRecordTypes.size() > 1) {
            return Optional.empty();
        }

        final RecordType recordType = Iterables.getOnlyElement(queriedRecordTypes);
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        final Value baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseRecordType);
        for (int i = 0; i < indexKeyValues.size(); i++) {
            final Value keyValue = indexKeyValues.get(i);
            if (keyValue instanceof FieldValue && keyValue.isFunctionallyDependentOn(baseObjectValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(i));
                if (!addCoveringField(builder, (FieldValue)keyValue, fieldData)) {
                    return Optional.empty();
                }
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        final IndexScanParameters scanParameters = new IndexScanComparisons(IndexScanType.BY_RANK, toScanComparisons(comparisonRanges));
        final RecordQueryPlanWithIndex indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        primaryKey,
                        scanParameters,
                        planContext.getPlannerConfiguration().getIndexFetchMethod(),
                        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        isReverse,
                        false,
                        partialMatch.getMatchCandidate(),
                        baseRecordType,
                        QueryPlanConstraint.tautology());

        final RecordQueryCoveringIndexPlan coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                recordType.getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                builder.build());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(memoizer.memoizePlans(coveringIndexPlan)), coveringIndexPlan::pushValueThroughFetch, baseRecordType, RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value toBePushedValue,
                                                 @Nonnull CorrelationIdentifier sourceAlias,
                                                 @Nonnull CorrelationIdentifier targetAlias) {
        return ScanWithFetchMatchCandidate.pushValueThroughFetch(toBePushedValue,
                baseAlias,
                sourceAlias,
                targetAlias,
                indexKeyValues);
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    private static boolean addCoveringField(@Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                            @Nonnull FieldValue fieldValue,
                                            @Nonnull AvailableFields.FieldData fieldData) {
        // TODO field names are for debugging purposes only, we should probably use field ordinals here instead.
        for (final var maybeFieldName : fieldValue.getFieldPrefix().getOptionalFieldNames()) {
            if (maybeFieldName.isEmpty()) {
                return false;
            }
            builder = builder.getFieldBuilder(maybeFieldName.get());
        }

        // TODO not sure what to do with the null standing requirement

        final var maybeFieldName = fieldValue.getLastFieldName();
        if (maybeFieldName.isEmpty()) {
            return false;
        }
        final String fieldName = maybeFieldName.get();
        if (!builder.hasField(fieldName)) {
            builder.addField(fieldName, fieldData.getSource(),
                    new AvailableFields.TruePredicate(), fieldData.getOrdinalPath(), fieldData.getInvertibleFunction());
        }
        return true;
    }

    @Nonnull
    public static List<CorrelationIdentifier> orderingAliases(@Nonnull final List<CorrelationIdentifier> groupingAliases,
                                                              @Nonnull final CorrelationIdentifier scoreAlias,
                                                              @Nonnull final List<CorrelationIdentifier> primaryKeyAliases) {
        return ImmutableList.<CorrelationIdentifier>builder().addAll(groupingAliases).add(scoreAlias).addAll(primaryKeyAliases).build();
    }
}
