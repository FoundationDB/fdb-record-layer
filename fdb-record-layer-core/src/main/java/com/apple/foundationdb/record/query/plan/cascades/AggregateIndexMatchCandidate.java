/*
 * AggregateIndexMatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Case class that represents a grouping index with aggregate function(s).
 */
public class AggregateIndexMatchCandidate implements MatchCandidate, WithBaseQuantifierMatchCandidate {

    // The backing index metadata structure.
    @Nonnull
    private final Index index;

    // The expression representation of the match candidate.
    @Nonnull
    private final Traversal traversal;

    // list of aliases pertaining ordering information.
    @Nonnull
    private final List<CorrelationIdentifier> sargableAndOrderAliases;

    // list of base indexed record types.
    @Nonnull
    private final List<RecordType> recordTypes;

    @Nonnull
    private final Type baseType;

    @Nonnull
    private final Value groupByResultValue;

    @Nonnull
    private final Value selectHavingResultValue;

    /**
     * Creates a new instance of {@link AggregateIndexMatchCandidate}.
     *
     * @param index The underlying index.
     * @param traversal The expression representation of the match candidate.
     * @param sargableAndOrderAliases A list of sargable and order aliases.
     * @param recordTypes The underlying base record types.
     * @param baseType The base type.
     * @param groupByResultValue The group by expression result value.
     * @param selectHavingResultValue The select-having expression result value.
     */
    public AggregateIndexMatchCandidate(@Nonnull final Index index,
                                        @Nonnull final Traversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> sargableAndOrderAliases,
                                        @Nonnull final Collection<RecordType> recordTypes,
                                        @Nonnull final Type baseType,
                                        @Nonnull final Value groupByResultValue,
                                        @Nonnull final Value selectHavingResultValue) {
        Preconditions.checkArgument(!recordTypes.isEmpty());
        this.index = index;
        this.traversal = traversal;
        this.sargableAndOrderAliases = sargableAndOrderAliases;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
        this.baseType = baseType;
        this.groupByResultValue = groupByResultValue;
        this.selectHavingResultValue = selectHavingResultValue;
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Nonnull
    @Override
    public Traversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return sargableAndOrderAliases; // only these for now, later on we should also add the aggregated column alias as well.
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return sargableAndOrderAliases;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return index.getRootExpression();
    }

    @Override
    public String toString() {
        return "Agg[" + getName() + "; " + index.getType() + "]";
    }

    @Override
    public boolean createsDuplicates() {
        return index.getRootExpression().createsDuplicates();
    }

    @Override
    public int getColumnSize() {
        return index.getColumnSize();
    }

    @Override
    public boolean isUnique() {
        return index.isUnique();
    }

    public boolean isPermuted() {
        return IndexTypes.PERMUTED_MAX.equals(index.getType()) || IndexTypes.PERMUTED_MIN.equals(index.getType());
    }

    private int getPermutedCount() {
        @Nullable String permutedSizeOption = index.getOption(IndexOptions.PERMUTED_SIZE_OPTION);
        return permutedSizeOption == null ? 0 : Integer.parseInt(permutedSizeOption);
    }

    @Nonnull
    @Override
    public List<MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull final MatchInfo matchInfo,
                                                                 @Nonnull final List<CorrelationIdentifier> sortParameterIds,
                                                                 final boolean isReverse) {
        final var parameterBindingMap = matchInfo.getParameterBindingMap();
        final var normalizedKeyExpressions =
                getFullKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<MatchedOrderingPart>builder();
        final var candidateParameterIds = getOrderingAliases();
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        final List<Value> deconstructedValue = Values.deconstructRecord(selectHavingResultValue);
        final AliasMap aliasMap = AliasMap.ofAliases(Iterables.getOnlyElement(selectHavingResultValue.getCorrelatedTo()), Quantifier.current());

        // Compute the ordering for this index by collecting the result values of the selectHaving statement
        // associated with each sortParameterId. Note that for most aggregate indexes, the aggregate value is
        // in the FDB value, and so it does not contribute to the ordering of the index, so there is no sortParameterId
        // corresponding to it. For the PERMUTED_MIN and PERMUTED_MAX indexes, the aggregate _does_ have a corresponding
        // sortParameterId. Its position is determined by the permutedSize option, handled below by adjusting the
        // sortParameterId's index before looking it up in the original key expression
        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            int permutedIndex = indexWithPermutation(ordinalInCandidate);
            final var normalizedKeyExpression = normalizedKeyExpressions.get(permutedIndex);

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

            // Grab the value for this sortParameterID from the selectHaving result columns
            final var value = deconstructedValue.get(permutedIndex).rebase(aliasMap);

            if (normalizedValues.add(value)) {
                builder.add(
                        MatchedOrderingPart.of(parameterId, value, comparisonRange,
                                MatchedSortOrder.ASCENDING));
            }
        }

        return builder.build();
    }

    private int indexWithPermutation(int i) {
        if (isPermuted()) {
            int permutedCount = getPermutedCount();
            final GroupingKeyExpression groupingKeyExpression = (GroupingKeyExpression)index.getRootExpression();
            int groupingCount = groupingKeyExpression.getGroupingCount();
            if ( i < groupingCount - permutedCount) {
                return i;
            } else if (i >= groupingCount - permutedCount && i < groupingCount - permutedCount + groupingKeyExpression.getGroupedCount()) {
                return i + permutedCount;
            } else {
                return i - permutedCount;
            }
        } else {
            return i;
        }
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        final var groupingKey = ((GroupingKeyExpression)index.getRootExpression()).getGroupingSubKey();

        if (groupingKey instanceof EmptyKeyExpression) {
            // TODO this should be something like anything-order.
            return Ordering.empty();
        }

        final List<Value> deconstructedValue = Values.deconstructRecord(selectHavingResultValue);
        final AliasMap aliasMap = AliasMap.ofAliases(Iterables.getOnlyElement(selectHavingResultValue.getCorrelatedTo()), Quantifier.current());

        // TODO include the aggregate Value itself in the ordering.
        final var normalizedKeyExpressions = groupingKey.normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

        // We keep a set for normalized values in order to check for duplicate values in the index definition.
        // We correct here for the case where an index is defined over {a, a} since its order is still just {a}.
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        for (var i = 0; i < equalityComparisons.size(); i++) {
            int permutedIndex = indexWithPermutation(i);
            if (permutedIndex < normalizedKeyExpressions.size()) {
                final var normalizedKeyExpression = normalizedKeyExpressions.get(permutedIndex);

                if (normalizedKeyExpression.createsDuplicates()) {
                    continue;
                }
            }

            final var comparison = equalityComparisons.get(i);
            final var value = deconstructedValue.get(permutedIndex).rebase(aliasMap);
            bindingMapBuilder.put(value, Binding.fixed(comparison));
            normalizedValues.add(value);
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (var i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            int permutedIndex = indexWithPermutation(i);
            if (permutedIndex < normalizedKeyExpressions.size()) {
                final var normalizedKeyExpression = normalizedKeyExpressions.get(permutedIndex);

                if (normalizedKeyExpression.createsDuplicates()) {
                    break;
                }
            }

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            final var normalizedValue = deconstructedValue.get(permutedIndex).rebase(aliasMap);

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
        final var baseRecordType = Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(recordTypes));

        // reset indexes of all fields, such that we can normalize them
        // TODO This is incorrect. Either the type indicates the field indexes or it does not. It is the truth here.
        //      Why do we need to remove the field indexes here? They should not be set for most trivial cases anyway.
        final var resultType = groupByResultValue.getResultType();
        final var messageBuilder = TypeRepository.newBuilder().addTypeIfNeeded(resultType).build().newMessageBuilder(resultType);
        final var messageDescriptor = Objects.requireNonNull(messageBuilder).getDescriptorForType();
        final var constraintMaybe = partialMatch.getMatchInfo().getConstraint();

        final var indexEntryConverter = createIndexEntryConverter(messageDescriptor);
        final var aggregateIndexScan = new RecordQueryIndexPlan(index.getName(),
                null,
                new IndexScanComparisons(IndexScanType.BY_GROUP, toScanComparisons(comparisonRanges)),
                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                reverseScanOrder,
                false,
                partialMatch.getMatchCandidate(),
                baseRecordType,
                QueryPlanConstraint.tautology());

        return new RecordQueryAggregateIndexPlan(aggregateIndexScan,
                recordTypes.get(0).getName(),
                indexEntryConverter,
                groupByResultValue,
                constraintMaybe);
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    private IndexKeyValueToPartialRecord createIndexEntryConverter(final Descriptors.Descriptor messageDescriptor) {
        final var selectHavingFields = Values.deconstructRecord(selectHavingResultValue);
        final var groupingCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
        Verify.verify(selectHavingFields.size() >= groupingCount);

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(messageDescriptor);
        if (isPermuted()) {
            addFieldsForPermutedIndexEntry(selectHavingFields, groupingCount, builder);
        } else {
            addFieldsForNonPermutedIndexEntry(selectHavingFields, groupingCount, builder);
        }

        if (!builder.isValid()) {
            throw new RecordCoreException("could not generate a covering index scan operator for index; Invalid mapping between index entries to partial record").addLogInfo(LogMessageKeys.INDEX_NAME, index.getName());
        }
        return builder.build();
    }

    private void addFieldsForPermutedIndexEntry(@Nonnull List<Value> selectHavingFields, int groupingCount, @Nonnull IndexKeyValueToPartialRecord.Builder builder) {
        // The selectHavingFields come in an order matching the original columns in the key expression. That is, if there are n grouping columns and m aggregate
        // columns, we have fields like:
        //
        // select-having value structure: (groupingCol1, groupingCol2, ... groupingColn, agg(coln+1), ..., agg(coln+m))
        //
        // But the actual index transposes the aggregate values with the last permutedCount columns, so the actual key structure is:
        //
        // key structure                : KEY(groupingCol1, groupingCol2, ... groupingColn-permuted, agg(coln+1), ..., agg(coln+m), groupingColn-permuted+1, ..., groupingColn) VALUE()
        //
        // This function then needs to take the index from the first list and find its corresponding spot. That means that:
        //
        //  1. The first (groupingCount - permutedCount) columns preserve their position
        //  2. The final permutedCount grouping columns need to be shifted over by the number of aggregate columns (typically 1)
        //  3. The aggregate columns need to be shifted over permutedCount columns
        //
        int permutedCount = getPermutedCount();
        int groupedCount = getColumnSize() - groupingCount;

        for (int i = 0; i < selectHavingFields.size(); i++) {
            final Value keyValue = selectHavingFields.get(i);
            if (keyValue instanceof FieldValue) {
                int havingIndex;
                if (i >= groupingCount - permutedCount && i < groupingCount) {
                    // Grouping column after the permuted aggregate. Adjust by skipping over the grouped aggregate columns
                    havingIndex = i + groupedCount;
                } else if (i >= groupingCount) {
                    // Aggregate column. Adjust by removing the permuted columns
                    havingIndex = i - permutedCount;
                } else {
                    // Grouping column before the permuted aggregate. Preserve original index
                    havingIndex = i;
                }
                final AvailableFields.FieldData fieldData = AvailableFields.FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(havingIndex));
                addCoveringField(builder, (FieldValue)keyValue, fieldData);
            }
        }
    }

    private void addFieldsForNonPermutedIndexEntry(@Nonnull List<Value> selectHavingFields, int groupingCount, @Nonnull IndexKeyValueToPartialRecord.Builder builder) {
        // key structure                : KEY(groupingCol1, groupingCol2, ... groupingColn), VALUE(agg(coln+1))
        // groupingCount                : n+1
        // select-having value structure: (groupingCol1, groupingCol2, ... groupingColn, agg(coln+1))

        for (int i = 0; i < groupingCount; i++) {
            final Value keyValue = selectHavingFields.get(i);
            if (keyValue instanceof FieldValue) {
                final AvailableFields.FieldData fieldData = AvailableFields.FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(i));
                addCoveringField(builder, (FieldValue)keyValue, fieldData);
            }
        }
        for (int i = groupingCount; i < selectHavingFields.size(); i++) {
            final Value keyValue = selectHavingFields.get(i);
            if (keyValue instanceof FieldValue) {
                final AvailableFields.FieldData fieldData = AvailableFields.FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.VALUE, ImmutableIntArray.of(i - groupingCount));
                addCoveringField(builder, (FieldValue)keyValue, fieldData);
            }
        }
    }

    private static void addCoveringField(@Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                         @Nonnull FieldValue fieldValue,
                                         @Nonnull AvailableFields.FieldData fieldData) {
        // TODO field names are for debugging purposes only, we should probably use field ordinals here instead.
        final var simplifiedFieldValue = (FieldValue)fieldValue.simplify(AliasMap.emptyMap(), ImmutableSet.of());
        for (final var maybeFieldName : simplifiedFieldValue.getFieldPrefix().getOptionalFieldNames()) {
            Verify.verify(maybeFieldName.isPresent());
            builder = builder.getFieldBuilder(maybeFieldName.get());
        }

        // TODO not sure what to do with the null standing requirement

        final var maybeFieldName = simplifiedFieldValue.getLastFieldName();
        Verify.verify(maybeFieldName.isPresent());
        final String fieldName = maybeFieldName.get();
        if (!builder.hasField(fieldName)) {
            builder.addField(fieldName, fieldData.getSource(),
                    fieldData.getCopyIfPredicate(), fieldData.getOrdinalPath(), fieldData.getInvertibleFunction());
        }
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        final ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public Type getBaseType() {
        return baseType;
    }
}
