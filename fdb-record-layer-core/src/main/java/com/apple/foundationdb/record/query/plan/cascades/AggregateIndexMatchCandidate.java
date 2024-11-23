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
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueComputationRuleSet;
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
    private final SelectExpression selectHavingExpression;

    /**
     * Creates a new instance of {@link AggregateIndexMatchCandidate}.
     *
     * @param index The underlying index.
     * @param traversal The expression representation of the match candidate.
     * @param sargableAndOrderAliases A list of sargable and order aliases.
     * @param recordTypes The underlying base record types.
     * @param baseType The base type.
     * @param groupByResultValue The group by expression result value.
     * @param selectHavingExpression The select-having expression.
     */
    public AggregateIndexMatchCandidate(@Nonnull final Index index,
                                        @Nonnull final Traversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> sargableAndOrderAliases,
                                        @Nonnull final Collection<RecordType> recordTypes,
                                        @Nonnull final Type baseType,
                                        @Nonnull final Value groupByResultValue,
                                        @Nonnull final SelectExpression selectHavingExpression) {
        Preconditions.checkArgument(!recordTypes.isEmpty());
        this.index = index;
        this.traversal = traversal;
        this.sargableAndOrderAliases = sargableAndOrderAliases;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
        this.baseType = baseType;
        this.groupByResultValue = groupByResultValue;
        this.selectHavingExpression = selectHavingExpression;
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
        final var parameterBindingMap =
                matchInfo.getRegularMatchInfo().getParameterBindingMap();
        final var normalizedKeyExpressions =
                getFullKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<MatchedOrderingPart>builder();
        final var candidateParameterIds = getOrderingAliases();
        final var normalizedValues = Sets.newHashSetWithExpectedSize(normalizedKeyExpressions.size());

        final var selectHavingResultValue = selectHavingExpression.getResultValue();
        final var deconstructedValue = Values.deconstructRecord(selectHavingResultValue);
        final var aliasMap = AliasMap.ofAliases(Iterables.getOnlyElement(selectHavingResultValue.getCorrelatedTo()), Quantifier.current());

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
                final var matchedOrderingPart =
                        value.<MatchedSortOrder, MatchedOrderingPart>deriveOrderingPart(AliasMap.emptyMap(), ImmutableSet.of(),
                                (v, sortOrder) -> MatchedOrderingPart.of(parameterId, v, comparisonRange, sortOrder),
                                OrderingValueComputationRuleSet.usingMatchedOrderingParts());
                builder.add(matchedOrderingPart);
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
        final int groupingCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();

        if (!isPermuted() && groupingCount == 0) {
            // TODO this should be something like anything-order.
            return Ordering.empty();
        }

        final var selectHavingResultValue = selectHavingExpression.getResultValue();
        final var selectHavingResultType = (Type.Record)selectHavingResultValue.getResultType();

        // Massaging the right type for the base object value
        var baseObjectAlias =
                Iterables.getOnlyElement(selectHavingExpression.getQuantifiers()).getAlias();
        var baseObjectValue = QuantifiedObjectValue.of(baseObjectAlias,
                selectHavingResultType);
        final var deconstructedValuesBuilder = ImmutableList.<Value>builder();
        for (final var field : selectHavingResultType.getFields()) {
            deconstructedValuesBuilder.add(FieldValue.ofFieldName(baseObjectValue, field.getFieldName()));
        }
        final var deconstructedValues = deconstructedValuesBuilder.build();

        //final var deconstructedValue = Values.deconstructRecord(selectHavingResultValue);
        final var aliasMap =
                AliasMap.ofAliases(Iterables.getOnlyElement(selectHavingResultValue.getCorrelatedTo()),
                        Quantifier.current());

        // Note: the aggregate Value itself only influences the ordering if the index type is permuted, as it otherwise
        // will appear in the value of the FDB key-value pair and thus does not participate in ordering.
        final var orderingColumnCount = isPermuted() ? deconstructedValues.size() : groupingCount;
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

        // We keep a set for normalized values in order to check for duplicate values in the index definition.
        // We correct here for the case where an index is defined over {a, a} since its order is still just {a}.
        final var seenValues = Sets.newHashSetWithExpectedSize(orderingColumnCount);

        for (var i = 0; i < equalityComparisons.size(); i++) {
            int permutedIndex = indexWithPermutation(i);
            final var comparison = equalityComparisons.get(i);
            final var value = deconstructedValues.get(permutedIndex).rebase(aliasMap);

            final var simplifiedComparisonPairOptional =
                    MatchCandidate.simplifyComparisonMaybe(value, comparison);
            if (simplifiedComparisonPairOptional.isEmpty()) {
                continue;
            }
            final var simplifiedComparisonPair = simplifiedComparisonPairOptional.get();
            bindingMapBuilder.put(simplifiedComparisonPair.getLeft(), Binding.fixed(simplifiedComparisonPair.getRight()));
            seenValues.add(simplifiedComparisonPair.getLeft());
        }

        final var orderingSequenceBuilder = ImmutableList.<Value>builder();
        for (var i = scanComparisons.getEqualitySize(); i < orderingColumnCount; i++) {
            int permutedIndex = indexWithPermutation(i);

            //
            // Note that it is not really important here if the keyExpression can be normalized in a lossless way
            // or not. A key expression containing repeated fields is sort-compatible with its normalized key
            // expression. We used to refuse to compute the sort order in the presence of repeats, however,
            // I think that restriction can be relaxed.
            //
            final var normalizedValue = deconstructedValues.get(permutedIndex).rebase(aliasMap);

            final var providedOrderingPart =
                    normalizedValue.deriveOrderingPart(AliasMap.emptyMap(), ImmutableSet.of(),
                            OrderingPart.ProvidedOrderingPart::new,
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
        final var baseRecordType = Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(recordTypes));

        final var selectHavingResultValue = selectHavingExpression.getResultValue();
        final var resultType = selectHavingResultValue.getResultType();
        final var messageDescriptor =
                Objects.requireNonNull(TypeRepository.newBuilder()
                        .addTypeIfNeeded(resultType)
                        .build()
                        .getMessageDescriptor(resultType));
        final var constraintMaybe = partialMatch.getRegularMatchInfo().getConstraint();

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
                selectHavingResultValue,
                groupByResultValue,
                constraintMaybe);
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return recordTypes;
    }

    protected int getGroupingCount() {
        final int keyExpressionGroupingCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
        return IndexTypes.BITMAP_VALUE.equals(index.getType())
               ? keyExpressionGroupingCount + 1
               : keyExpressionGroupingCount;
    }

    /**
     * Creates a new {@link IndexKeyValueToPartialRecord} to facilitate the correct copying of information from the
     * index-tuple structure to a partial record (which in this case is dynamically-typed).
     * @param messageDescriptor message descriptor for the select having result {@link Value}
     *        TODO This message descriptor is not actually needed as the logic it is used for might as well just use
     *             the type directly. The problem is that {@link IndexKeyValueToPartialRecord} is shared between the
     *             heuristic and the cascades planner and the heuristic planner does not maintain a type system. So
     *             in the heuristic planner, this descriptor is always a descriptor of a record type or synthetic
     *             type, while here we use it for a dynamically derived type. We should maybe use separate structures
     *             for the respective planners.
     * @return a new {@link IndexKeyValueToPartialRecord}
     */
    @Nonnull
    private IndexKeyValueToPartialRecord createIndexEntryConverter(@Nonnull final Descriptors.Descriptor messageDescriptor) {
        final var selectHavingResultValue = selectHavingExpression.getResultValue();
        final var selectHavingResultType = (Type.Record)selectHavingResultValue.getResultType();
        final var groupingCount = getGroupingCount();
        Verify.verify(selectHavingResultType.getFields().size() >= groupingCount);

        // Massaging the right type for the base object value
        var baseObjectAlias =
                Iterables.getOnlyElement(selectHavingExpression.getQuantifiers()).getAlias();
        var baseObjectValue = QuantifiedObjectValue.of(baseObjectAlias, selectHavingResultType);

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(messageDescriptor);
        if (isPermuted()) {
            addFieldsForPermutedIndexEntry(selectHavingResultType, baseObjectValue, groupingCount, builder);
        } else {
            addFieldsForNonPermutedIndexEntry(selectHavingResultType, baseObjectValue, groupingCount, builder);
        }

        if (!builder.isValid()) {
            throw new RecordCoreException("could not generate a covering index scan operator for index; Invalid mapping between index entries to partial record").addLogInfo(LogMessageKeys.INDEX_NAME, index.getName());
        }
        return builder.build();
    }

    private void addFieldsForPermutedIndexEntry(@Nonnull final Type.Record selectHavingResultType,
                                                @Nonnull final Value baseObjectValue,
                                                int groupingCount,
                                                @Nonnull IndexKeyValueToPartialRecord.Builder builder) {
        //
        // The selectHavingFields come in an order matching the original columns in the key expression.
        // That is, if there are n grouping columns and m aggregate columns, we have fields like:
        //
        // select-having value structure: (groupingCol1, groupingCol2, ... groupingColn, agg(coln+1), ..., agg(coln+m))
        //
        // But the actual index transposes the aggregate values with the last permutedCount columns,
        // so the actual key structure is:
        //
        // key structure : KEY(groupingCol1, groupingCol2, ... groupingColn-permuted, agg(coln+1), ...,
        //                     agg(coln+m), groupingColn-permuted+1, ..., groupingColn) VALUE()
        //
        // This function then needs to take the index from the first list and find its corresponding spot. That means
        // that:
        //
        //  1. The first (groupingCount - permutedCount) columns preserve their position
        //  2. The final permutedCount grouping columns need to be shifted over by the number of aggregate columns
        //     (typically 1)
        //  3. The aggregate columns need to be shifted over permutedCount columns
        //

        final var selectHavingResultFields = selectHavingResultType.getFields();

        int permutedCount = getPermutedCount();
        int groupedCount = getColumnSize() - groupingCount;

        for (int i = 0; i < selectHavingResultFields.size(); i++) {
            final var field = selectHavingResultFields.get(i);

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
            addCoveringField(builder, field, baseObjectValue, IndexKeyValueToPartialRecord.TupleSource.KEY, havingIndex);
        }
    }

    private void addFieldsForNonPermutedIndexEntry(@Nonnull final Type.Record selectHavingResultType,
                                                   @Nonnull final Value baseObjectValue,
                                                   final int groupingCount,
                                                   @Nonnull final IndexKeyValueToPartialRecord.Builder builder) {
        //
        // key structure : KEY(groupingCol1, groupingCol2, ... groupingColn), VALUE(agg(coln+1))
        // groupingCount : n+1
        // select-having value structure: (groupingCol1, groupingCol2, ... groupingColn, agg(coln+1))
        //

        final var selectHavingResultFields = selectHavingResultType.getFields();

        for (int i = 0; i < groupingCount; i++) {
            final var field = selectHavingResultFields.get(i);
            addCoveringField(builder, field, baseObjectValue, IndexKeyValueToPartialRecord.TupleSource.KEY, i);
        }
        for (int i = groupingCount; i < selectHavingResultFields.size(); i++) {
            final var field = selectHavingResultFields.get(i);
            addCoveringField(builder, field, baseObjectValue, IndexKeyValueToPartialRecord.TupleSource.VALUE,
                    i - groupingCount);
        }
    }

    private static void addCoveringField(@Nonnull final IndexKeyValueToPartialRecord.Builder builder,
                                         @Nonnull final Type.Record.Field field,
                                         @Nonnull final Value baseObjectValue,
                                         @Nonnull final IndexKeyValueToPartialRecord.TupleSource tupleSource,
                                         final int index) {
        final var fieldName = field.getFieldName();
        final var fieldValue = FieldValue.ofFieldName(baseObjectValue, fieldName);

        final var extractFromIndexEntryPairOptional =
                fieldValue.extractFromIndexEntryMaybe(baseObjectValue, AliasMap.emptyMap(), ImmutableSet.of(),
                        tupleSource, ImmutableIntArray.of(index));

        Verify.verify(extractFromIndexEntryPairOptional.isPresent());
        final var extractFromIndexEntryPair = extractFromIndexEntryPairOptional.get();
        final var extractValue = extractFromIndexEntryPair.getRight();

        if (!builder.hasField(fieldName)) {
            builder.addField(fieldName, extractValue);
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
