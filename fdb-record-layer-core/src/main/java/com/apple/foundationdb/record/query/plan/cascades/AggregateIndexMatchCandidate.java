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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
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
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Case class that represents a grouping index with aggregate function(s).
 */
public class AggregateIndexMatchCandidate implements MatchCandidate, WithBaseQuantifierMatchCandidate {

    // The backing index metadata structure.
    @Nonnull
    private final Index index;

    // The expression representation of the match candidate.
    @Nonnull
    private final ExpressionRefTraversal traversal;

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
                                        @Nonnull final ExpressionRefTraversal traversal,
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
    public ExpressionRefTraversal getTraversal() {
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

    @Nonnull
    @Override
    public List<MatchedOrderingPart> computeMatchedOrderingParts(@Nonnull final MatchInfo matchInfo, @Nonnull final List<CorrelationIdentifier> sortParameterIds, final boolean isReverse) {
        final var parameterBindingMap = matchInfo.getParameterBindingMap();

        final var normalizedKeys =
                getFullKeyExpression().normalizeKeyForPositions();

        final var builder = ImmutableList.<MatchedOrderingPart>builder();
        final var candidateParameterIds = getOrderingAliases();

        for (final var parameterId : sortParameterIds) {
            final var ordinalInCandidate = candidateParameterIds.indexOf(parameterId);
            Verify.verify(ordinalInCandidate >= 0);
            final var normalizedKeyExpression = normalizedKeys.get(ordinalInCandidate);

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
                            baseType);

            builder.add(
                    MatchedOrderingPart.of(value,
                            comparisonRange == null ? ComparisonRange.Type.EMPTY : comparisonRange.getRangeType(),
                            isReverse));
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        final var equalityBoundValueMapBuilder = ImmutableSetMultimap.<Value, Comparisons.Comparison>builder();
        final var groupingKey = ((GroupingKeyExpression)index.getRootExpression()).getGroupingSubKey();

        if (groupingKey instanceof EmptyKeyExpression) {
            // TODO this should be something like anything-order.
            return Ordering.emptyOrder();
        }

        // TODO include the aggregate Value itself in the ordering.
        final var normalizedKeyExpressions = groupingKey.normalizeKeyForPositions();
        final var equalityComparisons = scanComparisons.getEqualityComparisons();

        for (var i = 0; i < equalityComparisons.size(); i++) {
            final var normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final var comparison = equalityComparisons.get(i);

            if (normalizedKeyExpression.createsDuplicates()) {
                continue;
            }

            final var normalizedValue =
                    new ScalarTranslationVisitor(normalizedKeyExpression).toResultValue(Quantifier.current(),
                            baseType);
            equalityBoundValueMapBuilder.put(normalizedValue, comparison);
        }

        final var result = ImmutableList.<OrderingPart>builder();
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
                            baseType);

            result.add(OrderingPart.of(normalizedValue, isReverse));
        }

        return new Ordering(equalityBoundValueMapBuilder.build(), result.build(), isDistinct);
    }

    @Nonnull
    @Override
    public RecordQueryPlan toEquivalentPlan(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final PlanContext planContext,
                                            @Nonnull final Memoizer memoizer,
                                            @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of scan"));

        final var baseRecordType = Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(recordTypes));

        // reset indexes of all fields, such that we can normalize them
        final var type = reset(groupByResultValue.getResultType());
        final var messageBuilder = TypeRepository.newBuilder().addTypeIfNeeded(type).build().newMessageBuilder(type);
        final var messageDescriptor = Objects.requireNonNull(messageBuilder).getDescriptorForType();
        final var constraintMaybe = partialMatch.getMatchInfo().getConstraintMaybe();

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
                messageDescriptor,
                groupByResultValue,
                constraintMaybe.orElse(QueryPlanConstraint.tautology()));
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return recordTypes;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    private IndexKeyValueToPartialRecord createIndexEntryConverter(final Descriptors.Descriptor messageDescriptor) {
        final var selectHavingFields = Values.deconstructRecord(selectHavingResultValue);
        final var groupingCount = ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount();
        Verify.verify(selectHavingFields.size() >= groupingCount);

        // key structure                : KEY(groupingCol1, groupingCol2, ... groupingColn), VALUE(agg(coln+1))
        // groupingCount                : n+1
        // select-having value structure: (groupingCol1, groupingCol2, ... groupingColn, agg(coln+1))

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(messageDescriptor);
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

        if (!builder.isValid()) {
            throw new RecordCoreException(String.format("could not generate a covering index scan operator for '%s'; Invalid mapping between index entries to partial record", index.getName()));
        }
        return builder.build();
    }

    @Nonnull
    private static Type reset(@Nonnull final Type type) {
        if (type instanceof Type.Record) {
            return Type.Record.fromFields(((Type.Record)type).getFields().stream().map(f -> Type.Record.Field.of(
                            reset(f.getFieldType()),
                            f.getFieldNameOptional(),
                            Optional.empty())).collect(Collectors.toList()));
        }
        return type;
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
            builder.addField(fieldName, fieldData.getSource(), fieldData.getCopyIfPredicate(), fieldData.getOrdinalPath());
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
