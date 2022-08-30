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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class ValueIndexScanMatchCandidate implements ScanWithFetchMatchCandidate, ValueIndexLikeMatchCandidate {
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
     * Holds the parameter names for all necessary parameters that need to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> parameters;

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
     * List of values that represent the key parts of the index represented by the candidate in the expanded graph.
     */
    @Nonnull
    private final List<Value> indexKeyValues;

    /**
     * List of values that represent the value parts of the index represented by the candidate in the expanded graph.
     */
    @Nonnull
    private final List<Value> indexValueValues;

    /**
     * Traversal object of the expanded index scan graph.
     */
    @Nonnull
    private final ExpressionRefTraversal traversal;

    @Nonnull
    private final KeyExpression fullKeyExpression;

    @Nullable
    private final KeyExpression primaryKey;

    @Nonnull
    private final Supplier<Optional<List<Value>>> primaryKeyValuesSupplier;

    public ValueIndexScanMatchCandidate(@Nonnull Index index,
                                        @Nonnull Collection<RecordType> queriedRecordTypes,
                                        @Nonnull final ExpressionRefTraversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> parameters,
                                        @Nonnull final Type baseType,
                                        @Nonnull final CorrelationIdentifier baseAlias,
                                        @Nonnull final List<Value> indexKeyValues,
                                        @Nonnull final List<Value> indexValueValues,
                                        @Nonnull final KeyExpression fullKeyExpression,
                                        @Nullable final KeyExpression primaryKey) {
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.baseType = baseType;
        this.baseAlias = baseAlias;
        this.indexKeyValues = ImmutableList.copyOf(indexKeyValues);
        this.indexValueValues = ImmutableList.copyOf(indexValueValues);
        this.fullKeyExpression = fullKeyExpression;
        this.primaryKey = primaryKey;
        this.primaryKeyValuesSupplier = Suppliers.memoize(() -> MatchCandidate.computePrimaryKeyValuesMaybe(primaryKey, baseType));
    }

    @Nonnull
    @Override
    public Index getIndex() {
        return index;
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
    public ExpressionRefTraversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return parameters;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return getSargableAliases();
    }

    @Nonnull
    public Type getBaseType() {
        return baseType;
    }

    @Nonnull
    public List<Value> getIndexKeyValues() {
        return indexKeyValues;
    }

    @Nonnull
    public List<Value> getIndexValueValues() {
        return indexValueValues;
    }

    @Nonnull
    @Override
    public KeyExpression getFullKeyExpression() {
        return fullKeyExpression;
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
    public RelationalExpression toEquivalentExpression(@Nonnull final PartialMatch partialMatch,
                                                       @Nonnull final PlanContext planContext,
                                                       @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of scan"));

        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes));

        return tryFetchCoveringIndexScan(partialMatch, planContext, comparisonRanges, reverseScanOrder, baseRecordType)
                .orElseGet(() ->
                        new RecordQueryIndexPlan(index.getName(),
                                primaryKey,
                                IndexScanComparisons.byValue(toScanComparisons(comparisonRanges)),
                                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                                reverseScanOrder,
                                false,
                                (ValueIndexScanMatchCandidate)partialMatch.getMatchCandidate(),
                                baseRecordType));
    }

    @Nonnull
    private Optional<RelationalExpression> tryFetchCoveringIndexScan(@Nonnull final PartialMatch partialMatch,
                                                                     @Nonnull final PlanContext planContext,
                                                                     @Nonnull final List<ComparisonRange> comparisonRanges,
                                                                     final boolean isReverse,
                                                                     @Nonnull Type.Record baseRecordType) {
        if (queriedRecordTypes.size() > 1) {
            return Optional.empty();
        }

        final RecordType recordType = Iterables.getOnlyElement(queriedRecordTypes);
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        final Value baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);
        for (int i = 0; i < indexKeyValues.size(); i++) {
            final Value keyValue = indexKeyValues.get(i);
            if (keyValue instanceof FieldValue && keyValue.isFunctionallyDependentOn(baseObjectValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, i);
                if (!addCoveringField(builder, (FieldValue)keyValue, fieldData)) {
                    return Optional.empty();
                }
            }
        }

        for (int i = 0; i < indexValueValues.size(); i++) {
            final Value valueValue = indexValueValues.get(i);
            if (valueValue instanceof FieldValue && valueValue.isFunctionallyDependentOn(baseObjectValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.VALUE, i);
                if (!addCoveringField(builder, (FieldValue)valueValue, fieldData)) {
                    return Optional.empty();
                }
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        final IndexScanParameters scanParameters = IndexScanComparisons.byValue(toScanComparisons(comparisonRanges));
        final RecordQueryPlanWithIndex indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        primaryKey,
                        scanParameters,
                        planContext.getPlannerConfiguration().getIndexFetchMethod(),
                        isReverse,
                        false,
                        (ValueIndexScanMatchCandidate)partialMatch.getMatchCandidate(),
                        baseRecordType);

        final RecordQueryCoveringIndexPlan coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                recordType.getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                builder.build());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(coveringIndexPlan, coveringIndexPlan::pushValueThroughFetch, baseRecordType));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                                 @Nonnull CorrelationIdentifier sourceAlias,
                                                 @Nonnull CorrelationIdentifier targetAlias) {
        final Value baseObjectValue = QuantifiedObjectValue.of(baseAlias, baseType);

        // replace the quantified column value inside the given value with the quantified value in the match candidate
        final Value translatedValue =
                value.rebase(AliasMap.of(sourceAlias, baseAlias));
        final AliasMap equivalenceMap = AliasMap.identitiesFor(ImmutableSet.of(baseAlias));

        for (final Value matchResultValue : Iterables.concat(ImmutableList.of(baseObjectValue), indexKeyValues, indexValueValues)) {
            final Set<CorrelationIdentifier> resultValueCorrelatedTo = matchResultValue.getCorrelatedTo();
            if (resultValueCorrelatedTo.size() != 1) {
                continue;
            }
            if (translatedValue.semanticEquals(matchResultValue, equivalenceMap)) {
                return Optional.of(matchResultValue.rebase(AliasMap.of(baseAlias, targetAlias)));
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

    private static boolean addCoveringField(@Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                            @Nonnull FieldValue fieldValue,
                                            @Nonnull AvailableFields.FieldData fieldData) {
        for (final Type.Record.Field field : fieldValue.getFieldPrefix()) {
            if (field.getFieldNameOptional().isEmpty()) {
                return false;
            }
            builder = builder.getFieldBuilder(field.getFieldName());
        }

        // TODO not sure what to do with the null standing requirement

        final Type.Record.Field field = fieldValue.getLastField();
        if (field.getFieldNameOptional().isEmpty()) {
            return false;
        }
        final String fieldName = field.getFieldName();
        if (!builder.hasField(fieldName)) {
            builder.addField(fieldName, fieldData.getSource(), fieldData.getIndex());
        }
        return true;
    }
}
