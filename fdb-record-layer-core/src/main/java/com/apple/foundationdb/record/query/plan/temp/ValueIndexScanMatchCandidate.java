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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class ValueIndexScanMatchCandidate implements MatchCandidate {
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
     * Holds the parameter names for all necessary parameters that need to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    /**
     * Value that flows the actual record.
     */
    @Nonnull
    private final Value recordValue;

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
    private final KeyExpression alternativeKeyExpression;

    public ValueIndexScanMatchCandidate(@Nonnull Index index,
                                        @Nonnull Collection<RecordType> recordTypes,
                                        @Nonnull final ExpressionRefTraversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> parameters,
                                        @Nonnull final Value recordValue,
                                        @Nonnull final List<Value> indexKeyValues,
                                        @Nonnull final List<Value> indexValueValues,
                                        @Nonnull final KeyExpression alternativeKeyExpression) {
        this.index = index;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.recordValue = recordValue;
        this.indexKeyValues = ImmutableList.copyOf(indexKeyValues);
        this.indexValueValues = ImmutableList.copyOf(indexValueValues);
        this.alternativeKeyExpression = alternativeKeyExpression;
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
    public List<CorrelationIdentifier> getParameters() {
        return parameters;
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
    public KeyExpression getAlternativeKeyExpression() {
        return alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public RelationalExpression toScanExpression(@Nonnull final List<ComparisonRange> comparisonRanges, final boolean isReverse) {
        return tryFetchCoveringIndexScan(comparisonRanges, isReverse)
                .orElseGet(() ->
                        new IndexScanExpression(getName(),
                                IndexScanType.BY_VALUE,
                                comparisonRanges,
                                isReverse));
    }

    @Nonnull
    private Optional<RelationalExpression> tryFetchCoveringIndexScan(@Nonnull final List<ComparisonRange> comparisonRanges, final boolean isReverse) {
        if (recordTypes.size() > 1) {
            return Optional.empty();
        }

        final RecordType recordType = Iterables.getOnlyElement(recordTypes);
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        for (int i = 0; i < indexKeyValues.size(); i++) {
            final Value keyValue = indexKeyValues.get(i);
            if (keyValue instanceof FieldValue && keyValue.isFunctionallyDependentOn(recordValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, i);
                addCoveringField(builder, (FieldValue)keyValue, fieldData);
            }
        }

        for (int i = 0; i < indexValueValues.size(); i++) {
            final Value valueValue = indexValueValues.get(i);
            if (valueValue instanceof FieldValue && valueValue.isFunctionallyDependentOn(recordValue)) {
                final AvailableFields.FieldData fieldData =
                        AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.VALUE, i);
                addCoveringField(builder, (FieldValue)valueValue, fieldData);
            }
        }

        if (!builder.isValid()) {
            return Optional.empty();
        }

        final RecordQueryPlanWithIndex indexPlan =
                new RecordQueryIndexPlan(index.getName(),
                        IndexScanType.BY_VALUE,
                        toScanComparisons(comparisonRanges),
                        isReverse);

        final RecordQueryCoveringIndexPlan coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                recordType.getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                builder.build());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(coveringIndexPlan));
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

        builder.addField(fieldValue.getFieldName(), fieldData.getSource(), fieldData.getIndex());
    }
}
