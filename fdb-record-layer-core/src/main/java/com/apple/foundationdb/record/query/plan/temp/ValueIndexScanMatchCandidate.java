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
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class ValueIndexScanMatchCandidate implements ScanWithFetchMatchCandidate {
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
    private final QuantifiedValue recordValue;

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
                                        @Nonnull final QuantifiedValue recordValue,
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
    public List<CorrelationIdentifier> getParameters() {
        return parameters;
    }

    @Nonnull
    public QuantifiedValue getRecordValue() {
        return recordValue;
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
    public RelationalExpression toEquivalentExpression(@Nonnull final PartialMatch partialMatch,
                                                       @Nonnull final List<ComparisonRange> comparisonRanges,
                                                       final boolean isReverse) {
        return tryFetchCoveringIndexScan(partialMatch, comparisonRanges, isReverse)
                .orElseGet(() ->
                        new RecordQueryIndexPlan(index.getName(),
                                IndexScanType.BY_VALUE,
                                toScanComparisons(comparisonRanges),
                                isReverse,
                                false,
                                (ValueIndexScanMatchCandidate)partialMatch.getMatchCandidate()));
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
                        isReverse,
                        false,
                        (ValueIndexScanMatchCandidate)partialMatch.getMatchCandidate());

        final RecordQueryCoveringIndexPlan coveringIndexPlan = new RecordQueryCoveringIndexPlan(indexPlan,
                recordType.getName(),
                AvailableFields.NO_FIELDS, // not used except for old planner properties
                builder.build());

        return Optional.of(new RecordQueryFetchFromPartialRecordPlan(coveringIndexPlan, coveringIndexPlan::pushValueThroughFetch));
    }

    @Nonnull
    @Override
    public Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                                 @Nonnull QuantifiedColumnValue indexRecordColumnValue) {

        final Set<Value> quantifiedColumnValues = ImmutableSet.copyOf(value.filter(v -> v instanceof QuantifiedColumnValue));

        // if this is a value that is referring to more than one value from its quantifier or two multiple quantifiers
        if (quantifiedColumnValues.size() != 1) {
            return Optional.empty();
        }

        final QuantifiedColumnValue quantifiedColumnValue = (QuantifiedColumnValue)Iterables.getOnlyElement(quantifiedColumnValues);

        // replace the quantified column value inside the given value with the quantified value in the match candidate
        final Optional<Value> translatedValueOptional =
                value.translate(ImmutableMap.of(quantifiedColumnValue, QuantifiedColumnValue.of(recordValue.getAlias(), 0)));
        if (!translatedValueOptional.isPresent()) {
            return Optional.empty();
        }
        final Value translatedValue = translatedValueOptional.get();
        final AliasMap equivalenceMap = AliasMap.identitiesFor(ImmutableSet.of(recordValue.getAlias()));

        for (final Value matchResultValue : Iterables.concat(ImmutableList.of(recordValue), indexKeyValues, indexValueValues)) {
            final Set<CorrelationIdentifier> resultValueCorrelatedTo = matchResultValue.getCorrelatedTo();
            if (resultValueCorrelatedTo.size() != 1) {
                continue;
            }
            if (translatedValue.semanticEquals(matchResultValue, equivalenceMap)) {
                return matchResultValue.translate(ImmutableMap.of(QuantifiedColumnValue.of(recordValue.getAlias(), 0), indexRecordColumnValue));
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
}
