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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

/**
 * Case class that represents a grouping index with an aggregate function(s).
 */
public class AggregateIndexMatchCandidate implements MatchCandidate {

    // The backing index metadata structure.
    @Nonnull
    private final Index index;

    @Nonnull
    private final ExpressionRefTraversal traversal;

    @Nonnull
    private final List<CorrelationIdentifier> sargableAndOrderAliases;

    @Nonnull
    private List<RecordType> recordTypes;

    public AggregateIndexMatchCandidate(@Nonnull final Index index,
                                        @Nonnull final ExpressionRefTraversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> sargableAndOrderAliases, final Collection<RecordType> recordTypes) {
        this.index = index;
        this.traversal = traversal;
        this.sargableAndOrderAliases = sargableAndOrderAliases;
        this.recordTypes =  ImmutableList.copyOf(recordTypes);
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
    public KeyExpression getAlternativeKeyExpression() {
        return index.getRootExpression();
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        // TODO: refactor
        return ValueIndexLikeMatchCandidate.computeOrderingFromKeyAndScanComparisons(((GroupingKeyExpression)index.getRootExpression()).getGroupingSubKey(), scanComparisons, isReverse, isDistinct);
    }

    @Nonnull
    @Override
    public RelationalExpression toEquivalentExpression(@Nonnull final PartialMatch partialMatch, @Nonnull final PlanContext planContext, @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of scan"));

        final var baseRecordType = Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(recordTypes));

        return new RecordQueryIndexPlan(index.getName(),
                null,
                IndexScanComparisons.byValue(toScanComparisons(comparisonRanges)),
                planContext.getPlannerConfiguration().getIndexFetchMethod(),
                reverseScanOrder,
                false,
                (ScanWithFetchMatchCandidate)partialMatch.getMatchCandidate(),
                baseRecordType);
    }

    @Nonnull
    private static ScanComparisons toScanComparisons(@Nonnull final List<ComparisonRange> comparisonRanges) {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }
}
