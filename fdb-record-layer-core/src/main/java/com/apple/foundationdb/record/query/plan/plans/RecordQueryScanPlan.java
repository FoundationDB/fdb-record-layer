/*
 * RecordQueryScanPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that scans records directly from the main tree within a range of primary keys.
 */
@API(API.Status.INTERNAL)
public class RecordQueryScanPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithComparisons, PlannerGraphRewritable {
    @Nullable
    private final Set<String> recordTypes;

    @Nonnull
    private final ScanComparisons comparisons;
    private final boolean reverse;

    /**
     * Overloaded constructor.
     * Use the overloaded constructor {@link #RecordQueryScanPlan(Set, ScanComparisons, boolean)}
     * to also pass in a set of record types.
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     */
    public RecordQueryScanPlan(@Nonnull ScanComparisons comparisons, boolean reverse) {
        this(null, comparisons, reverse);
    }

    /**
     * Overloaded constructor.
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     */
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes, @Nonnull ScanComparisons comparisons, boolean reverse) {
        this.recordTypes = recordTypes == null ? null : ImmutableSet.copyOf(recordTypes);
        this.comparisons = comparisons;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final TupleRange range = comparisons.toTupleRange(store, context);
        return store.scanRecords(
                range.getLow(), range.getHigh(), range.getLowEndpoint(), range.getHighEndpoint(), continuation,
                executeProperties.asScanProperties(reverse))
                .map(store::queriedRecord);
    }

    @Nullable
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    @Override
    public ScanComparisons getComparisons() {
        return comparisons;
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return true;
    }

    @Override
    public boolean hasFullRecordScan() {
        // full record scan happens iff the bounds of the scan fields are unlimited
        return comparisons.isEmpty();
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return new HashSet<>();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }


    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public String toString() {
        @Nullable final TupleRange tupleRange = comparisons.toTupleRangeWithoutContext();
        final String range = tupleRange == null ? comparisons.toString() : tupleRange.toString();
        return "Scan(" + range + ")";
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryScanPlan rebase(@Nonnull final AliasMap translationMap) {
        return new RecordQueryScanPlan(getComparisons(), isReverse());
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQueryScanPlan that = (RecordQueryScanPlan)otherExpression;
        return Objects.equals(recordTypes, that.recordTypes) &&
               reverse == that.reverse &&
               Objects.equals(comparisons, that.comparisons);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(recordTypes, comparisons, reverse);
    }

    @Override
    public int planHash(PlanHashKind hashKind) {
        // TODO: Is this right?
        return comparisons.planHash(hashKind) + (reverse ? 1 : 0);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_SCAN);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    /**
     * Rewrite the planner graph for better visualization of a query scan plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models scanned storage as a separate node that is connected to the
     *         actual scan plan node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());

        @Nullable final TupleRange tupleRange = comparisons.toTupleRangeWithoutContext();

        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> additionalAttributes = ImmutableMap.builder();

        if (tupleRange != null) {
            detailsBuilder.add("range: " + tupleRange.getLowEndpoint().toString(false) + "{{low}}, {{high}}" + tupleRange.getHighEndpoint().toString(true));
            additionalAttributes.put("low", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            additionalAttributes.put("high", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("comparisons: {{comparisons}}");
            additionalAttributes.put("comparisons", Attribute.gml(comparisons.toString()));
        }

        if (reverse) {
            detailsBuilder.add("direction: {{direction}}");
            additionalAttributes.put("direction", Attribute.gml("reversed"));
        }

        final PlannerGraph.DataNodeWithInfo dataNodeWithInfo;
        if (getRecordTypes() == null) {
            dataNodeWithInfo =
                    new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                            ImmutableList.of("ALL"));
        } else {
            dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                    ImmutableList.of("record types: {{types}}"),
                    ImmutableMap.of("types", Attribute.gml(getRecordTypes().stream().map(Attribute::gml).collect(ImmutableList.toImmutableList()))));
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.SCAN_OPERATOR,
                        detailsBuilder.build(),
                        additionalAttributes.build()),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }
}
