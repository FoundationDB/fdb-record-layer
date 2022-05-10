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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Scan-Plan");

    @Nullable
    private final Set<String> recordTypes;
    @Nullable
    private final KeyExpression commonPrimaryKey;
    @Nonnull
    private final ScanComparisons comparisons;
    private final boolean reverse;
    private final boolean strictlySorted;

    /**
     * Overloaded constructor.
     * Use the overloaded constructor {@link #RecordQueryScanPlan(Set, KeyExpression, ScanComparisons, boolean, boolean)}
     * to also pass in a set of record types.
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     */
    public RecordQueryScanPlan(@Nonnull ScanComparisons comparisons, boolean reverse) {
        this(null, null, comparisons, reverse, false);
    }

    /**
     * Overloaded constructor.
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param commonPrimaryKey common primary key
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     */
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes,
                               @Nullable KeyExpression commonPrimaryKey,
                               @Nonnull ScanComparisons comparisons,
                               boolean reverse) {
        this(recordTypes, commonPrimaryKey, comparisons, reverse, false);
    }

    /**
     * Overloaded constructor.
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     * @param strictlySorted whether scan is stricted sorted for original query
     */
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes, @Nullable KeyExpression commonPrimaryKey, @Nonnull ScanComparisons comparisons, boolean reverse, boolean strictlySorted) {
        this.recordTypes = recordTypes == null ? null : ImmutableSet.copyOf(recordTypes);
        this.commonPrimaryKey = commonPrimaryKey;
        this.comparisons = comparisons;
        this.reverse = reverse;
        this.strictlySorted = strictlySorted;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final TupleRange range = comparisons.toTupleRange(store, context);
        return store.scanRecords(
                range.getLow(), range.getHigh(), range.getLowEndpoint(), range.getHighEndpoint(), continuation,
                executeProperties.asScanProperties(reverse))
                .map(store::queriedRecord)
                .map(QueryResult::fromQueriedRecord);
    }

    @Nullable
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Nullable
    public KeyExpression getCommonPrimaryKey() {
        return commonPrimaryKey;
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

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        if (comparisons.isEquality() &&
                metaData.getRecordTypes().values().stream().allMatch(t -> t.getPrimaryKey().getColumnSize() <= comparisons.size())) {
            return 1;
        } else {
            return UNKNOWN_MAX_CARDINALITY;
        }
    }

    @Override
    public boolean isStrictlySorted() {
        return strictlySorted;
    }

    @Override
    public RecordQueryScanPlan strictlySorted() {
        return new RecordQueryScanPlan(recordTypes, commonPrimaryKey, comparisons, reverse, true);
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
    public RecordQueryScanPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        // TODO make return this dependent on whether the index scan is correlated according to the translation map
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(new Type.Any());
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
               Objects.equals(commonPrimaryKey, that.commonPrimaryKey) &&
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
        return Objects.hash(recordTypes, commonPrimaryKey, comparisons, reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return comparisons.planHash(hashKind) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, comparisons, reverse, recordTypes, commonPrimaryKey);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
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
                            getResultType(),
                            ImmutableList.of("ALL"));
        } else {
            dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                    getResultType(),
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
