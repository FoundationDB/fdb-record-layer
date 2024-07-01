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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryScanPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRanges;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that scans records directly from the main tree within a range of primary keys.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RecordQueryScanPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithComparisons, PlannerGraphRewritable, RecordQueryPlanWithMatchCandidate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Scan-Plan");

    @Nullable
    private final Set<String> recordTypes;
    @Nonnull
    private final Type flowedType;
    @Nullable
    private final KeyExpression commonPrimaryKey;
    @Nonnull
    private final ScanComparisons comparisons;
    private final boolean reverse;
    private final boolean strictlySorted;
    @Nonnull
    private final Optional<? extends WithPrimaryKeyMatchCandidate> matchCandidateOptional;
    @Nonnull
    private final Supplier<ComparisonRanges> comparisonRangesSupplier;

    /**
     * Overloaded constructor.
     * Use other overloaded constructor to also pass in a set of record types.
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     */
    public RecordQueryScanPlan(@Nonnull ScanComparisons comparisons, boolean reverse) {
        this(null, new Type.Any(), null, comparisons, reverse, false, Optional.empty());
    }

    /**
     * Overloaded constructor for heuristic (old planner).
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param flowedType type of scan elements
     * @param commonPrimaryKey primary key of scanned records
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     * @param strictlySorted whether scan is strictly sorted for original query
     */
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes,
                               @Nonnull Type flowedType,
                               @Nullable KeyExpression commonPrimaryKey,
                               @Nonnull ScanComparisons comparisons,
                               boolean reverse,
                               boolean strictlySorted) {
        this(recordTypes, flowedType, commonPrimaryKey, comparisons, reverse, strictlySorted, Optional.empty());
    }

    /**
     * Overloaded constructor.
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param flowedType type of scan elements
     * @param commonPrimaryKey primary key of scanned records
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     * @param strictlySorted whether scan is strictly sorted for original query
     * @param matchCandidate a match candidate that was matched and resulted in this scan plan
     */
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes,
                               @Nonnull Type flowedType,
                               @Nullable KeyExpression commonPrimaryKey,
                               @Nonnull ScanComparisons comparisons,
                               boolean reverse,
                               boolean strictlySorted,
                               @Nonnull final WithPrimaryKeyMatchCandidate matchCandidate) {
        this(recordTypes, flowedType, commonPrimaryKey, comparisons, reverse, strictlySorted, Optional.of(matchCandidate));
    }


    /**
     * Overloaded constructor.
     * @param recordTypes a super set of record types of the records that this scan operator can produce
     * @param flowedType type of scan elements
     * @param commonPrimaryKey primary key of scanned records
     * @param comparisons comparisons to be applied by the operator
     * @param reverse indicator whether this scan is reverse
     * @param strictlySorted whether scan is strictly sorted for original query
     * @param matchCandidateOptional a match candidate optional that if not empty contains the match candidate that was
     *        matched and resulted in this scan plan
     */
    @VisibleForTesting
    public RecordQueryScanPlan(@Nullable Set<String> recordTypes,
                                @Nonnull Type flowedType,
                                @Nullable KeyExpression commonPrimaryKey,
                                @Nonnull ScanComparisons comparisons,
                                boolean reverse,
                                boolean strictlySorted,
                                @Nonnull final Optional<? extends WithPrimaryKeyMatchCandidate> matchCandidateOptional) {
        this.recordTypes = recordTypes == null ? null : ImmutableSet.copyOf(recordTypes);
        this.flowedType = flowedType;
        this.commonPrimaryKey = commonPrimaryKey;
        this.comparisons = comparisons;
        this.reverse = reverse;
        this.strictlySorted = strictlySorted;
        this.matchCandidateOptional = matchCandidateOptional;
        this.comparisonRangesSupplier = Suppliers.memoize(this::computeComparisonRanges);
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
    public ScanComparisons getScanComparisons() {
        return comparisons;
    }

    @Nonnull
    @Override
    public ComparisonRanges getComparisonRanges() {
        return comparisonRangesSupplier.get();
    }

    @Nonnull
    private ComparisonRanges computeComparisonRanges() {
        return ComparisonRanges.from(comparisons);
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
    public RecordQueryScanPlan strictlySorted(@Nonnull Memoizer memoizer) {
        return new RecordQueryScanPlan(recordTypes, flowedType, commonPrimaryKey, comparisons, reverse, true, matchCandidateOptional);
    }

    @Nonnull
    @Override
    public Optional<? extends WithPrimaryKeyMatchCandidate> getMatchCandidateMaybe() {
        return matchCandidateOptional;
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
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return comparisons.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryScanPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryScanPlan(recordTypes,
                flowedType,
                commonPrimaryKey,
                comparisons.translateCorrelations(translationMap),
                reverse,
                strictlySorted,
                matchCandidateOptional);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(flowedType);
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
               flowedType.equals(otherExpression.getResultValue().getResultType()) &&
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
        return Objects.hash(recordTypes, flowedType, commonPrimaryKey, comparisons, reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return comparisons.planHash(mode) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, comparisons, reverse, recordTypes, commonPrimaryKey);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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

    @Nonnull
    @Override
    public PRecordQueryScanPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryScanPlan.Builder builder = PRecordQueryScanPlan.newBuilder();
        builder.setHasRecordTypes(recordTypes != null);
        if (recordTypes != null) {
            for (final String recordType : recordTypes) {
                builder.addRecordTypes(recordType);
            }
        }
        builder.setFlowedType(flowedType.toTypeProto(serializationContext));
        if (commonPrimaryKey != null) {
            builder.setCommonPrimaryKey(commonPrimaryKey.toKeyExpression());
        }
        builder.setComparisons(comparisons.toProto(serializationContext));
        builder.setReverse(reverse);
        builder.setStrictlySorted(strictlySorted);
        return builder.build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setScanPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryScanPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryScanPlan recordQueryScanPlanProto) {
        Verify.verify(recordQueryScanPlanProto.hasReverse());
        Verify.verify(recordQueryScanPlanProto.hasStrictlySorted());
        Verify.verify(recordQueryScanPlanProto.hasHasRecordTypes());
        final Set<String> recordTypes;
        if (recordQueryScanPlanProto.getHasRecordTypes()) {
            final ImmutableSet.Builder<String> recordTypesBuilder = ImmutableSet.builder();
            for (int i = 0; i < recordQueryScanPlanProto.getRecordTypesCount(); i++) {
                recordTypesBuilder.add(recordQueryScanPlanProto.getRecordTypes(i));
            }
            recordTypes = recordTypesBuilder.build();
        } else {
            recordTypes = null;
        }
        final KeyExpression commonPrimaryKey;
        if (recordQueryScanPlanProto.hasCommonPrimaryKey()) {
            commonPrimaryKey = KeyExpression.fromProto(recordQueryScanPlanProto.getCommonPrimaryKey());
        } else {
            commonPrimaryKey = null;
        }
        return new RecordQueryScanPlan(recordTypes,
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordQueryScanPlanProto.getFlowedType())),
                commonPrimaryKey,
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(recordQueryScanPlanProto.getComparisons())),
                recordQueryScanPlanProto.getReverse(),
                recordQueryScanPlanProto.getStrictlySorted(),
                Optional.empty());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryScanPlan, RecordQueryScanPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryScanPlan> getProtoMessageClass() {
            return PRecordQueryScanPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryScanPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PRecordQueryScanPlan recordQueryScanPlanProto) {
            return RecordQueryScanPlan.fromProto(serializationContext, recordQueryScanPlanProto);
        }
    }
}
