/*
 * RecordQueryCoveringIndexPlan.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.planprotos.PRecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ScanWithFetchMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
@API(API.Status.INTERNAL)
public class RecordQueryCoveringIndexPlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithMatchCandidate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Covering-Index-Plan");

    private final RecordQueryPlanWithIndex indexPlan;
    private final String recordTypeName;
    @Nullable
    private final AvailableFields availableFields;
    private final IndexKeyValueToPartialRecord toRecord;

    protected RecordQueryCoveringIndexPlan(final PlanSerializationContext serializationContext,
                                           final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
        this.indexPlan = (RecordQueryPlanWithIndex)RecordQueryPlan.fromRecordQueryPlanProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getIndexPlan()));
        this.availableFields = null; // planner field
        this.recordTypeName = Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getRecordTypeName());
        this.toRecord = IndexKeyValueToPartialRecord.fromProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getToRecord()));
    }

    public RecordQueryCoveringIndexPlan(RecordQueryPlanWithIndex indexPlan,
                                        final String recordTypeName,
                                        AvailableFields availableFields,
                                        IndexKeyValueToPartialRecord toRecord) {
        this.indexPlan = indexPlan;
        this.availableFields = availableFields;
        this.recordTypeName = recordTypeName;
        this.toRecord = toRecord;
    }

    @Override
    @SuppressWarnings("resource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        return indexPlan
                .executeEntries(store, context, continuation, executeProperties)
                .map(indexEntryToQueriedRecord(store))
                .map(queriedRecord -> QueryResult.fromQueriedRecord(getResultValue().getResultType(), context, queriedRecord));
    }

    @API(API.Status.INTERNAL)
    public <M extends Message> Function<IndexEntry, FDBQueriedRecord<M>> indexEntryToQueriedRecord(final FDBRecordStoreBase<M> store) {
        final IndexScanType scanType = getScanType();
        boolean hasPrimaryKey = !scanType.equals(IndexScanType.BY_GROUP);
        return QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordTypeName, getIndexName(), toRecord, hasPrimaryKey);
    }

    public RecordQueryPlanWithIndex getIndexPlan() {
        return indexPlan;
    }

    public String getIndexName() {
        return indexPlan.getIndexName();
    }

    public IndexScanType getScanType() {
        return indexPlan.getScanType();
    }

    @Override
    public boolean isReverse() {
        return indexPlan.isReverse();
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(String indexName) {
        return indexPlan.hasIndexScan(indexName);
    }

    @Override
    public Set<String> getUsedIndexes() {
        return indexPlan.getUsedIndexes();
    }

    @Override
    public int maxCardinality(RecordMetaData metaData) {
        return indexPlan.maxCardinality(metaData);
    }

    @Override
    public boolean isStrictlySorted() {
        return indexPlan.isStrictlySorted();
    }

    @Override
    public RecordQueryCoveringIndexPlan strictlySorted(final FinalMemoizer memoizer) {
        return new RecordQueryCoveringIndexPlan(indexPlan.strictlySorted(memoizer), recordTypeName, getAvailableFields(), toRecord);
    }

    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return indexPlan.getMatchCandidateMaybe();
    }

    @Override
    public AvailableFields getAvailableFields() {
        return Objects.requireNonNull(availableFields);
    }

    public IndexKeyValueToPartialRecord getToRecord() {
        return toRecord;
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public Value getResultValue() {
        // TODO This should generate a value whose result type are the parts of what the index returns flattened out
        //      in the way that it is stored on disk. As we currently massage the index keys (and values) into a partial
        //      record we cannot do that just yet. In essence, we currently have to create a type that is the base record
        //      type with the assumption that columns not contained in the index are omitted from that record.
        return new IndexedValue(Objects.requireNonNull(indexPlan.getResultType().getInnerType()));
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return indexPlan.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryCoveringIndexPlan translateCorrelations(final TranslationMap translationMap,
                                                              final boolean shouldSimplifyValues,
                                                              final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }

        final var translatedIndexPlan =
                indexPlan.translateCorrelations(translationMap, shouldSimplifyValues, translatedQuantifiers);
        if (translatedIndexPlan != indexPlan) {
            return new RecordQueryCoveringIndexPlan(translatedIndexPlan, recordTypeName, getAvailableFields(), toRecord);
        }
        return this;
    }

    @Override
    public boolean canBeMinimized() {
        return indexPlan.canBeMinimized();
    }

    @Override
    public RecordQueryCoveringIndexPlan minimize(final List<Quantifier.Physical> newQuantifiers) {
        Verify.verify(newQuantifiers.isEmpty());
        return new RecordQueryCoveringIndexPlan((RecordQueryPlanWithIndex)indexPlan.minimize(newQuantifiers),
                recordTypeName, getAvailableFields(), toRecord);
    }

    public Optional<Value> pushValueThroughFetch(Value value,
                                                 CorrelationIdentifier sourceAlias,
                                                 CorrelationIdentifier targetAlias) {
        return indexPlan.getMatchCandidateMaybe()
                .flatMap(matchCandidate -> matchCandidate instanceof ScanWithFetchMatchCandidate ? Optional.of((ScanWithFetchMatchCandidate)matchCandidate) : Optional.empty())
                .flatMap(scanWithFetchMatchCandidate -> scanWithFetchMatchCandidate.pushValueThroughFetch(value, sourceAlias, targetAlias));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQueryCoveringIndexPlan other = (RecordQueryCoveringIndexPlan) otherExpression;
        return indexPlan.structuralEquals(other.indexPlan, equivalencesMap) &&
               recordTypeName.equals(other.recordTypeName) &&
               toRecord.equals(other.toRecord);
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(indexPlan, recordTypeName, toRecord);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_COVERING_INDEX);
    }

    @Override
    public int getComplexity() {
        return indexPlan.getComplexity();
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return indexPlan.planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, indexPlan);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return indexPlan.createIndexPlannerGraph(this,
                NodeInfo.COVERING_INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Override
    public PRecordQueryCoveringIndexPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryCoveringIndexPlan.newBuilder()
                .setIndexPlan(indexPlan.toRecordQueryPlanProto(serializationContext))
                .setRecordTypeName(recordTypeName)
                .setToRecord(toRecord.toProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setCoveringIndexPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryCoveringIndexPlan fromProto(final PlanSerializationContext serializationContext,
                                                         final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
        return new RecordQueryCoveringIndexPlan(serializationContext, recordQueryCoveringIndexPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryCoveringIndexPlan, RecordQueryCoveringIndexPlan> {
        @Override
        public Class<PRecordQueryCoveringIndexPlan> getProtoMessageClass() {
            return PRecordQueryCoveringIndexPlan.class;
        }

        @Override
        public RecordQueryCoveringIndexPlan fromProto(final PlanSerializationContext serializationContext,
                                                      final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
            return RecordQueryCoveringIndexPlan.fromProto(serializationContext, recordQueryCoveringIndexPlanProto);
        }
    }
}
