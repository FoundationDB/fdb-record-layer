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
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ScanWithFetchMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
@API(API.Status.INTERNAL)
public class RecordQueryCoveringIndexPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithMatchCandidate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Covering-Index-Plan");

    @Nonnull
    private final RecordQueryPlanWithIndex indexPlan;
    @Nonnull
    private final String recordTypeName;
    @Nullable
    private final AvailableFields availableFields;
    @Nonnull
    private final IndexKeyValueToPartialRecord toRecord;

    protected RecordQueryCoveringIndexPlan(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
        this.indexPlan = (RecordQueryPlanWithIndex)RecordQueryPlan.fromRecordQueryPlanProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getIndexPlan()));
        this.availableFields = null; // planner field
        this.recordTypeName = Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getRecordTypeName());
        this.toRecord = IndexKeyValueToPartialRecord.fromProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexPlanProto.getToRecord()));
    }

    public RecordQueryCoveringIndexPlan(@Nonnull RecordQueryPlanWithIndex indexPlan,
                                        @Nonnull final String recordTypeName,
                                        @Nonnull AvailableFields availableFields,
                                        @Nonnull IndexKeyValueToPartialRecord toRecord) {
        this.indexPlan = indexPlan;
        this.availableFields = availableFields;
        this.recordTypeName = recordTypeName;
        this.toRecord = toRecord;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return indexPlan
                .executeEntries(store, context, continuation, executeProperties)
                .map(indexEntryToQueriedRecord(store))
                .map(QueryResult::fromQueriedRecord);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public <M extends Message> Function<IndexEntry, FDBQueriedRecord<M>> indexEntryToQueriedRecord(final @Nonnull FDBRecordStoreBase<M> store) {
        final IndexScanType scanType = getScanType();
        boolean hasPrimaryKey = !scanType.equals(IndexScanType.BY_GROUP);
        return QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordTypeName, getIndexName(), toRecord, hasPrimaryKey);
    }

    @Nonnull
    public RecordQueryPlanWithIndex getIndexPlan() {
        return indexPlan;
    }

    @Nonnull
    public String getIndexName() {
        return indexPlan.getIndexName();
    }

    @Nonnull
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
    public boolean hasIndexScan(@Nonnull String indexName) {
        return indexPlan.hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return indexPlan.getUsedIndexes();
    }

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        return indexPlan.maxCardinality(metaData);
    }

    @Override
    public boolean isStrictlySorted() {
        return indexPlan.isStrictlySorted();
    }

    @Override
    public RecordQueryCoveringIndexPlan strictlySorted(@Nonnull final Memoizer memoizer) {
        return new RecordQueryCoveringIndexPlan((RecordQueryPlanWithIndex)indexPlan.strictlySorted(memoizer), recordTypeName, getAvailableFields(), toRecord);
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return indexPlan.getMatchCandidateMaybe();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return Objects.requireNonNull(availableFields);
    }

    @Nonnull
    public IndexKeyValueToPartialRecord getToRecord() {
        return toRecord;
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        // TODO This should generate a value whose result type are the parts of what the index returns flattened out
        //      in the way that it is stored on disk. As we currently massage the index keys (and values) into a partial
        //      record we cannot do that just yet. In essence, we currently have to create a type that is the base record
        //      type with the assumption that columns not contained in the index are omitted from that record.
        return new IndexedValue(Objects.requireNonNull(indexPlan.getResultType().getInnerType()));
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return indexPlan.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryCoveringIndexPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedIndexPlan = indexPlan.translateCorrelations(translationMap, translatedQuantifiers);
        if (translatedIndexPlan != indexPlan) {
            return new RecordQueryCoveringIndexPlan(translatedIndexPlan, recordTypeName, getAvailableFields(), toRecord);
        }
        return this;
    }

    @Nonnull
    public Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                                 @Nonnull CorrelationIdentifier sourceAlias,
                                                 @Nonnull CorrelationIdentifier targetAlias) {
        return indexPlan.getMatchCandidateMaybe()
                .flatMap(matchCandidate -> matchCandidate instanceof ScanWithFetchMatchCandidate ? Optional.of((ScanWithFetchMatchCandidate)matchCandidate) : Optional.empty())
                .flatMap(scanWithFetchMatchCandidate -> scanWithFetchMatchCandidate.pushValueThroughFetch(value, sourceAlias, targetAlias));
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
    public int hashCodeWithoutChildren() {
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return indexPlan.planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, indexPlan);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return indexPlan.createIndexPlannerGraph(this,
                NodeInfo.COVERING_INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Nonnull
    @Override
    public PRecordQueryCoveringIndexPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryCoveringIndexPlan.newBuilder()
                .setIndexPlan(indexPlan.toRecordQueryPlanProto(serializationContext))
                .setRecordTypeName(recordTypeName)
                .setToRecord(toRecord.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setCoveringIndexPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryCoveringIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
        return new RecordQueryCoveringIndexPlan(serializationContext, recordQueryCoveringIndexPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryCoveringIndexPlan, RecordQueryCoveringIndexPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryCoveringIndexPlan> getProtoMessageClass() {
            return PRecordQueryCoveringIndexPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryCoveringIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PRecordQueryCoveringIndexPlan recordQueryCoveringIndexPlanProto) {
            return RecordQueryCoveringIndexPlan.fromProto(serializationContext, recordQueryCoveringIndexPlanProto);
        }
    }
}
