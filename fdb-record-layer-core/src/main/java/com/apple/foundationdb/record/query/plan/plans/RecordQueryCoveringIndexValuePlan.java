/*
 * RecordQueryCoveringIndexValuePlan.java
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
import com.apple.foundationdb.record.planprotos.PRecordQueryCoveringIndexValuePlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A query plan that reconstructs records from the entries in a covering index, by evaluating a value rather than by
 * running the copiers of an {@link com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord}, which
 * {@link RecordQueryCoveringIndexPlan} does for the heuristic planner. See
 * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2907">issue 2907</a>.
 */
@API(API.Status.INTERNAL)
public class RecordQueryCoveringIndexValuePlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithMatchCandidate, RecordQueryPlanWithIndexEntryToQueriedRecord {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Covering-Index-Value-Plan");

    @Nonnull
    private final RecordQueryPlanWithIndex indexPlan;
    @Nonnull
    private final String recordTypeName;
    @Nonnull
    private final Value indexEntryToRecordValue;

    protected RecordQueryCoveringIndexValuePlan(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryCoveringIndexValuePlan recordQueryCoveringIndexValuePlanProto) {
        this.indexPlan = (RecordQueryPlanWithIndex)RecordQueryPlan.fromRecordQueryPlanProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexValuePlanProto.getIndexPlan()));
        this.recordTypeName = Objects.requireNonNull(recordQueryCoveringIndexValuePlanProto.getRecordTypeName());
        this.indexEntryToRecordValue = Value.fromValueProto(serializationContext,
                Objects.requireNonNull(recordQueryCoveringIndexValuePlanProto.getIndexEntryToRecordValue()));
    }

    public RecordQueryCoveringIndexValuePlan(@Nonnull RecordQueryPlanWithIndex indexPlan,
                                            @Nonnull final String recordTypeName,
                                             @Nonnull Value indexEntryToRecordValue) {
        this.indexPlan = indexPlan;
        this.recordTypeName = recordTypeName;
        this.indexEntryToRecordValue = indexEntryToRecordValue;
    }

    @Nonnull
    @Override
    @SuppressWarnings("resource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return indexPlan
                .executeEntries(store, context, continuation, executeProperties)
                .map(indexEntry -> indexEntryToQueriedRecord(store, context, indexEntry))
                .map(queriedRecord -> QueryResult.fromQueriedRecord(getResultValue().getResultType(), context, queriedRecord));
    }

    @Nonnull
    @Override
    public <M extends Message> FDBQueriedRecord<M> indexEntryToQueriedRecord(@Nonnull final FDBRecordStoreBase<M> store,
                                                                            @Nonnull final EvaluationContext context,
                                                                            @Nonnull final IndexEntry indexEntry) {
        final var metaData = store.getRecordMetaData();
        return RecordQueryPlanWithIndexEntryToQueriedRecord.toQueriedRecord(store, context,
                metaData.getIndex(getIndexName()),
                metaData.getQueryableRecordType(recordTypeName),
                indexEntryToRecordValue,
                !getScanType().equals(IndexScanType.BY_GROUP),
                indexEntry);
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
    public RecordQueryCoveringIndexValuePlan strictlySorted(@Nonnull final FinalMemoizer memoizer) {
        return new RecordQueryCoveringIndexValuePlan(indexPlan.strictlySorted(memoizer), recordTypeName, indexEntryToRecordValue);
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return indexPlan.getMatchCandidateMaybe();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    public Value getIndexEntryToRecordValue() {
        return indexEntryToRecordValue;
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
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return indexPlan.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryCoveringIndexValuePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                              final boolean shouldSimplifyValues,
                                                              @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }

        final var translatedIndexPlan =
                indexPlan.translateCorrelations(translationMap, shouldSimplifyValues, translatedQuantifiers);
        if (translatedIndexPlan != indexPlan) {
            return new RecordQueryCoveringIndexValuePlan(translatedIndexPlan, recordTypeName, indexEntryToRecordValue);
        }
        return this;
    }

    @Override
    public boolean canBeMinimized() {
        return indexPlan.canBeMinimized();
    }

    @Nonnull
    @Override
    public RecordQueryCoveringIndexValuePlan minimize(@Nonnull final List<Quantifier.Physical> newQuantifiers) {
        Verify.verify(newQuantifiers.isEmpty());
        return new RecordQueryCoveringIndexValuePlan((RecordQueryPlanWithIndex)indexPlan.minimize(newQuantifiers),
                recordTypeName, indexEntryToRecordValue);
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
        final RecordQueryCoveringIndexValuePlan other = (RecordQueryCoveringIndexValuePlan) otherExpression;
        return indexPlan.structuralEquals(other.indexPlan, equivalencesMap) &&
               recordTypeName.equals(other.recordTypeName) &&
               indexEntryToRecordValue.equals(other.indexEntryToRecordValue);
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
        return Objects.hash(indexPlan, recordTypeName, indexEntryToRecordValue);
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
    public PRecordQueryCoveringIndexValuePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryCoveringIndexValuePlan.newBuilder()
                .setIndexPlan(indexPlan.toRecordQueryPlanProto(serializationContext))
                .setRecordTypeName(recordTypeName)
                .setIndexEntryToRecordValue(indexEntryToRecordValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setCoveringIndexValuePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryCoveringIndexValuePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryCoveringIndexValuePlan recordQueryCoveringIndexPlanProto) {
        return new RecordQueryCoveringIndexValuePlan(serializationContext, recordQueryCoveringIndexPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryCoveringIndexValuePlan, RecordQueryCoveringIndexValuePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryCoveringIndexValuePlan> getProtoMessageClass() {
            return PRecordQueryCoveringIndexValuePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryCoveringIndexValuePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PRecordQueryCoveringIndexValuePlan recordQueryCoveringIndexPlanProto) {
            return RecordQueryCoveringIndexValuePlan.fromProto(serializationContext, recordQueryCoveringIndexPlanProto);
        }
    }
}
