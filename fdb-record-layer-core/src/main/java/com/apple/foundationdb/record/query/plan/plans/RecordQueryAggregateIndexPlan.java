/*
 * RecordQueryAggregateIndexPlan.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PRecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRanges;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * A query plan that reconstructs records from the entries in an aggregate index.
 */
@API(API.Status.INTERNAL)
public class RecordQueryAggregateIndexPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithMatchCandidate, RecordQueryPlanWithConstraint, RecordQueryPlanWithComparisons {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Aggregate-Index-Plan");

    @Nonnull
    private final RecordQueryIndexPlan indexPlan;
    @Nonnull
    private final String recordTypeName;
    @Nonnull
    private final IndexKeyValueToPartialRecord toRecord;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final QueryPlanConstraint constraint;

    /**
     * Creates an instance of {@link RecordQueryAggregateIndexPlan}.
     *
     * @param indexPlan The underlying index.
     * @param recordTypeName The name of the base record, used for debugging.
     * @param indexEntryToPartialRecordConverter A converter from index entry to record.
     * @param resultValue The result value.
     */
    public RecordQueryAggregateIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan,
                                         @Nonnull final String recordTypeName,
                                         @Nonnull final IndexKeyValueToPartialRecord indexEntryToPartialRecordConverter,
                                         @Nonnull final Value resultValue) {
        this(indexPlan, recordTypeName, indexEntryToPartialRecordConverter, resultValue, QueryPlanConstraint.tautology());
    }

    /**
     * Creates an instance of {@link RecordQueryAggregateIndexPlan}.
     *
     * @param indexPlan The underlying index.
     * @param recordTypeName The name of the base record, used for debugging.
     * @param indexEntryToPartialRecordConverter A converter from index entry to record.
     * @param resultValue The result value.
     * @param constraint The index filter.
     */
    public RecordQueryAggregateIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan,
                                         @Nonnull final String recordTypeName,
                                         @Nonnull final IndexKeyValueToPartialRecord indexEntryToPartialRecordConverter,
                                         @Nonnull final Value resultValue,
                                         @Nonnull final QueryPlanConstraint constraint) {
        this.indexPlan = indexPlan;
        this.recordTypeName = recordTypeName;
        this.toRecord = indexEntryToPartialRecordConverter;
        this.resultValue = resultValue;
        this.constraint = constraint;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final TypeRepository typeRepository = context.getTypeRepository();
        final Descriptors.Descriptor recordDescriptor = Objects.requireNonNull(typeRepository.getMessageDescriptor(resultValue.getResultType()));

        return indexPlan
                .executeEntries(store, context, continuation, executeProperties)
                .map(indexEntry -> {
                    final RecordMetaData metaData = store.getRecordMetaData();
                    final RecordType recordType = metaData.getRecordType(recordTypeName);
                    final Index index = metaData.getIndex(getIndexName());
                    return store.coveredIndexQueriedRecord(index, indexEntry, recordType, (M)toRecord.toRecord(recordDescriptor, indexEntry), false);
                })
                .map(QueryResult::fromQueriedRecord);
    }

    @Nonnull
    public RecordQueryIndexPlan getIndexPlan() {
        return indexPlan;
    }

    public Optional<Value> getGroupingValueMaybe() {
        final var hasGroupingValue = StreamSupport.stream(resultValue.getChildren().spliterator(), false).count() > 1;
        if (hasGroupingValue) {
            return Optional.of(resultValue.getChildren().iterator().next());
        } else {
            return Optional.empty();
        }
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
    public RecordQueryAggregateIndexPlan strictlySorted(@Nonnull final Memoizer memoizer) {
        return new RecordQueryAggregateIndexPlan(indexPlan.strictlySorted(memoizer), recordTypeName, toRecord, resultValue);
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return indexPlan.getMatchCandidateMaybe();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
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
        return resultValue;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final var result = ImmutableSet.<CorrelationIdentifier>builder();
        result.addAll(indexPlan.getCorrelatedTo());
        result.addAll(resultValue.getCorrelatedTo());
        return result.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryAggregateIndexPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedIndexPlan = indexPlan.translateCorrelations(translationMap, translatedQuantifiers);
        final var maybeNewResult = resultValue.translateCorrelations(translationMap);
        if (translatedIndexPlan != indexPlan || maybeNewResult != resultValue) {
            return new RecordQueryAggregateIndexPlan(translatedIndexPlan, recordTypeName, toRecord, maybeNewResult);
        }
        return this;
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
        final RecordQueryAggregateIndexPlan other = (RecordQueryAggregateIndexPlan) otherExpression;
        return indexPlan.structuralEquals(other.indexPlan, equivalencesMap) &&
               recordTypeName.equals(other.recordTypeName) &&
               toRecord.equals(other.toRecord) &&
               resultValue.semanticEquals(other.resultValue, equivalencesMap);
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
        return Objects.hash(indexPlan, recordTypeName, toRecord, resultValue);
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
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, indexPlan, resultValue);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return indexPlan.createIndexPlannerGraph(this,
                NodeInfo.INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Nonnull
    @Override
    public QueryPlanConstraint getConstraint() {
        return constraint;
    }

    @Nonnull
    @Override
    public ScanComparisons getScanComparisons() {
        return indexPlan.getScanComparisons();
    }

    @Override
    public boolean hasComparisonRanges() {
        return indexPlan.hasComparisonRanges();
    }

    @Nonnull
    @Override
    public ComparisonRanges getComparisonRanges() {
        return indexPlan.getComparisonRanges();
    }

    @Nonnull
    @Override
    public PRecordQueryAggregateIndexPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryAggregateIndexPlan.newBuilder()
                .setIndexPlan(indexPlan.toRecordQueryIndexPlanProto(serializationContext))
                .setRecordTypeName(recordTypeName)
                .setToRecord(toRecord.toProto(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .setConstraint(constraint.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setAggregateIndexPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryAggregateIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PRecordQueryAggregateIndexPlan recordQueryAggregateIndexPlanProto) {
        return new RecordQueryAggregateIndexPlan(RecordQueryIndexPlan.fromProto(serializationContext, Objects.requireNonNull(recordQueryAggregateIndexPlanProto.getIndexPlan())),
                Objects.requireNonNull(recordQueryAggregateIndexPlanProto.getRecordTypeName()),
                IndexKeyValueToPartialRecord.fromProto(serializationContext, Objects.requireNonNull(recordQueryAggregateIndexPlanProto.getToRecord())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryAggregateIndexPlanProto.getResultValue())),
                QueryPlanConstraint.fromProto(serializationContext, Objects.requireNonNull(recordQueryAggregateIndexPlanProto.getConstraint())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryAggregateIndexPlan, RecordQueryAggregateIndexPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryAggregateIndexPlan> getProtoMessageClass() {
            return PRecordQueryAggregateIndexPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryAggregateIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PRecordQueryAggregateIndexPlan recordQueryAggregateIndexPlanProto) {
            return RecordQueryAggregateIndexPlan.fromProto(serializationContext, recordQueryAggregateIndexPlanProto);
        }
    }
}
