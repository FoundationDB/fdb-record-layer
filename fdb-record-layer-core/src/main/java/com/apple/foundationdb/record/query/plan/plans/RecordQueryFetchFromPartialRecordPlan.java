/*
 * RecordQueryFetchFromPartialRecordPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PFetchIndexRecords;
import com.apple.foundationdb.record.planprotos.PRecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.DerivedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that transforms a stream of partial records (derived from index entries, as in the {@link RecordQueryCoveringIndexPlan})
 * into full records by fetching the records by primary key.
 */
@API(API.Status.INTERNAL)
public class RecordQueryFetchFromPartialRecordPlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Fetch-From-Partial-Record-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Type resultType;
    @Nullable // planner-only
    private final TranslateValueFunction translateValueFunction;

    @Nonnull
    private final FetchIndexRecords fetchIndexRecords;

    @Nonnull
    private final Supplier<? extends Value> resultValueSupplier;

    protected RecordQueryFetchFromPartialRecordPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
        this.inner = Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getInner()));
        this.resultType = Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getResultType()));
        this.translateValueFunction = null; // not serialized as this is a planner-only structure
        this.fetchIndexRecords = FetchIndexRecords.fromProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getFetchIndexRecords()));
        this.resultValueSupplier = Suppliers.memoize(this::computeResultValue);
    }

    public RecordQueryFetchFromPartialRecordPlan(@Nonnull RecordQueryPlan inner, @Nonnull final TranslateValueFunction translateValueFunction, @Nonnull final Type resultType, @Nonnull final FetchIndexRecords fetchIndexRecords) {
        this(Quantifier.physical(Reference.of(inner)), translateValueFunction, resultType, fetchIndexRecords);
    }

    public RecordQueryFetchFromPartialRecordPlan(@Nonnull final Quantifier.Physical inner, @Nonnull final TranslateValueFunction translateValueFunction, @Nonnull final Type resultType, @Nonnull final FetchIndexRecords fetchIndexRecords) {
        this.inner = inner;
        this.resultType = resultType;
        this.translateValueFunction = translateValueFunction;
        this.fetchIndexRecords = fetchIndexRecords;
        this.resultValueSupplier = Suppliers.memoize(this::computeResultValue);
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return fetchIndexRecords.fetchIndexRecords(
                        store,
                        getChild().executePlan(store, context, continuation, executeProperties)
                                .map(QueryResult::getIndexEntry), executeProperties)
                .map(QueryResult::fromQueriedRecord);
    }

    @Nonnull
    public Quantifier.Physical getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Nonnull
    public FetchIndexRecords getFetchIndexRecords() {
        return fetchIndexRecords;
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_FETCH);
    }

    @Override
    public int getComplexity() {
        return 1 + getChild().getComplexity();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    public TranslateValueFunction getPushValueFunction() {
        return Objects.requireNonNull(translateValueFunction);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryFetchFromPartialRecordPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryFetchFromPartialRecordPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), Objects.requireNonNull(translateValueFunction), resultType, fetchIndexRecords);
    }

    @Nonnull
    public Optional<Value> pushValue(@Nonnull Value value, @Nonnull CorrelationIdentifier sourceAlias, @Nonnull CorrelationIdentifier targetAlias) {
        return Objects.requireNonNull(translateValueFunction).translateValue(value, sourceAlias, targetAlias);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(childRef), TranslateValueFunction.unableToTranslate(), resultType, fetchIndexRecords);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValueSupplier.get();
    }

    @Nonnull
    public Value computeResultValue() {
        return new DerivedValue(ImmutableList.of(QuantifiedObjectValue.of(inner.getAlias(), resultType)), resultType);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final var otherFetchPlan = (RecordQueryFetchFromPartialRecordPlan)otherExpression;
        return fetchIndexRecords == otherFetchPlan.fetchIndexRecords;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object o) {
        return structuralEquals(o);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH, fetchIndexRecords.name());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return 13 + 7 * getChild().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.FETCH_OPERATOR),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryFetchFromPartialRecordPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryFetchFromPartialRecordPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setResultType(resultType.toTypeProto(serializationContext))
                .setFetchIndexRecords(fetchIndexRecords.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setFetchFromPartialRecordPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryFetchFromPartialRecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                  @Nonnull final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
        return new RecordQueryFetchFromPartialRecordPlan(serializationContext, recordQueryFetchFromPartialRecordPlanProto);
    }

    /**
     * Enum to govern how to interpret the primary key of an index entry when accessing its base record(s).
     */
    public enum FetchIndexRecords {
        PRIMARY_KEY(new FetchIndexRecordsFunction() {
            @Nonnull
            @Override
            public <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                           @Nonnull final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                           @Nonnull final ExecuteProperties executeProperties) {
                return store.fetchIndexRecords(entryRecordCursor, IndexOrphanBehavior.ERROR, executeProperties.getState())
                        .map(store::queriedRecord);
            }
        }),
        SYNTHETIC_CONSTITUENTS(new FetchIndexRecordsFunction() {
            @Nonnull
            @Override
            public <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                           @Nonnull final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                           @Nonnull final ExecuteProperties executeProperties) {
                return entryRecordCursor.mapPipelined(
                        indexEntry -> store.loadSyntheticRecord(indexEntry.getPrimaryKey())
                                .thenApply(syntheticRecord -> FDBQueriedRecord.synthetic(indexEntry.getIndex(), indexEntry, syntheticRecord)),
                        store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
            }
        });

        @Nonnull
        private final FetchIndexRecordsFunction fetchIndexRecordsFunction;

        FetchIndexRecords(@Nonnull final FetchIndexRecordsFunction fetchIndexRecordsFunction) {
            this.fetchIndexRecordsFunction = fetchIndexRecordsFunction;
        }

        @Nonnull
        <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                @Nonnull final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                @Nonnull final ExecuteProperties executeProperties) {
            return fetchIndexRecordsFunction.fetchIndexRecords(store, entryRecordCursor, executeProperties);
        }

        @Nonnull
        @SuppressWarnings("unused")
        public final PFetchIndexRecords toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case PRIMARY_KEY:
                    return PFetchIndexRecords.PRIMARY_KEY;
                case SYNTHETIC_CONSTITUENTS:
                    return PFetchIndexRecords.SYNTHETIC_CONSTITUENTS;
                default:
                    throw new RecordCoreException("unknown fetch index records mapping. did you forget to add it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static FetchIndexRecords fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PFetchIndexRecords fetchIndexRecordsProto) {
            switch (fetchIndexRecordsProto) {
                case PRIMARY_KEY:
                    return PRIMARY_KEY;
                case SYNTHETIC_CONSTITUENTS:
                    return SYNTHETIC_CONSTITUENTS;
                default:
                    throw new RecordCoreException("unknown fetch index records mapping. did you forget to add it?");
            }
        }

        /**
         * The function to apply.
         */
        public interface FetchIndexRecordsFunction {
            @Nonnull
            <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(@Nonnull FDBRecordStoreBase<M> store,
                                                                                    @Nonnull RecordCursor<IndexEntry> entryRecordCursor,
                                                                                    @Nonnull ExecuteProperties executeProperties);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryFetchFromPartialRecordPlan, RecordQueryFetchFromPartialRecordPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryFetchFromPartialRecordPlan> getProtoMessageClass() {
            return PRecordQueryFetchFromPartialRecordPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryFetchFromPartialRecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                               @Nonnull final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
            return RecordQueryFetchFromPartialRecordPlan.fromProto(serializationContext, recordQueryFetchFromPartialRecordPlanProto);
        }
    }
}
