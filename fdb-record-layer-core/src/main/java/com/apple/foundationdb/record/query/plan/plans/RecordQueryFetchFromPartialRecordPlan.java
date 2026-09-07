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
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.DerivedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

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
public class RecordQueryFetchFromPartialRecordPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Fetch-From-Partial-Record-Plan");

    private final Quantifier.Physical inner;
    private final Type resultType;
    @Nullable // planner-only
    private final TranslateValueFunction translateValueFunction;

    private final FetchIndexRecords fetchIndexRecords;

    @SuppressWarnings("this-escape")
    private final Supplier<? extends Value> resultValueSupplier = Suppliers.memoize(this::computeResultValue);

    protected RecordQueryFetchFromPartialRecordPlan(final PlanSerializationContext serializationContext,
                                                    final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
        this.inner = Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getInner()));
        this.resultType = Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getResultType()));
        this.translateValueFunction = null; // not serialized as this is a planner-only structure
        this.fetchIndexRecords = FetchIndexRecords.fromProto(serializationContext, Objects.requireNonNull(recordQueryFetchFromPartialRecordPlanProto.getFetchIndexRecords()));
    }

    @HeuristicPlanner
    public RecordQueryFetchFromPartialRecordPlan(RecordQueryPlan inner,
                                                 final TranslateValueFunction translateValueFunction,
                                                 final Type resultType,
                                                 final FetchIndexRecords fetchIndexRecords) {
        this(Quantifier.physical(Reference.plannedOf(Debugger.verifyHeuristicPlanner(inner))),
                translateValueFunction, resultType, fetchIndexRecords);
    }

    public RecordQueryFetchFromPartialRecordPlan(final Quantifier.Physical inner, final TranslateValueFunction translateValueFunction, final Type resultType, final FetchIndexRecords fetchIndexRecords) {
        this.inner = inner;
        this.resultType = resultType;
        this.translateValueFunction = translateValueFunction;
        this.fetchIndexRecords = fetchIndexRecords;
    }

    @Override
    // QueryResult.getIndexEntry() is legitimately @Nullable, but the target type of
    // this method reference is the JDK's non-nullness-aware java.util.function.Function,
    // whose R is inferred @NonNull here from RecordCursor.map's declared signature.
    @SuppressWarnings({"resource", "NullAway"})
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        return fetchIndexRecords.fetchIndexRecords(
                        store,
                        getChild().executePlan(store, context, continuation, executeProperties)
                                .map(QueryResult::getIndexEntry), executeProperties)
                .map(queriedRecord -> QueryResult.fromQueriedRecord(resultType, context, queriedRecord));
    }

    public Quantifier.Physical getInner() {
        return inner;
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

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

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    public TranslateValueFunction getPushValueFunction() {
        return Objects.requireNonNull(translateValueFunction);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public RecordQueryFetchFromPartialRecordPlan translateCorrelations(final TranslationMap translationMap,
                                                                       final boolean shouldSimplifyValues,
                                                                       final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        return new RecordQueryFetchFromPartialRecordPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                Objects.requireNonNull(translateValueFunction), resultType, fetchIndexRecords);
    }

    public Optional<Value> pushValue(Value value, CorrelationIdentifier sourceAlias, CorrelationIdentifier targetAlias) {
        return Objects.requireNonNull(translateValueFunction).translateValue(value, sourceAlias, targetAlias);
    }

    @Override
    public RecordQueryPlanWithChild withChild(final Reference childRef) {
        return new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(childRef, inner.getAlias()),
                TranslateValueFunction.unableToTranslate(), resultType, fetchIndexRecords);
    }

    @Override
    public Value getResultValue() {
        return resultValueSupplier.get();
    }

    public Value computeResultValue() {
        return new DerivedValue(ImmutableList.of(QuantifiedObjectValue.of(inner.getAlias(), resultType)), resultType);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(final RelationalExpression otherExpression, final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        if (!semanticEqualsForResults(otherExpression, equivalences)) {
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH, fetchIndexRecords.name());
    }

    @Override
    public int planHash(final PlanHashMode mode) {
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
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.FETCH_OPERATOR),
                childGraphs);
    }

    @Override
    public PRecordQueryFetchFromPartialRecordPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryFetchFromPartialRecordPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setResultType(resultType.toTypeProto(serializationContext))
                .setFetchIndexRecords(fetchIndexRecords.toProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setFetchFromPartialRecordPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryFetchFromPartialRecordPlan fromProto(final PlanSerializationContext serializationContext,
                                                                  final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
        return new RecordQueryFetchFromPartialRecordPlan(serializationContext, recordQueryFetchFromPartialRecordPlanProto);
    }

    /**
     * Enum to govern how to interpret the primary key of an index entry when accessing its base record(s).
     */
    public enum FetchIndexRecords {
        PRIMARY_KEY(new FetchIndexRecordsFunction() {
            @Override
            public <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(final FDBRecordStoreBase<M> store,
                                                                                           final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                           final ExecuteProperties executeProperties) {
                return store.fetchIndexRecords(entryRecordCursor, IndexOrphanBehavior.ERROR, executeProperties.getState())
                        .map(store::queriedRecord);
            }
        }),
        SYNTHETIC_CONSTITUENTS(new FetchIndexRecordsFunction() {
            @Override
            public <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(final FDBRecordStoreBase<M> store,
                                                                                           final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                           final ExecuteProperties executeProperties) {
                return entryRecordCursor.mapPipelined(
                        indexEntry -> store.loadSyntheticRecord(indexEntry.getPrimaryKey())
                                .thenApply(syntheticRecord -> FDBQueriedRecord.synthetic(indexEntry.getIndex(), indexEntry, syntheticRecord)),
                        store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
            }
        });

        private final FetchIndexRecordsFunction fetchIndexRecordsFunction;

        FetchIndexRecords(final FetchIndexRecordsFunction fetchIndexRecordsFunction) {
            this.fetchIndexRecordsFunction = fetchIndexRecordsFunction;
        }

        <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(final FDBRecordStoreBase<M> store,
                                                                                final RecordCursor<IndexEntry> entryRecordCursor,
                                                                                final ExecuteProperties executeProperties) {
            return fetchIndexRecordsFunction.fetchIndexRecords(store, entryRecordCursor, executeProperties);
        }

        @SuppressWarnings("unused")
        public final PFetchIndexRecords toProto(final PlanSerializationContext serializationContext) {
            switch (this) {
                case PRIMARY_KEY:
                    return PFetchIndexRecords.PRIMARY_KEY;
                case SYNTHETIC_CONSTITUENTS:
                    return PFetchIndexRecords.SYNTHETIC_CONSTITUENTS;
                default:
                    throw new RecordCoreException("unknown fetch index records mapping. did you forget to add it?");
            }
        }

        @SuppressWarnings("unused")
        public static FetchIndexRecords fromProto(final PlanSerializationContext serializationContext,
                                                  final PFetchIndexRecords fetchIndexRecordsProto) {
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
            <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(FDBRecordStoreBase<M> store,
                                                                                    RecordCursor<IndexEntry> entryRecordCursor,
                                                                                    ExecuteProperties executeProperties);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryFetchFromPartialRecordPlan, RecordQueryFetchFromPartialRecordPlan> {
        @Override
        public Class<PRecordQueryFetchFromPartialRecordPlan> getProtoMessageClass() {
            return PRecordQueryFetchFromPartialRecordPlan.class;
        }

        @Override
        public RecordQueryFetchFromPartialRecordPlan fromProto(final PlanSerializationContext serializationContext,
                                                               final PRecordQueryFetchFromPartialRecordPlan recordQueryFetchFromPartialRecordPlanProto) {
            return RecordQueryFetchFromPartialRecordPlan.fromProto(serializationContext, recordQueryFetchFromPartialRecordPlanProto);
        }
    }
}
