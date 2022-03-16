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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.DerivedValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
    private final Value resultValue;
    @Nonnull
    private final TranslateValueFunction translateValueFunction;

    public RecordQueryFetchFromPartialRecordPlan(@Nonnull RecordQueryPlan inner, @Nonnull final TranslateValueFunction translateValueFunction) {
        this(Quantifier.physical(GroupExpressionRef.of(inner)), translateValueFunction);
    }

    private RecordQueryFetchFromPartialRecordPlan(@Nonnull final Quantifier.Physical inner, @Nonnull final TranslateValueFunction translateValueFunction) {
        this.inner = inner;
        this.resultValue = new DerivedValue(ImmutableList.of(inner.getFlowedObjectValue()));
        this.translateValueFunction = translateValueFunction;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        // Plan return exactly one (full) record for each (partial) record from inner, so we can preserve all limits.
        return store.fetchIndexRecords(getChild().executePlan(store, context, continuation, executeProperties)
                .map(result -> result.getIndexEntry(0)), IndexOrphanBehavior.ERROR, executeProperties.getState())
                .map(store::queriedRecord)
                .map(QueryResult::of);
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
        return translateValueFunction;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryFetchFromPartialRecordPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryFetchFromPartialRecordPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class), translateValueFunction);
    }

    @Nonnull
    public Optional<Value> pushValue(@Nonnull Value value, @Nonnull CorrelationIdentifier newAlias) {
        return translateValueFunction.translateValue(value, QuantifiedColumnValue.of(newAlias, 0));
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryFetchFromPartialRecordPlan(child, TranslateValueFunction.unableToTranslate());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        return getClass() == otherExpression.getClass();
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
        return 31;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return 13 + 7 * getChild().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "Fetch(" + getChild().toString() + ")";
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.FETCH_OPERATOR),
                childGraphs);
    }
}
