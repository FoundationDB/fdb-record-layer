/*
 * RecordQueryFilterPlanBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryFilterPlanBase;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A base class for all query plans that filter based on predicates.
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryFilterPlanBase implements RecordQueryPlanWithChild {
    @Nonnull
    private final Quantifier.Physical inner;

    @Nonnull
    private static final Set<StoreTimer.Count> inCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Counts.QUERY_FILTER_PLAN_GIVEN);
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_FILTER);
    @Nonnull
    private static final Set<StoreTimer.Count> successCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_FILTER_PLAN_PASSED);
    @Nonnull
    private static final Set<StoreTimer.Count> failureCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_DISCARDED);

    protected RecordQueryFilterPlanBase(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PRecordQueryFilterPlanBase recordQueryFilterPlanBaseProto) {
        this(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFilterPlanBaseProto.getInner())));
    }

    protected RecordQueryFilterPlanBase(@Nonnull Quantifier.Physical inner) {
        this.inner = inner;
    }

    protected abstract boolean hasAsyncFilter();

    @Nullable
    protected abstract <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store,
                                                              @Nonnull EvaluationContext context,
                                                              @Nonnull QueryResult datum);

    @Nullable
    protected abstract <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store,
                                                                                      @Nonnull EvaluationContext context,
                                                                                      @Nonnull QueryResult datum);

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final RecordCursor<QueryResult> results = getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());

        if (hasAsyncFilter()) {
            return results
                    .filterAsyncInstrumented(result -> evalFilterAsync(store, context, result),
                            store.getPipelineSize(PipelineOperation.RECORD_ASYNC_FILTER),
                            store.getTimer(), inCounts, duringEvents, successCounts, failureCounts)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        } else {
            return results
                    .filterInstrumented(result -> evalFilter(store, context, result), store.getTimer(),
                            inCounts, duringEvents, successCounts, failureCounts)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        }
    }

    @Nonnull
    public Quantifier.Physical getInner() {
        return inner;
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Override
    public boolean hasRecordScan() {
        return getInnerPlan().hasRecordScan();
    }

    @Override
    public boolean hasFullRecordScan() {
        return getInnerPlan().hasFullRecordScan();
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getInnerPlan().hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return getInnerPlan().getUsedIndexes();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_FILTER);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    @Nonnull
    protected PRecordQueryFilterPlanBase toRecordQueryFilterPlanBaseProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryFilterPlanBase.newBuilder().setInner(inner.toProto(serializationContext)).build();
    }
}
