/*
 * RecordQueryPlanWithIndex.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A query plan that uses a single index. This is usually by scanning
 * the index in some way. This is meant for plans that directly use an index,
 * such as the {@link RecordQueryIndexPlan}. Plans that use an index but only
 * through one of their child plans will not implement this interface.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithIndex extends RecordQueryPlan, PlannerGraphRewritable {

    /**
     * Gets the name of the index used by this plan.
     *
     * @return the name of the index used by this plan
     */
    @Nonnull
    String getIndexName();

    @Nonnull
    IndexScanType getScanType();

    @Nonnull
    <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store,
                                                                @Nonnull EvaluationContext context,
                                                                @Nullable byte[] continuation,
                                                                @Nonnull ExecuteProperties executeProperties);

    @Nonnull
    @Override
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                          @Nonnull EvaluationContext context,
                                                                          @Nullable byte[] continuation,
                                                                          @Nonnull ExecuteProperties executeProperties) {
        final RecordCursor<IndexEntry> entryRecordCursor = executeEntries(store, context, continuation, executeProperties);
        return store.fetchIndexRecords(entryRecordCursor, IndexOrphanBehavior.ERROR, executeProperties.getState())
                .map(store::queriedRecord);
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Nonnull
    @Override
    default PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.Node(this,
                        getClass().getSimpleName(),
                        toString());
        final PlannerGraph.SourceNode source = new PlannerGraph.SourceNode(getIndexName());
        return PlannerGraph.builder(root)
                .addNode(source)
                .addEdge(source, root, new PlannerGraph.Edge())
                .build();
    }
}
