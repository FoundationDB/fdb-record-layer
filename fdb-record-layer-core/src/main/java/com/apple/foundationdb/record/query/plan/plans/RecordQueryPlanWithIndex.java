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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A query plan that uses a single index. This is usually by scanning
 * the index in some way. This is meant for plans that directly use an index,
 * such as the {@link RecordQueryIndexPlan}. Plans that use an index but only
 * through one of their child plans will not implement this interface.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithIndex extends RecordQueryPlan {

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
    Optional<? extends MatchCandidate> getMatchCandidateOptional();

    @Nonnull
    @Override
    RecordQueryPlanWithIndex translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers);

    @Nonnull
    <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store,
                                                                @Nonnull EvaluationContext context,
                                                                @Nullable byte[] continuation,
                                                                @Nonnull ExecuteProperties executeProperties);

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    default <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                      @Nonnull EvaluationContext context,
                                                                      @Nullable byte[] continuation,
                                                                      @Nonnull ExecuteProperties executeProperties) {
        final RecordCursor<IndexEntry> entryRecordCursor = executeEntries(store, context, continuation, executeProperties);
        return store.fetchIndexRecords(entryRecordCursor, IndexOrphanBehavior.ERROR, executeProperties.getState())
                .map(store::queriedRecord)
                .map(QueryResult::fromQueriedRecord);
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
        Verify.verify(childGraphs.isEmpty());
        return createIndexPlannerGraph(this,
                NodeInfo.INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    /**
     * Create an planner graph for this index scan. Note that this method allows for composition with the covering
     * index scan path. It is called by {@link #rewritePlannerGraph} to create the subgraph but allows for greater
     * flexibility and parameterization of the constituent parts.
     *
     * @param identity identity of the node representing the index scan, may be {@code this} or some other plan object
     * @param nodeInfo node info to determine the actul flavor of index scan
     * @param additionalDetails additional details to be kept with the index scan node
     * @param additionalAttributeMap additional attributes to be kept with the index scan node
     * @return a new planner graph representing the index scan
     */
    @Nonnull
    PlannerGraph createIndexPlannerGraph(@Nonnull RecordQueryPlan identity,
                                         @Nonnull NodeInfo nodeInfo,
                                         @Nonnull List<String> additionalDetails,
                                         @Nonnull Map<String, Attribute> additionalAttributeMap);
}
