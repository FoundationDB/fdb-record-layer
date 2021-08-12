/*
 * QueryPlanner.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;

/**
 * A common interface for classes that can plan a {@link RecordQuery} into a {@link RecordQueryPlan}. The common
 * interface allows tests to be run against several planners.
 */
@API(API.Status.STABLE)
public interface QueryPlanner {
    /**
     * Preference between index scan and record scan.
     * @see #setIndexScanPreference
     */
    enum IndexScanPreference {
        /**
         * Prefer a full scan over an index scan.
         * A full scan does not have to separately fetch the record, but may return records of types that just
         * need to be skipped.
         */
        PREFER_SCAN,
        /**
         * Prefer index scan over full scan.
         * An index scan always fetches the record separately but is less vulnerable to the addition of records
         * of unrelated record types.
         */
        PREFER_INDEX,
        /**
         * Prefer a scan using an index for <em>exactly</em> the primary key.
         */
        PREFER_PRIMARY_KEY_INDEX
    }

    /**
     * Create a plan to get the results of the provided query.
     *
     * @param query a query for records on this planner's metadata
     * @param parameterRelationshipGraph a set of bindings and their relationships that provide additional information
     *        to the planner that may improve plan quality but may also tighten requirements imposed on the parameter
     *        bindings that are used to execute the query
     * @return a plan that will return the results of the provided query when executed
     * @throws com.apple.foundationdb.record.RecordCoreException if the planner cannot plan the query
     */
    @Nonnull
    RecordQueryPlan plan(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph);

    /**
     * Create a plan to get the results of the provided query.
     *
     * @param query a query for records on this planner's metadata
     * @return a plan that will return the results of the provided query when executed
     * @throws com.apple.foundationdb.record.RecordCoreException if the planner cannot plan the query
     */
    @Nonnull
    default RecordQueryPlan plan(@Nonnull RecordQuery query) {
        return plan(query, ParameterRelationshipGraph.empty());
    }

    /**
     * Create a plan to get the results of the provided query.
     * This method returns a {@link QueryPlanResult} that contains the same plan as returned by {@link #plan(RecordQuery)}
     * with additional information provided in the {@link QueryPlanInfo}
     *
     * @param query a query for records on this planner's metadata
     * @param parameterRelationshipGraph a set of bindings and their relationships that provide additional information
     *        to the planner that may improve plan quality but may also tighten requirements imposed on the parameter
     *        bindings that are used to execute the query
     * @return a {@link QueryPlanResult} that contains the plan for the query with additional information
     * @throws com.apple.foundationdb.record.RecordCoreException if the planner cannot plan the query
     */
    @Nonnull
    QueryPlanResult planQuery(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph);

    @Nonnull
    default QueryPlanResult planQuery(@Nonnull RecordQuery query) {
        return planQuery(query, ParameterRelationshipGraph.empty());
    }

    /**
     * Get the {@link RecordMetaData} for this planner.
     * @return the meta-data
     */
    @Nonnull
    RecordMetaData getRecordMetaData();

    /**
     * Get the {@link RecordStoreState} for this planner.
     * @return the record store state
     */
    @Nonnull
    RecordStoreState getRecordStoreState();

    /**
     * Set whether {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} is preferred over
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} even when it does not satisfy any
     * additional conditions.
     * Scanning without an index is more efficient, but will have to skip over unrelated record types.
     * For that reason, it is safer to use an index, except when there is only one record type.
     * If the meta-data has more than one record type but the record store does not, this can be overridden.
     * @param indexScanPreference whether to prefer index scan over record scan
     */
    void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference);

    @Nonnull
    RecordQueryPlannerConfiguration getConfiguration();

    /**
     * Set the {@link RecordQueryPlannerConfiguration} for this planner.
     * If an {@link com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference} is already set using
     * {@link #setIndexScanPreference(IndexScanPreference)} then it will be ignored.
     * @param configuration a configuration object for this planner
     */
    void setConfiguration(@Nonnull final RecordQueryPlannerConfiguration configuration);
}
