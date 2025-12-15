/*
 * QueryPlanInfoKeys.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventStatsMaps;

/**
 * Container for {@link QueryPlanInfo.QueryPlanInfoKey} static instances used in the planner.
 */
public class QueryPlanInfoKeys {
    public static final QueryPlanInfo.QueryPlanInfoKey<Integer> TOTAL_TASK_COUNT =
            new QueryPlanInfo.QueryPlanInfoKey<>("totalTaskCount");
    public static final QueryPlanInfo.QueryPlanInfoKey<Integer> MAX_TASK_QUEUE_SIZE =
            new QueryPlanInfo.QueryPlanInfoKey<>("maxTaskQueueSize");
    public static final QueryPlanInfo.QueryPlanInfoKey<QueryPlanConstraint> CONSTRAINTS =
            new QueryPlanInfo.QueryPlanInfoKey<>("constraints");
    public static final QueryPlanInfo.QueryPlanInfoKey<PlannerEventStatsMaps> STATS_MAPS =
            new QueryPlanInfo.QueryPlanInfoKey<>("statsMaps");

    private QueryPlanInfoKeys() {
    }
}
