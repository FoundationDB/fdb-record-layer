/*
 * QueryPlanResult.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;

/**
 * The result of planning a query.
 * This is the result of the call of {@link QueryPlanner#plan(RecordQuery)} call. It contains the actual plan produced coupled
 * with some additional information related to the plan and the planning process.
 */
@API(API.Status.STABLE)
public class QueryPlanResult implements PlanHashable {
    @Nonnull
    private final RecordQueryPlan plan;
    @Nonnull
    private final QueryPlanInfo planInfo;

    public QueryPlanResult(@Nonnull final RecordQueryPlan plan) {
        this.plan = plan;
        planInfo = QueryPlanInfo.empty();
    }

    public QueryPlanResult(@Nonnull final RecordQueryPlan plan, @Nonnull final QueryPlanInfo planInfo) {
        this.plan = plan;
        this.planInfo = planInfo;
    }

    @Nonnull
    public RecordQueryPlan getPlan() {
        return plan;
    }

    @Nonnull
    public QueryPlanInfo getPlanInfo() {
        return planInfo;
    }

    /**
     * To make transition from RecordQueryPlan to QueryPlanResult smoother, this implements PlanHashable.
     * @param hashKind the "kind" of hash to calculate. Each kind of hash has a particular logic with regards to included and excluded items.
     * @return plan hash for the underlying plan.
     */
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return plan.planHash(hashKind);
    }

    @Override
    public int planHash() {
        return plan.planHash();
    }

    @Override
    public String toString() {
        return plan.toString();
    }
}
