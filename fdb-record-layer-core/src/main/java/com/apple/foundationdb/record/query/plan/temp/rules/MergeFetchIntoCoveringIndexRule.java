/*
 * MergeFetchIntoCoveringIndexRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlanOf;

/**
 * A rule that merges a fetch into a covering index scan.
 */
@API(API.Status.EXPERIMENTAL)
public class MergeFetchIntoCoveringIndexRule extends PlannerRule<RecordQueryFetchFromPartialRecordPlan> {
    @Nonnull
    private static final BindingMatcher<RecordQueryIndexPlan> innerPlanMatcher = indexPlan();
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> root =
            fetchFromPartialRecordPlan(coveringIndexPlan().where(indexPlanOf(innerPlanMatcher)));

    public MergeFetchIntoCoveringIndexRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final RecordQueryIndexPlan innerPlan = call.get(innerPlanMatcher);
        call.yield(call.ref(innerPlan));
    }
}
