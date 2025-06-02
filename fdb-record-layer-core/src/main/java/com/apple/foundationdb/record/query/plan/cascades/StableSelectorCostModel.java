/*
 * StableSelectorCostModel.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;

/**
 * A comparator implementing a simple cost model for the {@link CascadesPlanner} to choose the plan with the smallest
 * plan hash.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class StableSelectorCostModel implements CascadesCostModel {
    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return RecordQueryPlannerConfiguration.defaultPlannerConfiguration();
    }

    @Override
    public int compare(@Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
        //
        // If plans are indistinguishable from a cost perspective, select one by planHash. This makes the cost model
        // stable (select the same plan on subsequent plannings).
        //
        if ((a instanceof PlanHashable) && (b instanceof PlanHashable)) {
            int hA = ((PlanHashable)a).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            int hB = ((PlanHashable)b).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            return Integer.compare(hA, hB);
        }

        return 0;
    }
}
