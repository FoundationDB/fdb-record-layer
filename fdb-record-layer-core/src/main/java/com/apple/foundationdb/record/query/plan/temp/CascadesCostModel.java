/*
 * CascadesCostModel.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.properties.ElementPredicateCountProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.RelationalExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.TypeFilterCountProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.UnmatchedFieldsProperty;

import javax.annotation.Nonnull;
import java.util.Comparator;

/**
 * A comparator implementing the current heuristic cost model for the {@link CascadesPlanner}.
 */
@API(API.Status.EXPERIMENTAL)
public class CascadesCostModel implements Comparator<PlannerExpression> {
    @Nonnull
    private final PlanContext planContext;

    public CascadesCostModel(@Nonnull PlanContext planContext) {
        this.planContext = planContext;
    }

    @Override
    public int compare(@Nonnull PlannerExpression a, @Nonnull PlannerExpression b) {
        if (a instanceof RecordQueryPlan && !(b instanceof RecordQueryPlan)) {
            return -1;
        }
        if (!(a instanceof RecordQueryPlan) && b instanceof RecordQueryPlan) {
            return 1;
        }

        int unsatisfiedFilterCompare = Integer.compare(ElementPredicateCountProperty.evaluate(a),
                ElementPredicateCountProperty.evaluate(b));
        if (unsatisfiedFilterCompare != 0) {
            return unsatisfiedFilterCompare;
        }

        int typeFilterCountCompare = Integer.compare(TypeFilterCountProperty.evaluate(a),
                TypeFilterCountProperty.evaluate(b));
        if (typeFilterCountCompare != 0) {
            return typeFilterCountCompare;
        }

        int typeFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(a)); // prefer the one with a deeper type filter
        if (typeFilterPositionCompare != 0) {
            return typeFilterPositionCompare;
        }

        int distinctFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(a));
        if (distinctFilterPositionCompare != 0) {
            return distinctFilterPositionCompare;
        }
        return Integer.compare(UnmatchedFieldsProperty.evaluate(planContext, a),
                UnmatchedFieldsProperty.evaluate(planContext, b));
    }
}
