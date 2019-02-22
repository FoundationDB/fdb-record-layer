/*
 * RelationalPlannerExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;

import javax.annotation.Nonnull;

/**
 * A relational expression is a {@link PlannerExpression} that represents a stream of records. At all times, the root
 * expression being planned must be relational. This interface acts as a common tag interface for
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s, which can actually produce a stream of records,
 * and various logical relational expressions (not yet introduced), which represent an abstract stream of records but can't
 * be executed directly (such as an unimplemented sort). Other planner expressions such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}
 * and {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} do not represent streams of records.
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalPlannerExpression extends PlannerExpression {
    static PlannerExpression fromRecordQuery(@Nonnull RecordQuery query) {
        RelationalPlannerExpression expression = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
        if (query.getSort() != null) {
            expression = new LogicalSortExpression(query.getSort(), query.isSortReverse(), expression);
        }
        if (query.getFilter() != null) {
            expression = new LogicalFilterExpression(query.getFilter(), expression);
        }
        if (!query.getRecordTypes().isEmpty()) {
            expression = new LogicalTypeFilterExpression(query.getRecordTypes(), expression);
        }
        return expression;
    }
}
