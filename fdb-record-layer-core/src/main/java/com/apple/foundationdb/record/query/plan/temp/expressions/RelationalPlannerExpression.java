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
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

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
    @Nonnull
    static PlannerExpression fromRecordQuery(@Nonnull RecordQuery query, @Nonnull PlanContext context) {

        RelationalPlannerExpression expression = new FullUnorderedScanExpression();
        final ViewExpression.Builder builder = ViewExpression.builder();
        for (String recordType : context.getRecordTypes()) {
            builder.addRecordType(recordType);
        }
        final Source baseSource = builder.buildBaseSource();
        if (query.getSort() != null) {
            List<Element> normalizedSort = query.getSort()
                    .normalizeForPlanner(baseSource, Function.identity())
                    .flattenForPlanner();
            expression = new LogicalSortExpression(normalizedSort, query.isSortReverse(), expression);
        }

        if (query.getFilter() != null) {
            final QueryPredicate normalized = query.getFilter().normalizeForPlanner(baseSource, Function.identity());
            expression = new LogicalFilterExpression(baseSource, normalized, expression);
        }

        if (!query.getRecordTypes().isEmpty()) {
            expression = new LogicalTypeFilterExpression(new HashSet<>(query.getRecordTypes()), expression);
        }
        if (query.removesDuplicates()) {
            expression = new LogicalDistinctExpression(expression);
        }
        return expression;
    }
}
