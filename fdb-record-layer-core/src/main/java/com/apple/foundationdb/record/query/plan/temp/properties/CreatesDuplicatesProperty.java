/*
 * CreatesDuplicatesProperty.java
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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A property that determines whether the expression may produce duplicate entries. If the given expression is a
 * {@link RelationalExpression}, this will return whether the expression might produce multiple instances
 * of the same record. If the given expression is a {@link KeyExpression}, then this will return whether the
 * expression might return multiple results for the same record.
 */
@API(API.Status.EXPERIMENTAL)
public class CreatesDuplicatesProperty implements PlannerProperty<Boolean> {
    @Nonnull
    private final PlanContext context;

    private CreatesDuplicatesProperty(@Nonnull PlanContext context) {
        this.context = context;
    }

    @Nonnull
    @Override
    public Boolean evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Boolean> childResults) {
        String indexName = null;
        if (expression instanceof RecordQueryPlanWithIndex) {
            indexName = ((RecordQueryPlanWithIndex)expression).getIndexName();
        } else if (expression instanceof IndexScanExpression) {
            indexName = ((IndexScanExpression)expression).getIndexName();
        }
        if (indexName != null) {
            return context.getIndexByName(indexName).getRootExpression().createsDuplicates();
        }

        if (expression instanceof RecordQueryUnorderedDistinctPlan ||
                expression instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan ||
                expression instanceof LogicalDistinctExpression) {
            // These expressions filter out duplicates, so they therefore will not return duplicates even if their
            // children will.
            return Boolean.FALSE;
        } else if (expression instanceof RecordQueryUnorderedUnionPlan || expression instanceof LogicalUnorderedUnionExpression) {
            // These expressions can create duplicates even if the underlying children do not.
            return Boolean.TRUE;
        } else {
            // Return whether any visited child creates duplicates.
            return childResults.stream().anyMatch(b -> b != null && b);
        }
    }

    @Nonnull
    @Override
    public Boolean evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Boolean> memberResults) {
        return memberResults.stream().anyMatch(b -> b != null && b);
    }

    public static boolean evaluate(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull PlanContext context) {
        return ref.acceptPropertyVisitor(new CreatesDuplicatesProperty(context));
    }

    public static boolean evaluate(@Nonnull RelationalExpression expression, @Nonnull PlanContext context) {
        // Won't actually be null for relational planner expressions.
        return expression.acceptPropertyVisitor(new CreatesDuplicatesProperty(context));
    }
}
