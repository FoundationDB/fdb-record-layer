/*
 * UnionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.visitor;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Type;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 * This visitor pulls index fetches after the intersection if possible.
 *
 *                                  Starting Plan
 *
 *                                      UnionPlan
 *                                  /              \
 *                                 /                \
 *                 RecordQueryPlan (IndexFetch) RecordQueryPlan (IndexFetch)
 *
 *                                          |
 *                                          |
 *                                          V
 *
 *                                  Transformed Plan
 *
 *                                  UnionPlan (IndexFetch)
 *                                  /              \
 *                                 /                \
 *                         RecordQueryPlan   RecordQueryPlan
 *
 */
public class UnionVisitor extends RecordQueryPlannerSubstitutionVisitor {
    public UnionVisitor(@Nonnull final RecordMetaData recordMetadata, @Nonnull final PlannableIndexTypes indexTypes, @Nullable final KeyExpression commonPrimaryKey) {
        super(recordMetadata, indexTypes, commonPrimaryKey);
    }

    @Nonnull
    @Override
    public RecordQueryPlan postVisit(@Nonnull final RecordQueryPlan recordQueryPlan) {
        if (recordQueryPlan instanceof RecordQueryUnionPlanBase) {
            RecordQueryUnionPlanBase unionPlan = (RecordQueryUnionPlanBase) recordQueryPlan;

            final Set<KeyExpression> requiredFields = unionPlan.getRequiredFields();
            boolean shouldPullOutFilter = false;
            QueryComponent filter = null;
            if (unionPlan.getChildren().stream().allMatch(child -> child instanceof RecordQueryFilterPlan)) {
                filter = ((RecordQueryFilterPlan) unionPlan.getChildren().get(0)).getConjunctedFilter();
                final QueryComponent finalFilter = filter; // needed for lambda expression
                shouldPullOutFilter = unionPlan.getChildren().stream().allMatch(plan -> ((RecordQueryFilterPlan) plan).getConjunctedFilter().equals(finalFilter));
            }

            List<ExpressionRef<RecordQueryPlan>> newChildren = new ArrayList<>(unionPlan.getChildren().size());
            for (RecordQueryPlan plan : unionPlan.getChildren()) {
                if (shouldPullOutFilter) { // All children have the same filter so we'll try to move it
                    // Check if the plan under the filter can have its index fetch removed.
                    if (!(plan instanceof RecordQueryFilterPlan)) {
                        throw new RecordCoreException("serious logic error: thought this was a filter plan but it wasn't");
                    }
                    plan = ((RecordQueryFilterPlan) plan).getChild();
                }
                @Nullable RecordQueryPlan newPlan = removeIndexFetch(plan, requiredFields);
                if (newPlan == null) { // can't remove index fetch, so give up
                    return recordQueryPlan;
                }
                newChildren.add(GroupExpressionRef.of(newPlan));
            }
            RecordQueryPlan newUnionPlan = new RecordQueryFetchFromPartialRecordPlan(
                    unionPlan.withChildrenReferences(newChildren),
                    TranslateValueFunction.unableToTranslate(),
                    new Type.Any());

            if (shouldPullOutFilter) {
                return new RecordQueryFilterPlan(newUnionPlan, filter);
            } else {
                return newUnionPlan;
            }
        }
        return recordQueryPlan;
    }
}
