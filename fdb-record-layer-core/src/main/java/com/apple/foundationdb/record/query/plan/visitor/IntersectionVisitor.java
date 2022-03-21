/*
 * IntersectionVisitor.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
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
 *                                  IntersectionPlan
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
 *                                FetchFromPartialRecordPlan
 *                                          |
 *                                          |
 *                                  IntersectionPlan (IndexFetch)
 *                                  /              \
 *                                 /                \
 *                         RecordQueryPlan   RecordQueryPlan
 *
 */
public class IntersectionVisitor extends RecordQueryPlannerSubstitutionVisitor {
    public IntersectionVisitor(@Nonnull final RecordMetaData recordMetadata, @Nonnull final PlannableIndexTypes indexTypes, @Nullable final KeyExpression commonPrimaryKey) {
        super(recordMetadata, indexTypes, commonPrimaryKey);
    }

    @Nonnull
    @Override
    public RecordQueryPlan postVisit(@Nonnull final RecordQueryPlan recordQueryPlan) {
        if (recordQueryPlan instanceof RecordQueryIntersectionPlan) {
            RecordQueryIntersectionPlan intersectionPlan = (RecordQueryIntersectionPlan) recordQueryPlan;
            Set<KeyExpression> requiredFields = intersectionPlan.getRequiredFields();

            List<RecordQueryPlan> newChildren = new ArrayList<>(intersectionPlan.getChildren().size());
            for (RecordQueryPlan plan : intersectionPlan.getChildren()) {
                @Nullable RecordQueryPlan newPlan = removeIndexFetch(plan, requiredFields);
                if (newPlan == null) { // can't remove index fetch, so give up
                    return recordQueryPlan;
                }
                newChildren.add(newPlan);
            }
            return new RecordQueryFetchFromPartialRecordPlan(
                    RecordQueryIntersectionPlan.from(newChildren, intersectionPlan.getComparisonKey()),
                    TranslateValueFunction.unableToTranslate(),
                    new Type.Any());
        }

        return recordQueryPlan;
    }
}
