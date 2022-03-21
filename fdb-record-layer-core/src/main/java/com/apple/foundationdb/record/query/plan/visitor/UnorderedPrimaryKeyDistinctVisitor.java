/*
 * IndexFetchToUnorderedPrimaryKeyDistinctVisitor.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.Type;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;


/**
 *
 * This class moves the index fetch up from the index scan to after a distinct operation has been applied to the records.
 *
 *                               UnorderedPrimaryKeyDistinct
 *                                          |
 *                                          |
 *                             RecordQueryPlanWithIndex (Index Lookup)
 *
 *                                         ||
 *                                         ||
 *                                         V
 *
 *                               UnorderedPrimaryKeyDistinct (Index Lookup)
 *                                          |
 *                                          |
 *                             RecordQueryPlanWithIndex (Covering Index)
 *
 *
 *
 */
public class UnorderedPrimaryKeyDistinctVisitor extends RecordQueryPlannerSubstitutionVisitor {
    public UnorderedPrimaryKeyDistinctVisitor(@Nonnull final RecordMetaData recordMetadata, @Nonnull final PlannableIndexTypes indexTypes, @Nullable final KeyExpression commonPrimaryKey) {
        super(recordMetadata, indexTypes, commonPrimaryKey);
    }

    @Nonnull
    @Override
    public RecordQueryPlan postVisit(@Nonnull final RecordQueryPlan recordQueryPlan) {
        if (recordQueryPlan instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan) {
            RecordQueryUnorderedPrimaryKeyDistinctPlan distinctPlan = (RecordQueryUnorderedPrimaryKeyDistinctPlan) recordQueryPlan;
            @Nullable RecordQueryPlan newPlan = removeIndexFetch(distinctPlan.getChild(), Collections.emptySet());
            if (newPlan != null) {
                return new RecordQueryFetchFromPartialRecordPlan(
                        new RecordQueryUnorderedPrimaryKeyDistinctPlan(newPlan),
                        TranslateValueFunction.unableToTranslate(),
                        new Type.Any());
            }
        }
        return recordQueryPlan;
    }
}
