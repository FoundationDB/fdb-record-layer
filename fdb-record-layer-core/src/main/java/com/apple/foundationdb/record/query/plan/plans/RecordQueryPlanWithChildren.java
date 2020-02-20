/*
 * RecordQueryPlanWithChildren.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalExpressionWithChildren;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

/**
 * A query plan with child plans.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithChildren extends RecordQueryPlan, RelationalExpressionWithChildren {
    @Override
    default boolean hasRecordScan() {
        return getChildren().stream().anyMatch(RecordQueryPlan::hasRecordScan);
    }

    @Override
    default boolean hasFullRecordScan() {
        return getChildren().stream().anyMatch(RecordQueryPlan::hasFullRecordScan);
    }

    @Override
    default boolean hasIndexScan(@Nonnull String indexName) {
        return getChildren().stream().anyMatch(p -> p.hasIndexScan(indexName));
    }

    @Nonnull
    @Override
    default Set<String> getUsedIndexes() {
        final Set<String> result = new HashSet<>();
        for (RecordQueryPlan child : getChildren()) {
            result.addAll(child.getUsedIndexes());
        }
        return result;
    }

    @Override
    default boolean hasLoadBykeys() {
        return getChildren().stream().anyMatch(RecordQueryPlan::hasLoadBykeys);
    }
}
