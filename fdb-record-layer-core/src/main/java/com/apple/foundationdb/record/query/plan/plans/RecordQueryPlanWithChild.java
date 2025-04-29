/*
 * RecordQueryPlanWithChild.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A query plan with a single child plan.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithChild extends RecordQueryPlanWithChildren {
    @Override
    @Nonnull
    default List<RecordQueryPlan> getChildren() {
        return Collections.singletonList(getChild());
    }

    RecordQueryPlan getChild();

    @Override
    default int getRelationalChildCount() {
        return 1;
    }

    @Override
    default boolean hasRecordScan() {
        return getChild().hasRecordScan();
    }

    @Override
    default boolean hasFullRecordScan() {
        return getChild().hasFullRecordScan();
    }

    @Override
    default boolean hasIndexScan(@Nonnull String indexName) {
        return getChild().hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    default Set<String> getUsedIndexes() {
        return getChild().getUsedIndexes();
    }

    @Override
    default int maxCardinality(@Nonnull RecordMetaData metaData) {
        return getChild().maxCardinality(metaData);
    }

    @Override
    default boolean isStrictlySorted() {
        return getChild().isStrictlySorted();
    }

    @Override
    default RecordQueryPlanWithChild strictlySorted(@Nonnull final Memoizer memoizer) {
        return withChild(memoizer.memoizePlan((RecordQueryPlan)getChild().strictlySorted(memoizer)));
    }

    @Override
    default boolean hasLoadBykeys() {
        return getChild().hasLoadBykeys();
    }

    @Nonnull
    @Override
    default AvailableFields getAvailableFields() {
        return getChild().getAvailableFields();
    }

    @Nonnull
    RecordQueryPlanWithChild withChild(@Nonnull Reference childRef);
}
