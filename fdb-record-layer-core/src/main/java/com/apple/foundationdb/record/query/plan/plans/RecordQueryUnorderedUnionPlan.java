/*
 * RecordQueryUnorderedUnionPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnorderedUnionCursor;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * A query plan that returns results from two-or-more cursors as they as ready. Unlike the {@link RecordQueryUnionPlan},
 * there are no ordering restrictions placed on the child plans (i.e., the children are free to return results
 * in any order). However, this plan also makes no effort to remove duplicates from its children, and it also
 * makes no guarantees as to what order it will return results.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryUnorderedUnionPlan extends RecordQueryUnionPlanBase {

    public RecordQueryUnorderedUnionPlan(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right, boolean reverse) {
        super(left, right, reverse);
    }

    public RecordQueryUnorderedUnionPlan(@Nonnull List<RecordQueryPlan> children, boolean reverse) {
        super(children, reverse);
    }

    @Nonnull
    @Override
    <M extends Message> RecordCursor<FDBQueriedRecord<M>> createUnionCursor(@Nonnull FDBRecordStoreBase<M> store,
                                                                            @Nonnull List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions,
                                                                            @Nullable byte[] continuation) {
        return UnorderedUnionCursor.create(childCursorFunctions, continuation, store.getTimer());
    }

    @Nonnull
    @Override
    String getDelimiter() {
        return " " + UNION + " ";
    }

    @Nonnull
    @Override
    public String toString() {
        return "Unordered(" + super.toString() + ")";
    }

    @Nonnull
    @Override
    StoreTimer.Count getPlanCount() {
        return FDBStoreTimer.Counts.PLAN_UNORDERED_UNION;
    }
}
