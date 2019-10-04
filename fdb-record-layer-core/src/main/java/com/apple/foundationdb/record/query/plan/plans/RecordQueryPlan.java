/*
 * RecordQueryPlan.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * An executable query plan for producing records.
 *
 * A query plan is run against a record store to produce a stream of matching records.
 *
 * A query plan of any complexity will have child plans and execute by altering or combining the children's streams in some way.
 *
 * @see com.apple.foundationdb.record.query.RecordQuery
 * @see com.apple.foundationdb.record.query.plan.RecordQueryPlanner#plan
 *
 */
@API(API.Status.STABLE)
public interface RecordQueryPlan extends QueryPlan<FDBQueriedRecord<Message>> {

    /**
     * Execute this query plan.
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                  @Nonnull EvaluationContext context,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ExecuteProperties executeProperties);

    @Nonnull
    @Override
    default RecordCursor<FDBQueriedRecord<Message>> execute(@Nonnull FDBRecordStore store,
                                                            @Nonnull EvaluationContext context,
                                                            @Nullable byte[] continuation,
                                                            @Nonnull ExecuteProperties executeProperties) {
        return execute((FDBRecordStoreBase<Message>)store, context, continuation, executeProperties);
    }

    /**
     * Execute this query plan.
     * @param store record store from which to fetch records
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store) {
        return execute(store, EvaluationContext.EMPTY);
    }

    /**
     * Execute this query plan.
     * @param store record store to access
     * @param context evaluation context containing parameter bindings
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        return execute(store, context, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    /**
     * Returns the (zero or more) {@code RecordQueryPlan} children of this plan.
     *
     * <p>
     * <b>Warning</b>: This part of the API is undergoing active development. At some point in the future,
     * the return type of this method will change to allow it to return a list of generic {@link QueryPlan}s.
     * At current, every {@code RecordQueryPlan} can only have other {@code RecordQueryPlan}s as children.
     * However, this is not guaranteed to be the case in the future. This method has been marked as
     * {@link API.Status#UNSTABLE} as of version 2.5.
     * </p>
     *
     * @return the child plans
     */
    @API(API.Status.UNSTABLE)
    @Nonnull
    List<RecordQueryPlan> getChildren();

    @Nonnull
    @Override
    default List<? extends QueryPlan<?>> getQueryPlanChildren() {
        return getChildren();
    }
}
