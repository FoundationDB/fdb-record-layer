/*
 * QueryExecutor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A query executor that executes a plan and returns its results.
 */
public class QueryExecutor {
    private final RecordLayerSchema schema;
    private final RecordQueryPlan plan;
    private final EvaluationContext evaluationContext;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "internal implementation should have proper usage")
    public QueryExecutor(@Nonnull final RecordQueryPlan plan,
                         @Nonnull final EvaluationContext evaluationContext,
                         @Nonnull final RecordLayerSchema schema) {
        this.schema = schema;
        this.evaluationContext = evaluationContext;
        this.plan = plan;
    }

    @Nonnull
    public ResumableIterator<Row> execute(@Nullable final Continuation continuation,
                                          @Nonnull final ExecuteProperties executeProperties) throws RelationalException {
        final FDBRecordStoreBase<Message> fdbRecordStore = Assert.notNull(schema.loadStore()).unwrap(FDBRecordStoreBase.class);
        final RecordCursor<QueryResult> cursor = plan.executePlan(fdbRecordStore,
                evaluationContext,
                continuation == null ? null : continuation.getExecutionState(), executeProperties);

        return RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
    }

    public Type getQueryResultType() {
        return plan.getResultType().getInnerType();
    }
}
