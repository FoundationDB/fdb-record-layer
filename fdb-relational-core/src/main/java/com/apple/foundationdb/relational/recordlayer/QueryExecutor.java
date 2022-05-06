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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A query executor that executes a plan and returns its results.
 */
public class QueryExecutor {
    private final RecordLayerSchema schema;
    private final RecordQueryPlan plan;
    private final String[] expectedFieldNames;
    private final EvaluationContext evaluationContext;
    private final boolean isExplain;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "internal implementation should have proper usage")
    public QueryExecutor(@Nonnull final RecordQueryPlan plan,
                         @Nonnull final String[] expectedFieldNames,
                         @Nonnull final EvaluationContext evaluationContext,
                         @Nonnull final RecordLayerSchema schema,
                         boolean isExplain) {
        this.schema = schema;
        this.expectedFieldNames = expectedFieldNames;
        this.evaluationContext = evaluationContext;
        this.isExplain = isExplain;
        this.plan = plan;
    }

    @Nonnull
    public ResumableIterator<Row> execute(@Nullable Continuation continuation) throws RelationalException {
        if (!isExplain) {
            Assert.that(continuation == null || continuation instanceof ContinuationImpl);
            final FDBRecordStore fdbRecordStore = Assert.notNull(schema.loadStore());
            final RecordCursor<QueryResult> cursor = plan.executePlan(fdbRecordStore,
                    evaluationContext,
                    continuation == null ? null : continuation.getBytes(), ExecuteProperties.newBuilder().build());

            return RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
        } else {
            return new ResumableIteratorImpl<>(Stream.of(plan.toString())
                            .map(ValueTuple::new)
                            .map(Row.class::cast)
                            .iterator(),
                    continuation);
        }
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP",
            justification = "class is internal implementation, this shouldn't escape proper usage")
    public String[] getFieldNames() {
        return expectedFieldNames;
    }
}
