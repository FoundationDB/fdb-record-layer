/*
 * QueryScannable.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Scannable that plans and executes a query.
 */
public class QueryScannable implements Scannable {
    private final RecordLayerSchema schema;
    private final RecordQueryPlan plan;

    private final String[] expectedFieldNames;
    private final boolean isExplain;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2",
            justification = "internal implemenation should have proper useage")
    public QueryScannable(RecordLayerSchema schema,
                          RecordQuery recordQuery,
                          String[] expectedFieldNames,
                          boolean isExplain) throws RelationalException {
        this.schema = schema;
        this.expectedFieldNames = expectedFieldNames;
        this.isExplain = isExplain;
        final FDBRecordStore fdbRecordStore = schema.loadStore();
        try {
            QueryPlanner planner = new CascadesPlanner(fdbRecordStore.getRecordMetaData(), fdbRecordStore.getRecordStoreState());
            // QueryPlanner planner = new RecordQueryPlanner(fdbRecordStore.getRecordMetaData(),fdbRecordStore.getRecordStoreState());
            final QueryPlanResult qpr = planner.planQuery(recordQuery);
            this.plan = qpr.getPlan();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public ResumableIterator<Row> openScan(@Nonnull Transaction transaction,
                                           @Nullable Row startKey,
                                           @Nullable Row endKey,
                                           @Nullable Continuation continuation,
                                           @Nonnull QueryProperties scanOptions) throws RelationalException {
        if (!isExplain) {
            assert continuation == null || continuation instanceof ContinuationImpl;
            final FDBRecordStore fdbRecordStore = schema.loadStore();
            final RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(fdbRecordStore,
                    EvaluationContext.empty(),
                    continuation == null ? null : continuation.getBytes(), ExecuteProperties.newBuilder().build());

            return RecordLayerIterator.create(
                    cursor,
                    messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getRecord()));
        } else {
            return new ResumableIteratorImpl<>(Stream.of(plan.toString())
                            .map(ValueTuple::new)
                            .map(Row.class::cast)
                            .iterator(),
                    continuation);
        }
    }

    @Override
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull QueryProperties scanOptions) throws RelationalException {
        throw new UnsupportedOperationException("Cannot perform point gets using a QueryScannable");
    }

    @Override
    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP",
            justification = "class is internal implementation, this shouldn't escape proper usage")
    public String[] getFieldNames() {
        return expectedFieldNames;
    }

    @Override
    public KeyBuilder getKeyBuilder() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public String getName() {
        return "query";
    }
}
