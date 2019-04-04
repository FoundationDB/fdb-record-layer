/*
 * RecordQueryLoadByKeysPlan.java
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
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that returns records whose primary keys are taken from some list.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryLoadByKeysPlan implements RecordQueryPlanWithNoChildren {
    @Nonnull
    private final ExpressionRef<KeysSource> keysSource;

    public RecordQueryLoadByKeysPlan(@Nonnull KeysSource keysSource) {
        this.keysSource = SingleExpressionRef.of(keysSource);
    }

    public RecordQueryLoadByKeysPlan(@Nonnull List<Tuple> primaryKeys) {
        this(new PrimaryKeysKeySource(primaryKeys));
    }

    public RecordQueryLoadByKeysPlan(@Nonnull String parameter) {
        this(new ParameterKeySource(parameter));
    }
                                                                                
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        // Cannot pass down limit(s) because we skip keys that don't load.
        RecordScanLimiter recordScanLimiter = executeProperties.getState().getRecordScanLimiter();
        return RecordCursor.fromList(store.getExecutor(), getKeysSource().getPrimaryKeys(context), continuation)
                .mapPipelined(key -> {
                    // TODO: Implement continuation handling and record scan limit for RecordQueryLoadByKeysPlan (https://github.com/FoundationDB/fdb-record-layer/issues/6)
                    if (recordScanLimiter != null) {
                        recordScanLimiter.tryRecordScan();
                    }
                    return store.loadRecordAsync(key);
                }, store.getPipelineSize(PipelineOperation.KEY_TO_RECORD))
                .filter(Objects::nonNull)
                .map(store::queriedRecord)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return false;
    }

    @Nonnull
    public KeysSource getKeysSource() {
        return keysSource.get();
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return new HashSet<>();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.keysSource);
    }

    @Nonnull
    @Override
    public String toString() {
        return "ByKeys(" + getKeysSource() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryLoadByKeysPlan that = (RecordQueryLoadByKeysPlan) o;
        return Objects.equals(getKeysSource(), that.getKeysSource());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeysSource());
    }

    @Override
    public int planHash() {
        return getKeysSource().planHash();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_LOAD_BY_KEYS);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    private static class PrimaryKeysKeySource implements KeysSource {
        private final List<Tuple> primaryKeys;

        public PrimaryKeysKeySource(List<Tuple> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        @Override
        public List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context) {
            return primaryKeys;
        }

        @Nonnull
        @Override
        @API(API.Status.EXPERIMENTAL)
        public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
            return Collections.emptyIterator();
        }

        @Override
        public String toString() {
            return primaryKeys.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimaryKeysKeySource that = (PrimaryKeysKeySource) o;
            return Objects.equals(primaryKeys, that.primaryKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryKeys);
        }

        @Override
        public int planHash() {
            return hashCode();
        }
    }

    /**
     * A source for the primary keys for records.
     */
    public interface KeysSource extends PlanHashable, PlannerExpression {
        List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context);
    }

    private static class ParameterKeySource implements KeysSource {
        private final String parameter;

        public ParameterKeySource(String parameter) {
            this.parameter = parameter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context) {
            return (List<Tuple>)context.getBinding(parameter);
        }

        @Nonnull
        @Override
        @API(API.Status.EXPERIMENTAL)
        public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
            return Collections.emptyIterator();
        }

        @Override
        public String toString() {
            return "$" + parameter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ParameterKeySource that = (ParameterKeySource) o;
            return Objects.equals(parameter, that.parameter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parameter);
        }

        @Override
        public int planHash() {
            return hashCode();
        }
    }
}
