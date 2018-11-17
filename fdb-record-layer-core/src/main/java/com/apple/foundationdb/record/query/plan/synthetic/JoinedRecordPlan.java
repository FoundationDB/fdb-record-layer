/*
 * JoinedRecordPlan.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.EvaluationContextBuilder;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Execute a stack of join queries starting with a stored {@link com.apple.foundationdb.record.metadata.RecordType}
 * and generating {@link JoinedRecordType} {@link FDBSyntheticRecord records}.
 */
@API(API.Status.EXPERIMENTAL)
public class JoinedRecordPlan implements SyntheticRecordFromStoredRecordPlan  {

    @Nonnull
    private final JoinedRecordType joinedRecordType;
    @Nonnull
    private final List<JoinedType> joinedTypes;
    @Nonnull
    private final List<RecordQueryPlan> queries;

    protected static class JoinedType implements PlanHashable {
        @Nonnull
        protected final JoinedRecordType.JoinConstituent constituent;
        @Nonnull
        protected final List<Binding> bindings;

        public JoinedType(@Nonnull JoinedRecordType.JoinConstituent constituent, @Nonnull List<Binding> bindings) {
            this.constituent = constituent;
            this.bindings = bindings;
        }

        public <M extends Message> EvaluationContext bind(@Nonnull EvaluationContext context, @Nullable FDBStoredRecord<M> record) {
            EvaluationContextBuilder builder = context.childBuilder();
            builder.setBinding(constituent.getName(), record);
            for (Binding binding : bindings) {
                builder.setBinding(binding.name, binding.evaluate(record));
            }
            return builder.build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JoinedType that = (JoinedType)o;
            return Objects.equals(constituent, that.constituent) &&
                   Objects.equals(bindings, that.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constituent, bindings);
        }

        @Override
        public int planHash() {
            return constituent.getName().hashCode() +
                   PlanHashable.planHash(bindings);
        }
    }

    protected static class Binding implements PlanHashable {
        @Nonnull
        protected final String name;
        @Nonnull
        protected final KeyExpression expression;
        protected final boolean singleton;

        public Binding(@Nonnull String name, @Nonnull KeyExpression expression, boolean singleton) {
            this.name = name;
            this.expression = expression;
            this.singleton = singleton;
        }

        public <M extends Message> Object evaluate(@Nullable FDBStoredRecord<M> record) {
            if (singleton) {
                return toValue(expression.evaluateSingleton(record));
            } else {
                return expression.evaluate(record).stream().map(Binding::toValue).collect(Collectors.toList());
            }
        }

        protected static Object toValue(@Nonnull Key.Evaluated evaluated) {
            if (evaluated.size() != 1) {
                throw new RecordCoreException("binding expression should evaluate to scalar values");
            }
            return evaluated.getObject(0);
        }

        @Override
        public String toString() {
            return name + ":" + expression;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Binding binding = (Binding)o;
            return singleton == binding.singleton &&
                   Objects.equals(name, binding.name) &&
                   Objects.equals(expression, binding.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, expression, singleton);
        }

        @Override
        public int planHash() {
            return name.hashCode() + expression.planHash() + (singleton ? 1 : 0);
        }
    }

    public JoinedRecordPlan(@Nonnull JoinedRecordType joinedRecordType, @Nonnull List<JoinedType> joinedTypes, @Nonnull List<RecordQueryPlan> queries) {
        if (joinedTypes.size() != joinedRecordType.getConstituents().size()) {
            throw new RecordCoreArgumentException("should join all constituents");
        }
        for (JoinedType joinedType : joinedTypes) {
            if (!joinedRecordType.getConstituents().contains(joinedType.constituent)) {
                throw new RecordCoreArgumentException("constituent " + joinedType.constituent + " does not come from joined record type");
            }
        }
        if (queries.size() != joinedTypes.size() - 1) {
            throw new RecordCoreArgumentException("should have one query for each join");
        }

        this.joinedRecordType = joinedRecordType;
        this.joinedTypes = joinedTypes;
        this.queries = queries;
    }

    @Override
    @Nonnull
    public Set<String> getStoredRecordTypes() {
        return Collections.singleton(joinedTypes.get(0).constituent.getRecordType().getName());
    }

    @Override
    @Nonnull
    public Set<String> getSyntheticRecordTypes() {
        return Collections.singleton(joinedRecordType.getName());
    }

    @Override
    @Nonnull
    public <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                                        @Nonnull FDBStoredRecord<M> record,
                                                                        @Nullable byte[] continuation,
                                                                        @Nonnull ExecuteProperties executeProperties) {
        final EvaluationContext context = joinedTypes.get(0).bind(EvaluationContext.EMPTY, record);
        final RecordCursor<EvaluationContext> joinedContexts;
        if (queries.size() == 1) {
            joinedContexts = query(0, store, context, continuation, executeProperties);
        } else {
            final ExecuteProperties baseProperties = executeProperties.clearSkipAndLimit();
            joinedContexts = nest(0, store.getPipelineSize(PipelineOperation.SYNTHETIC_RECORD_JOIN), store, context, continuation, baseProperties)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        }
        return joinedContexts.map(this::toSyntheticRecord);
    }

    private RecordCursor<EvaluationContext> query(int depth, @Nonnull FDBRecordStore store, @Nonnull EvaluationContext context,
                                                  @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        RecordCursor<FDBQueriedRecord<Message>> records = queries.get(depth).execute(store, context, continuation, executeProperties);
        final JoinedType joinedType = joinedTypes.get(depth + 1);
        if (joinedType.constituent.isOuterJoined()) {
            records = records.orElse(executor -> RecordCursor.fromFuture(executor, CompletableFuture.completedFuture(null)));
        }
        return records.map(qr -> joinedType.bind(context, qr == null ? null : qr.getStoredRecord()));
    }

    private RecordCursor<EvaluationContext> nest(int depth, int pipelineSize, @Nonnull FDBRecordStore store, @Nonnull EvaluationContext context,
                                                 @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        if (depth == queries.size() - 1) {
            return query(depth, store, context, continuation, executeProperties);
        } else {
            return RecordCursor.flatMapPipelined(
                    outerContinuation -> query(depth, store, context, outerContinuation, executeProperties),
                    (innerContext, innerContination) -> nest(depth + 1, pipelineSize, store, innerContext, innerContination, executeProperties),
                    continuation, pipelineSize);
        }
    }

    private FDBSyntheticRecord toSyntheticRecord(@Nonnull EvaluationContext context) {
        final Map<String, FDBStoredRecord<? extends Message>> records = new HashMap<>();
        for (JoinedRecordType.JoinConstituent joinConstituent : joinedRecordType.getConstituents()) {
            records.put(joinConstituent.getName(), (FDBStoredRecord<? extends Message>)context.getBinding(joinConstituent.getName()));
        }
        return FDBSyntheticRecord.of(joinedRecordType, records);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < joinedTypes.size(); i++) {
            JoinedType joinedType = joinedTypes.get(i);
            str.append(joinedType.constituent.getName());
            str.append(":");
            if (i == 0) {
                str.append("?");
            } else {
                str.append(queries.get(i - 1));
            }
            for (Binding binding : joinedType.bindings) {
                str.append(", ").append(binding);
            }
            str.append(" => ");
        }
        str.append(joinedRecordType.getName());
        return str.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinedRecordPlan that = (JoinedRecordPlan)o;
        return Objects.equals(joinedRecordType, that.joinedRecordType) &&
               Objects.equals(joinedTypes, that.joinedTypes) &&
               Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinedRecordType, joinedTypes, queries);
    }

    @Override
    public int planHash() {
        return joinedRecordType.getName().hashCode() +
               PlanHashable.planHash(joinedTypes) +
               PlanHashable.planHash(queries);
    }
}
