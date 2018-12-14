/*
 * QueryRecordFunctionWithComparison.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} whose value from the record,
 * to be compared with the comparison's value, is the result of applying a {@link RecordFunction} to the record.
 */
public class QueryRecordFunctionWithComparison implements ComponentWithComparison {
    @Nonnull
    private final RecordFunction<?> function;
    @Nonnull
    private final ExpressionRef<Comparisons.Comparison> comparison;

    public QueryRecordFunctionWithComparison(@Nonnull RecordFunction<?> function, @Nonnull Comparisons.Comparison comparison) {
        this.function = function;
        this.comparison = SingleExpressionRef.of(comparison);
    }

    @Nonnull
    public RecordFunction<?> getFunction() {
        return function;
    }

    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison.get();
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new QueryRecordFunctionWithComparison(function, comparison);
    }

    @Override
    public String getName() {
        return function.toString();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBEvaluationContext<M> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        return evalMessageAsync(context, record, message).join();
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBEvaluationContext<M> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (record == null) {
            return CompletableFuture.completedFuture(getComparison().eval(context, null));
        }
        return context.evaluateRecordFunction(function, record).thenApply(value -> getComparison().eval(context, value));
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        function.validate(descriptor);
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.comparison);
    }

    @Override
    public String toString() {
        return function.toString() + " " + getComparison();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryRecordFunctionWithComparison that = (QueryRecordFunctionWithComparison) o;
        return Objects.equals(function, that.function) &&
               Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, getComparison());
    }

    @Override
    public int planHash() {
        return function.planHash() + getComparison().planHash();
    }
}
