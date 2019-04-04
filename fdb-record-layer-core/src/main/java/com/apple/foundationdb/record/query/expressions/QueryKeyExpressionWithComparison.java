/*
 * QueryKeyExpressionWithComparison.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * A {@link QueryComponent} that implements a {@link Comparisons.Comparison} against a {@link QueryableKeyExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryKeyExpressionWithComparison implements ComponentWithComparison {
    @Nonnull
    private final QueryableKeyExpression keyExpression;
    @Nonnull
    private final Comparisons.Comparison comparison;

    public QueryKeyExpressionWithComparison(@Nonnull QueryableKeyExpression keyExpression, @Nonnull Comparisons.Comparison comparison) {
        this.keyExpression = keyExpression;
        this.comparison = comparison;
    }

    @Nonnull
    public QueryableKeyExpression getKeyExpression() {
        return keyExpression;
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        return getComparison().eval(store, context, keyExpression.evalForQuery(store, context, record, message));
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        keyExpression.validate(descriptor);
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return this.comparison;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return keyExpression + " " + getComparison();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryKeyExpressionWithComparison that = (QueryKeyExpressionWithComparison) o;
        return Objects.equals(keyExpression, that.keyExpression) &&
               Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyExpression, getComparison());
    }

    @Override
    public int planHash() {
        return keyExpression.planHash() + getComparison().planHash();
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new QueryKeyExpressionWithComparison(keyExpression, comparison);
    }

    @Override
    public String getName() {
        return keyExpression.getName();
    }
}
