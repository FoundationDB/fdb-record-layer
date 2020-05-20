/*
 * LogicalTypeFilterExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A relational planner expression that represents an unimplemented type filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalTypeFilterExpression implements TypeFilterExpression {
    @Nonnull
    private final Set<String> recordTypes;
    @Nonnull
    private final Quantifier.ForEach inner;

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull RelationalExpression inner) {
        this(recordTypes, GroupExpressionRef.of(inner));
    }

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull ExpressionRef<RelationalExpression> inner) {
        this.recordTypes = recordTypes;
        this.inner = Quantifier.forEach(inner);
    }

    @Override
    @Nonnull
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    @Nonnull
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    private Quantifier getInner() {
        return inner;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof LogicalTypeFilterExpression &&
               ((LogicalTypeFilterExpression)otherExpression).getRecordTypes().equals(getRecordTypes());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogicalTypeFilterExpression)) {
            return false;
        }
        final LogicalTypeFilterExpression that = (LogicalTypeFilterExpression)o;
        return getRecordTypes().equals(that.getRecordTypes()) &&
               getInner().equals(that.getInner());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRecordTypes(), getInner());
    }
}
