/*
 * LogicalUnorderedUnionExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A relational planner expression that represents an unimplemented unordered union of its children.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalUnorderedUnionExpression implements RelationalExpressionWithChildren {
    @Nonnull
    private List<Quantifier.ForEach> children;

    public LogicalUnorderedUnionExpression(@Nonnull List<ExpressionRef<RelationalExpression>> expressionChildren) {
        this.children = Quantifiers.forEachQuantifiers(expressionChildren);
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return children;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof LogicalUnorderedUnionExpression;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogicalUnorderedUnionExpression)) {
            return false;
        }
        final LogicalUnorderedUnionExpression that = (LogicalUnorderedUnionExpression)o;
        return children.equals(that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children);
    }
}
