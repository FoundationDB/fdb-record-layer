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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.Set;

/**
 * A relational planner expression that represents an unimplemented unordered union of its children.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalUnorderedUnionExpression implements RelationalExpressionWithChildren {
    @Nonnull
    private List<? extends Quantifier> quantifiers;

    public LogicalUnorderedUnionExpression(@Nonnull List<? extends Quantifier> quantifiers) {
        this.quantifiers = quantifiers;
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalUnorderedUnionExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalUnorderedUnionExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public LogicalUnorderedUnionExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                        @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new LogicalUnorderedUnionExpression(rebasedQuantifiers);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        return getClass() == otherExpression.getClass();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return 31;
    }
}
