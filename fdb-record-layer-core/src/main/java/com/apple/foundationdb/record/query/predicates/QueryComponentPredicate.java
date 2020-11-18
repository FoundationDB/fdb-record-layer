/*
 * NotPredicate.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link QueryPredicate} that is satisfied when its child component is satisfied.
 *
 * For tri-valued logic, if the child evaluates to unknown / {@code null}, {@code NOT} is still unknown.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryComponentPredicate implements QueryPredicate {
    @Nonnull
    public final QueryComponent queryComponent;

    public QueryComponentPredicate(@Nonnull QueryComponent queryComponent) {
        this.queryComponent = queryComponent;
    }

    @Nonnull
    public QueryComponent getQueryComponent() {
        return queryComponent;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return queryComponent.evalMessage(store, context, record, message);
    }

    @Override
    @Nonnull
    public Stream<PlannerBindings> bindTo(@Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(this, ImmutableList.of());
    }

    @Override
    public String toString() {
        return "QueryComponent(" + getQueryComponent() + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final QueryComponentPredicate that = (QueryComponentPredicate)other;
        return getQueryComponent().equals(that.getQueryComponent());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(getQueryComponent());
    }

    @Override
    public int planHash() {
        return getQueryComponent().planHash() + 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public QueryComponentPredicate rebase(@Nonnull final AliasMap translationMap) {
        return new QueryComponentPredicate(getQueryComponent());
    }
}
