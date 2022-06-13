/*
 * QueryComponentPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link QueryPredicate} that is satisfied when its child component is satisfied. This class represents an
 * interim solution to be able to evaluate a {@link QueryComponent} but in the context of a predicate. Effectively,
 * this class bridges the old planner constructs and new planner constructs. We may need to evaluate
 * {@link QueryComponent}s when we need to compensate for partial matches during index matching as
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression} and others do not have a runtime
 * implementation yet.
 * TODO remove this class eventually
 */
@API(API.Status.EXPERIMENTAL)
public class QueryComponentPredicate implements LeafQueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Predicate-With-Query-Component");

    @Nonnull
    public final QueryComponent queryComponent;

    @Nonnull
    public final CorrelationIdentifier correlation;

    public QueryComponentPredicate(@Nonnull QueryComponent queryComponent, @Nonnull CorrelationIdentifier correlation) {
        this.queryComponent = queryComponent;
        this.correlation = correlation;
    }

    @Nonnull
    public QueryComponent getQueryComponent() {
        return queryComponent;
    }

    public boolean hasAsyncQueryComponent() {
        return queryComponent.isAsync();
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        final var result = (QueryResult)context.getBinding(correlation);
        if (result.getDatum() == null) {
            return null;
        }

        final var record = result.<M>getQueriedRecordMaybe().orElse(null);
        // this predicate must be expressed over a record
        final var message = result.<M>getMessage();
        return queryComponent.evalMessage(store, context, record, message);
    }

    public <M extends Message> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        final var result = (QueryResult)context.getBinding(correlation);
        if (result.getDatum() == null) {
            return null;
        }

        final var record = result.<M>getQueriedRecordMaybe().orElse(null);
        // this predicate must be expressed over a record
        final var message = result.<M>getMessage();
        return queryComponent.evalMessageAsync(store, context, record, message);
    }

    @Override
    public String toString() {
        return "QueryComponent(" + getQueryComponent() + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!LeafQueryPredicate.super.equalsWithoutChildren(other, equivalenceMap)) {
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
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return queryComponent.planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH, queryComponent);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of(correlation);
    }

    @Nullable
    @Override
    public QueryComponentPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        if (translationMap.containsSourceAlias(correlation)) {
            return new QueryComponentPredicate(getQueryComponent(), translationMap.getTargetAlias(correlation));
        }
        return this;
    }
}
