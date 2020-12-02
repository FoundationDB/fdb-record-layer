/*
 * ExistsPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An existential predicate that is true if the inner correlation produces any values, and false otherwise.
 */
@API(API.Status.EXPERIMENTAL)
public class ExistsPredicate implements QueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Predicate");
    @Nonnull
    private final CorrelationIdentifier existentialAlias;

    @Nonnull
    private final QueryComponent alternativeComponent;

    public ExistsPredicate(@Nonnull final CorrelationIdentifier existentialAlias, @Nonnull final QueryComponent alternativeComponent) {
        this.existentialAlias = existentialAlias;
        this.alternativeComponent = alternativeComponent;
    }

    @Nonnull
    public CorrelationIdentifier getExistentialAlias() {
        return existentialAlias;
    }


    @Nonnull
    public QueryComponent getAlternativeComponent() {
        return alternativeComponent;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        throw new RecordCoreException("this predicate cannot be evaluated per record");
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return Collections.singleton(existentialAlias);
    }

    @Nonnull
    @Override
    public ExistsPredicate rebase(@Nonnull final AliasMap translationMap) {
        if (translationMap.containsSource(existentialAlias)) {
            return new ExistsPredicate(translationMap.getTargetOrThrow(existentialAlias), alternativeComponent);
        } else {
            return this;
        }
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final PlannerBindings outerBindings, @Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(outerBindings, this, ImmutableList.of());
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
        final ExistsPredicate that = (ExistsPredicate)other;
        return aliasMap.containsMapping(existentialAlias, that.existentialAlias);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return planHash();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "∃" + existentialAlias;
    }
}
