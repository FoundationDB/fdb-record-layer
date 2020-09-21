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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
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
    @Nonnull
    private final CorrelationIdentifier existentialAlias;

    public ExistsPredicate(@Nonnull final CorrelationIdentifier existentialAlias) {
        this.existentialAlias = existentialAlias;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nonnull final SourceEntry sourceEntry) {
        // TODO not sure how to execute this
        return null;
    }

    @Nonnull
    public CorrelationIdentifier getExistentialAlias() {
        return existentialAlias;
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
            return new ExistsPredicate(translationMap.getTargetOrThrow(existentialAlias));
        } else {
            return this;
        }
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(this);
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
    public int semanticHashCode() {
        return planHash();
    }

    @Override
    public int planHash() {
        return 29;
    }

    @Override
    public String toString() {
        return "∃" + existentialAlias;
    }
}
