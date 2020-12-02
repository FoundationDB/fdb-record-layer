/*
 * AndOrPredicate.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common base class for predicates with many children, such as {@link AndPredicate} and {@link OrPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AndOrPredicate implements QueryPredicate {
    @Nonnull
    private final List<QueryPredicate> children;

    protected AndOrPredicate(@Nonnull List<QueryPredicate> children) {
        if (children.size() < 2) {
            throw new RecordCoreException(getClass().getSimpleName() + " must have at least two children");
        }

        this.children = children;
    }

    @Nonnull
    public List<QueryPredicate> getChildren() {
        return children;
    }

    @Override
    @Nonnull
    public Stream<PlannerBindings> bindTo(@Nonnull final PlannerBindings outerBindings, @Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(outerBindings, this, getChildren());
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        for (final QueryPredicate child : getChildren()) {
            builder.addAll(child.getCorrelatedTo());
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public AndOrPredicate rebase(@Nonnull final AliasMap translationMap) {
        return rebaseWithRebasedChildren(translationMap,
                getChildren().stream()
                        .map(child -> child.rebase(translationMap))
                        .collect(Collectors.toList()));
    }

    public abstract AndOrPredicate rebaseWithRebasedChildren(final AliasMap translationMap,
                                                             final List<QueryPredicate> rebasedChildren);

    @Override
    @SuppressWarnings({"squid:S1206", "EqualsWhichDoesntCheckParameterClass"})
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(ImmutableSet.of(getChildren()));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public boolean semanticEquals(@Nullable final Object other,
                                  @Nonnull final AliasMap aliasMap) {
        if (other == null) {
            return false;
        }

        if (this == other) {
            return true;
        }

        if (!(other instanceof AndOrPredicate)) {
            return false;
        }

        final AndOrPredicate otherAndOrPred = (AndOrPredicate)other;
        if (!equalsWithoutChildren(otherAndOrPred, aliasMap)) {
            return false;
        }

        final List<QueryPredicate> preds = getChildren();
        final List<QueryPredicate> otherPreds = otherAndOrPred.getChildren();
        if (preds.size() != otherPreds.size()) {
            return false;
        }
        return Streams
                .zip(preds.stream(), otherPreds.stream(), Pair::of)
                .allMatch(pair -> pair.getLeft().semanticEquals(pair.getRight(), aliasMap));
    }

    @SuppressWarnings({"squid:S1172", "unused"})
    protected boolean equalsWithoutChildren(@Nonnull final AndOrPredicate other,
                                            @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        return other.getClass() == getClass();
    }
}
