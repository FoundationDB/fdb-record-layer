/*
 * ElementPredicate.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link QueryPredicate} that evaluates a {@link Comparisons.Comparison} on the value of an {@link Element}.
 *
 * <p>
 * An element predicate is a "leaf" in a predicate tree. It pairs an element, which can be evaluated with respect to
 * a given {@link SourceEntry}, with a comparison. An element predicate evalutes to the result of the comparison on the
 * value of the element.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ElementPredicate implements QueryPredicate {
    @Nonnull
    private final Element element;
    @Nonnull
    private final Comparisons.Comparison comparison;

    public ElementPredicate(@Nonnull Element element, @Nonnull Comparisons.Comparison comparison) {
        this.element = element;
        this.comparison = comparison;
    }

    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Nonnull
    public Element getElement() {
        return element;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                            @Nonnull SourceEntry sourceEntry) {
        return comparison.eval(store, context, element.eval(sourceEntry));
    }

    @Override
    @Nonnull
    public Stream<PlannerBindings> bindTo(@Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(this, ImmutableList.of());
    }

    @Override
    public String toString() {
        return element.toString() + " " + comparison.toString();
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
        ElementPredicate that = (ElementPredicate)other;
        return element.semanticEquals(that.element, aliasMap) &&
               Objects.equals(comparison, that.comparison);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(element, comparison);
    }

    @Override
    public int planHash() {
        return element.planHash() + comparison.planHash();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        // TODO needs to return a correlation identifier synthesized from the element
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public ElementPredicate rebase(@Nonnull final AliasMap translationMap) {
        // TODO needs to create a new element to refer to the translated correlation identifier
        return new ElementPredicate(getElement(), getComparison());
    }
}
