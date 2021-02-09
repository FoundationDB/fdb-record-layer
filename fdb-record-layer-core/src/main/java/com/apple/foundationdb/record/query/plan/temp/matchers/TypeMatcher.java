/*
 * TypeMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * A matcher that is specified by the class of its expression: to match any of several types of expressions, they must all
 * implement a common interface (which itself extends <code>PlannerExpression</code>). The bindings produced by this
 * matcher provide access to a single expression only, and allow it to be accessed by the rule.
 * @param <T> the planner expression type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypeMatcher<T extends Bindable> implements ExpressionMatcher<T> {
    @Nonnull
    private final Class<? extends T> bindableClass;
    @Nonnull
    private final ExpressionChildrenMatcher childrenMatcher;

    protected TypeMatcher(@Nonnull Class<? extends T> bindableClass,
                          @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        this.bindableClass = bindableClass;
        this.childrenMatcher = childrenMatcher;
    }

    @Override
    @Nonnull
    public Class<? extends Bindable> getRootClass() {
        return bindableClass;
    }

    @Nonnull
    public ExpressionChildrenMatcher getChildrenMatcher() {
        return childrenMatcher;
    }

    public static <U extends Bindable> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass) {
        return new TypeMatcher<>(expressionClass, ListChildrenMatcher.empty());
    }

    @SafeVarargs
    public static <U extends Bindable> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                         @Nonnull ExpressionMatcher<? extends Bindable>... children) {
        ImmutableList.Builder<ExpressionMatcher<? extends Bindable>> builder = ImmutableList.builder();
        for (ExpressionMatcher<? extends Bindable> child : children) {
            builder.add(child);
        }
        return of(expressionClass, ListChildrenMatcher.of(builder.build()));
    }

    public static <U extends Bindable> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                         @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        return new TypeMatcher<>(expressionClass, childrenMatcher);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings,
                                             @Nonnull final ExpressionRef<? extends RelationalExpression> ref,
                                             @Nonnull final List<? extends Bindable> children) {
        // A type matcher will never match a reference. Ask the reference whether its contents match properly.
        return ref.bindWithin(outerBindings, this);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull RelationalExpression expression, @Nonnull final List<? extends Bindable> children) {
        return matchClassWith(expression)
                .flatMap(bindings ->
                        getChildrenMatcher()
                                .matches(outerBindings, children)
                                .map(bindings::mergedWith));
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull QueryPredicate predicate, @Nonnull final List<? extends Bindable> children) {
        return matchClassWith(predicate)
                .flatMap(bindings ->
                        getChildrenMatcher()
                                .matches(outerBindings, children)
                                .map(bindings::mergedWith));
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull final Quantifier quantifier, @Nonnull final List<? extends Bindable> children) {
        return matchClassWith(quantifier)
                .flatMap(bindings ->
                        getChildrenMatcher()
                                .matches(outerBindings, children)
                                .map(bindings::mergedWith));
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull final PartialMatch partialMatch, @Nonnull final List<? extends Bindable> children) {
        return matchClassWith(partialMatch)
                .flatMap(bindings ->
                        getChildrenMatcher()
                                .matches(outerBindings, children)
                                .map(bindings::mergedWith));
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull final MatchPartition matchPartition, @Nonnull final List<? extends Bindable> children) {
        return matchClassWith(matchPartition)
                .flatMap(bindings ->
                        getChildrenMatcher()
                                .matches(outerBindings, children)
                                .map(bindings::mergedWith));
    }

    @Nonnull
    private Stream<PlannerBindings> matchClassWith(final Bindable bindable) {
        if (bindableClass.isInstance(bindable)) {
            return Stream.of(PlannerBindings.from(this, bindable));
        } else {
            return Stream.empty();
        }
    }
}
