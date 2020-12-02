/*
 * TypeWithPredicateMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Matches a subclass of {@link RelationalExpressionWithPredicate} with a given predicate (as determined by
 * {@link RelationalExpressionWithPredicate#getPredicate()} and a given matcher against the children.
 * @param <T> the type of {@link RelationalExpressionWithPredicate} to match against
 */
public class TypeWithPredicateMatcher<T extends RelationalExpressionWithPredicate> extends TypeMatcher<T> {
    @Nonnull
    private final ExpressionMatcher<? extends QueryPredicate> predicateMatcher;

    private TypeWithPredicateMatcher(@Nonnull Class<? extends T> expressionClass,
                                     @Nonnull ExpressionMatcher<? extends QueryPredicate> predicateMatcher,
                                     @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        super(expressionClass, childrenMatcher);
        this.predicateMatcher = predicateMatcher;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull final PlannerBindings outerBindings, @Nonnull final RelationalExpression expression, @Nonnull final List<? extends Bindable> children) {
        if (!(expression instanceof RelationalExpressionWithPredicate)) {
            return Stream.empty();
        }
        Stream<PlannerBindings> superBindings = super.matchWith(outerBindings, expression, children);
        QueryPredicate predicate = ((RelationalExpressionWithPredicate)expression).getPredicate();
        return superBindings.flatMap(bindings -> predicate.bindTo(outerBindings, predicateMatcher).map(bindings::mergedWith));
    }

    public static <U extends RelationalExpressionWithPredicate> TypeWithPredicateMatcher<U> ofPredicate(@Nonnull Class<? extends U> expressionClass,
                                                                                                        @Nonnull ExpressionMatcher<? extends QueryPredicate> predicateMatcher) {
        return ofPredicate(expressionClass, predicateMatcher, AnyChildrenMatcher.ANY);
    }

    @SafeVarargs
    public static <U extends RelationalExpressionWithPredicate> TypeWithPredicateMatcher<U> ofPredicate(@Nonnull Class<? extends U> expressionClass,
                                                                                                        @Nonnull ExpressionMatcher<? extends QueryPredicate> predicateMatcher,
                                                                                                        @Nonnull ExpressionMatcher<? extends Bindable>... children) {
        ImmutableList.Builder<ExpressionMatcher<? extends Bindable>> builder = ImmutableList.builder();
        for (ExpressionMatcher<? extends Bindable> child : children) {
            builder.add(child);
        }
        return ofPredicate(expressionClass, predicateMatcher, ListChildrenMatcher.of(builder.build()));
    }

    public static <U extends RelationalExpressionWithPredicate> TypeWithPredicateMatcher<U> ofPredicate(@Nonnull Class<? extends U> expressionClass,
                                                                                                        @Nonnull ExpressionMatcher<? extends QueryPredicate> predicateMatcher,
                                                                                                        @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        return new TypeWithPredicateMatcher<>(expressionClass, predicateMatcher, childrenMatcher);
    }
}
