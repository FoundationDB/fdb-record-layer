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
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * A matcher that is specified by the class of its expression: to match any of several types of expressions, they must all
 * implement a common interface (which itself extends <code>PlannerExpression</code>). The bindings produced by this
 * matcher provide access to a single expression only, and allow it to be accessed by the rule.
 * @param <T> the planner expression type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypeMatcher<T extends PlannerExpression> implements ExpressionMatcher<T> {
    @Nonnull
    private final Class<? extends T> expressionClass;
    @Nonnull
    private final ExpressionChildrenMatcher childrenMatcher;

    private TypeMatcher(@Nonnull Class<? extends T> expressionClass,
                        @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        this.expressionClass = expressionClass;
        this.childrenMatcher = childrenMatcher;
    }

    @Override
    @Nonnull
    public Class<? extends T> getRootClass() {
        return expressionClass;
    }

    @Override
    @Nonnull
    public ExpressionChildrenMatcher getChildrenMatcher() {
        return childrenMatcher;
    }

    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass) {
        return new TypeMatcher<>(expressionClass, ListChildrenMatcher.empty());
    }

    @SafeVarargs
    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                                  @Nonnull ExpressionMatcher<? extends Bindable>... children) {
        ImmutableList.Builder<ExpressionMatcher<? extends Bindable>> builder = ImmutableList.builder();
        for (ExpressionMatcher<? extends Bindable> child : children) {
            builder.add(child);
        }
        return of(expressionClass, ListChildrenMatcher.of(builder.build()));
    }

    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                                  @Nonnull ExpressionChildrenMatcher childrenMatcher) {
        return new TypeMatcher<>(expressionClass, childrenMatcher);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        // A type matcher will never match a reference. Ask the reference whether its contents match properly.
        return ref.bindWithin(this);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matchWith(@Nonnull PlannerExpression expression) {
        if (expressionClass.isInstance(expression)) {
            return Stream.of(PlannerBindings.from(this, expression));
        } else {
            return Stream.empty();
        }
    }
}
