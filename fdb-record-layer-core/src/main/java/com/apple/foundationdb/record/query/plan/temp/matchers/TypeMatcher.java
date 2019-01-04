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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * A matcher that is specified by the class of its expression: to match any of several types of expressions, they must all
 * implement a common interface (which itself extends <code>PlannerExpression</code>). The bindings produced by this
 * matcher provide access to a single expression only, and allow it to be accessed by the rule.
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypeMatcher<T extends PlannerExpression> implements ExpressionMatcher<T> {
    @Nonnull
    private final Class<? extends T> expressionClass;
    @Nonnull
    private final List<ExpressionMatcher<? extends Bindable>> children;

    private TypeMatcher(@Nonnull Class<? extends T> expressionClass,
                        @Nonnull List<ExpressionMatcher<? extends Bindable>> children) {
        this.expressionClass = expressionClass;
        this.children = children;
    }

    @Override
    @Nonnull
    public Class<? extends T> getRootClass() {
        return expressionClass;
    }

    @Override
    @Nonnull
    public List<ExpressionMatcher<? extends Bindable>> getChildren() {
        return children;
    }

    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass) {
        return new TypeMatcher<>(expressionClass, Collections.emptyList());
    }

    @SafeVarargs
    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                                  @Nonnull ExpressionMatcher<? extends Bindable>... children) {
        ImmutableList.Builder<ExpressionMatcher<? extends Bindable>> builder = ImmutableList.builder();
        for (ExpressionMatcher<? extends Bindable> child : children) {
            builder.add(child);
        }
        return of(expressionClass, builder.build());
    }

    public static <U extends PlannerExpression> TypeMatcher<U> of(@Nonnull Class<? extends U> expressionClass,
                                                                  @Nonnull List<ExpressionMatcher<? extends Bindable>> children) {
        return new TypeMatcher<>(expressionClass, children);
    }

    @Override
    public Result matches(@Nonnull Bindable bindable) {
        if (bindable instanceof ExpressionRef) {
            return Result.UNKNOWN;
        }
        if (expressionClass.isInstance(bindable)) {
            return Result.MATCHES;
        } else {
            return Result.DOES_NOT_MATCH;
        }
    }
}
