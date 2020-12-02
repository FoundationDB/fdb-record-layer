/*
 * AnyChildMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.Bindable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * An expression children matcher that tries to match any child to the given {@link ExpressionMatcher}, producing a
 * stream of bindings that is the concatenation of the (possibly empty) streams of bindings from attempting to match each
 * child to the given matcher.
 */
@API(API.Status.EXPERIMENTAL)
public class AnyChildMatcher implements ExpressionChildrenMatcher {
    @Nonnull
    private final ExpressionMatcher<? extends Bindable> childMatcher;

    private AnyChildMatcher(@Nonnull ExpressionMatcher<? extends Bindable> childMatcher) {
        this.childMatcher = childMatcher;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matches(@Nonnull final PlannerBindings outerBindings, @Nonnull List<? extends Bindable> children) {
        return children
                .stream()
                .flatMap(child -> child.bindTo(outerBindings, childMatcher));
    }

    /**
     * Get a matcher that tries to match any child with the given {@link ExpressionMatcher}.
     * @param childMatcher an expression matcher to match any one of the children
     * @return a matcher that tries to match any child with the given child matcher
     */
    @Nonnull
    public static AnyChildMatcher anyMatching(@Nonnull ExpressionMatcher<? extends Bindable> childMatcher) {
        return new AnyChildMatcher(childMatcher);
    }
}
