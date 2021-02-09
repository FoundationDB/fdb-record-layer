/*
 * ListChildrenMatcher.java
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

import com.apple.foundationdb.record.query.plan.temp.Bindable;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A matcher for a children that tries to match each child to a specified matcher. It implements the "default" matching
 * for children, as exposed by the {@link TypeMatcher#of(Class, ExpressionMatcher[])} helper method.
 *
 * <p>
 * If every child matches the specified matcher and if every matcher matches a child, the {@link ListChildrenMatcher}
 * returns a stream of bindings that includes the Cartesian product of the stream of bindings from each child. Note that
 * this matcher can only match a fixed number of children. Currently, matching is not especially efficient: the cross
 * product is computed by collecting each child stream into a list.
 * </p>
 */
public class ListChildrenMatcher implements ExpressionChildrenMatcher {
    @Nonnull
    private static final ListChildrenMatcher EMPTY = new ListChildrenMatcher(Collections.emptyList());

    @Nonnull
    private final List<ExpressionMatcher<? extends Bindable>> childMatchers;

    private ListChildrenMatcher(@Nonnull List<ExpressionMatcher<? extends Bindable>> childMatchers) {
        this.childMatchers = childMatchers;
    }

    @Nonnull
    @Override
    @SuppressWarnings("java:S3958")
    public Stream<PlannerBindings> matches(@Nonnull final PlannerBindings outerBindings, @Nonnull List<? extends Bindable> children) {
        if (children.size() != childMatchers.size()) {
            return Stream.empty();
        }

        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());
        for (int i = 0; i < children.size(); i++) {
            final Bindable child = children.get(i);
            final ExpressionMatcher<? extends Bindable> matcher = childMatchers.get(i);
            List<PlannerBindings> possible = child.bindTo(outerBindings, matcher).collect(Collectors.toList());
            if (possible.isEmpty()) {
                return Stream.empty();
            }
            bindingStream = bindingStream.flatMap(existing -> possible.stream().map(existing::mergedWith));
        }

        return bindingStream;
    }

    /**
     * Get a matcher with no child matchers which matches an empty collection of children. The returned matcher may
     * be a static instance and may not be distinct across different calls. Unlike an {@link ExpressionMatcher},
     * an {@code ExpressionChildrenMatcher} is not used for creating bindings and so need not be a distinct object.
     * @return a matcher that matches an empty collection of children
     */
    @Nonnull
    public static ListChildrenMatcher empty() {
        return EMPTY;
    }

    /**
     * Get a matcher that tries to match the planner expression children, in order, to the given list of
     * {@code ExpressionMatcher}s.
     * @param childMatchers a list of matcher for the children, in order
     * @return a matcher that attempts to match the children, in order, to the given list of matchers
     */
    @Nonnull
    public static ListChildrenMatcher of(List<ExpressionMatcher<? extends Bindable>> childMatchers) {
        if (childMatchers.isEmpty()) {
            return empty();
        }
        return new ListChildrenMatcher(childMatchers);
    }
}
