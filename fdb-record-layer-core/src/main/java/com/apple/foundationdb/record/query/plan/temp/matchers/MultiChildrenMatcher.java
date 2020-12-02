/*
 * MultiChildrenMatcher.java
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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An expression children matcher that tries to match all children to a single {@link ExpressionMatcher}.
 *
 * Given a single {@code ExpressionMatcher}, this matcher tries to match it to every child. If it matches all of them, it
 * produces a stream of bindings containing the Cartesian product of the streams of bindings from each child, merged
 * using {@link PlannerBindings#mergedWith(PlannerBindings)}. Because the same matcher is used for all children, the
 * merged bindings will map the single child matcher to a collected list of {@link Bindable}s; such a binding must be
 * retrieved using {@link PlannerBindings#getAll(ExpressionMatcher)} rather than the usual {@code get()} method.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class MultiChildrenMatcher implements ExpressionChildrenMatcher {
    @Nonnull
    private final ExpressionMatcher<? extends Bindable> childMatcher;

    private MultiChildrenMatcher(@Nonnull ExpressionMatcher<? extends Bindable> childMatcher) {
        this.childMatcher = childMatcher;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matches(@Nonnull final PlannerBindings outerBindings, @Nonnull List<? extends Bindable> children) {
        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());

        // The children need to be merged in the same order that they appear to satisfy the contract of
        // PlannerBindings.getAll().
        for (final Bindable child : children) {
            final List<PlannerBindings> individualBindings = child.bindTo(outerBindings, childMatcher).collect(Collectors.toList());
            if (individualBindings.isEmpty()) {
                final Optional<Stream<PlannerBindings>> onEmptyStreamOptional =
                        onEmptyIndividualBindings(bindingStream);
                if (!onEmptyStreamOptional.isPresent()) {
                    return Stream.empty();
                } else {
                    bindingStream = onEmptyStreamOptional.get();
                }
            } else {
                bindingStream = bindingStream.flatMap(existing -> individualBindings.stream().map(existing::mergedWith));
            }
        }
        return bindingStream;
    }

    @Nonnull
    protected abstract Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(@Nonnull final Stream<PlannerBindings> accumulatedStream);

    /**
     * Get a matcher that tries to match all children with the given {@link ExpressionMatcher}.
     * @param childMatcher an expression matcher to match all of the children
     * @return a matcher that tries to match all children with the given child matcher
     */
    @Nonnull
    public static MultiChildrenMatcher allMatching(@Nonnull ExpressionMatcher<? extends Bindable> childMatcher) {
        return new AllChildrenMatcher(childMatcher);
    }

    /**
     * Get a matcher that tries to match some children with the given {@link ExpressionMatcher}.
     * @param childMatcher an expression matcher to match all of the children
     * @return a matcher that tries to match some children with the given child matcher
     */
    @Nonnull
    public static MultiChildrenMatcher someMatching(@Nonnull ExpressionMatcher<? extends Bindable> childMatcher) {
        return new SomeChildrenMatcher(childMatcher);
    }

    /**
     * An expression children matcher that tries to match all children to a single {@link ExpressionMatcher}.
     *
     * Given a single {@code ExpressionMatcher}, this matcher tries to match it to every child. If it matches all of them, it
     * produces a stream of bindings containing the Cartesian product of the streams of bindings from each child, merged
     * using {@link PlannerBindings#mergedWith(PlannerBindings)}. Because the same matcher is used for all children, the
     * merged bindings will map the single child matcher to a collected list of {@link Bindable}s; such a binding must be
     * retrieved using {@link PlannerBindings#getAll(ExpressionMatcher)} rather than the usual {@code get()} method.
     */
    public static class AllChildrenMatcher extends MultiChildrenMatcher {
        public AllChildrenMatcher(@Nonnull final ExpressionMatcher<? extends Bindable> childMatcher) {
            super(childMatcher);
        }

        @Nonnull
        @Override
        protected Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(@Nonnull final Stream<PlannerBindings> accumulatedStream) {
            return Optional.empty();
        }
    }

    /**
     * An expression children matcher that tries to match some children to a single {@link ExpressionMatcher}.
     *
     * Given a single {@code ExpressionMatcher}, this matcher tries to match it to every child. For the ones that match, it
     * produces a stream of bindings containing the Cartesian product of the streams of bindings from each matching child,
     * merged using {@link PlannerBindings#mergedWith(PlannerBindings)}. Because the same matcher is used for all children, the
     * merged bindings will map the single child matcher to a collected list of {@link Bindable}s; such a binding must be
     * retrieved using {@link PlannerBindings#getAll(ExpressionMatcher)} rather than the usual {@code get()} method.
     */
    public static class SomeChildrenMatcher extends MultiChildrenMatcher {
        public SomeChildrenMatcher(@Nonnull final ExpressionMatcher<? extends Bindable> childMatcher) {
            super(childMatcher);
        }

        @Nonnull
        @Override
        protected Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(@Nonnull final Stream<PlannerBindings> accumulatedStream) {
            return Optional.of(accumulatedStream);
        }
    }
}
