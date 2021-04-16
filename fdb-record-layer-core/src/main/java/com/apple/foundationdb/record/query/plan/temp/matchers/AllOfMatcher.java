/*
 * AllOfMatcher.java
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
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A <code>BindingMatcher</code> is an expression that can be matched against a
 * {@link RelationalExpression} tree, while binding certain expressions/references in the tree to expression matcher objects.
 * The bindings can be retrieved from the rule call once the binding is matched.
 *
 * <p>
 * Extreme care should be taken when implementing <code>ExpressionMatcher</code>, since it can be very delicate.
 * In particular, expression matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>ExpressionMatcher</code> must use the (default) reference equals.
 * </p>
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class AllOfMatcher<T> implements BindingMatcher<T> {
    private final Class<? extends T> staticClassOfT;
    private final List<ExtractingMatcher<T>> extractingMatchers;

    private AllOfMatcher(@Nonnull final Class<? extends T> staticClassOfT, @Nonnull final Iterable<ExtractingMatcher<T>> matchingExtractors) {
        this.staticClassOfT = staticClassOfT;
        this.extractingMatchers = ImmutableList.copyOf(matchingExtractors);
    }

    @Nonnull
    @Override
    public Class<? extends T> getRootClass() {
        return staticClassOfT;
    }

    /**
     * Attempt to match this matcher against the given expression reference.
     * Note that implementations of {@code matchWith()} should only attempt to match the given root with this planner
     * expression or attempt to access the members of the given reference.
     *
     * @param outerBindings preexisting bindings to be used by the matcher
     * @param in the bindable we attempt to match
     * @return a stream of {@link PlannerBindings} containing the matched bindings, or an empty stream is no match was found
     */
    @Nonnull
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull PlannerBindings outerBindings, @Nonnull T in) {
        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());

        for (final ExtractingMatcher<T> extractingMatcher : extractingMatchers) {
            bindingStream = bindingStream.flatMap(bindings -> extractingMatcher.unapplyAndBindExtractedMatches(outerBindings, in).map(bindings::mergedWith));
        }

        return bindingStream;
    }

    public static <T> AllOfMatcher<T> matchingAllOf(@Nonnull final Class<? extends T> staticClassOfT,
                                                    @Nonnull final Collection<ExtractingMatcher<T>> matchingExtractors) {
        return new AllOfMatcher<>(staticClassOfT, matchingExtractors);
    }

    public static <T> BindingMatcher<T> matchingAllOf(@Nonnull final Collection<BindingMatcher<T>> bindingMatchers) {
        final BindingMatcher<T> oneMatcher = Objects.requireNonNull(Iterables.getFirst(bindingMatchers, null));

        final ImmutableList<ExtractingMatcher<T>> extractingMatchers =
                bindingMatchers
                        .stream()
                        .map(bindingMatcher -> ExtractingMatcher.<T, T>of(t -> t, bindingMatcher))
                        .collect(ImmutableList.toImmutableList());

        return AllOfMatcher.matchingAllOf(oneMatcher.getRootClass(), extractingMatchers);
    }
}
