/*
 * NotMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Matcher that matches the current object if its downstream was unable to match and vice versa. Note that downstream
 * bindings are not kept (as that is not meaningful).
 */
@API(API.Status.EXPERIMENTAL)
public class NotMatcher implements BindingMatcher<Object> {
    @Nonnull
    private final BindingMatcher<?> downstream;

    public NotMatcher(@Nonnull final BindingMatcher<?> downstream) {
        this.downstream = downstream;
    }

    @Nonnull
    @Override
    public Class<Object> getRootClass() {
        return Object.class;
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
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull PlannerBindings outerBindings, @Nonnull Object in) {
        final Optional<PlannerBindings> nestedBindings =
                downstream.bindMatches(outerBindings, in)
                        .findFirst();

        if (nestedBindings.isPresent()) {
            return Stream.empty();
        }

        return Stream.of(PlannerBindings.empty());
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        return "not(" + downstream.explainMatcher(atLeastType, boundId, indentation) + ")";
    }

    @Nonnull
    public static <T> NotMatcher not(@Nonnull final BindingMatcher<T> downstream) {
        return new NotMatcher(downstream);
    }
}
