/*
 * OptionalIfPresentMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A matcher that matches an optional if the object is present.
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class OptionalIfPresentMatcher<T> implements BindingMatcher<Optional<T>> {
    @Nonnull
    private final BindingMatcher<?> downstream;

    public OptionalIfPresentMatcher(@Nonnull final BindingMatcher<?> downstream) {
        this.downstream = downstream;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Class<Optional<T>> getRootClass() {
        return (Class<Optional<T>>)(Class<?>)Optional.class;
    }

    @SuppressWarnings("OptionalIsPresent")
    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Optional<T> in) {
        return Stream.of(PlannerBindings.from(this, in))
                .flatMap(bindings -> {
                    if (!in.isPresent()) {
                        return Stream.empty();
                    }
                    return downstream
                            .bindMatches(plannerConfiguration, outerBindings, in.get())
                            .map(bindings::mergedWith);
                });
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        if (Optional.class.isAssignableFrom(atLeastType)) {
            return "case " + boundId + " if " + boundId + " isPresent() => success";
        } else {
            return "case " + boundId + ":Optional if " + boundId + " isPresent() => success";
        }
    }

    @Nonnull
    public static <T> OptionalIfPresentMatcher<T> present(@Nonnull final BindingMatcher<?> downstream) {
        return new OptionalIfPresentMatcher<>(downstream);
    }
}
