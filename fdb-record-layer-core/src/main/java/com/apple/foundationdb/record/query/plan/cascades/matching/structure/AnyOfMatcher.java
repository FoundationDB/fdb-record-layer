/*
 * AnyOfMatcher.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A matcher that matches if any of its downstream matchers produce bindings. This matcher is
 * intended to be used to express a logical <em>or</em> between matchers on an object.
 *
 * As an example
 *
 * {@code
 * matchingAnyOf(greaterThan(0), divisibleBy(2)).matches(2))
 * }
 *
 * produces a stream of one binding which binds to {@code 2}.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class AnyOfMatcher<T> implements BindingMatcher<T> {
    @Nonnull
    private final Class<T> staticClassOfT;
    @Nonnull
    private final List<BindingMatcher<?>> downstreams;

    private AnyOfMatcher(@Nonnull final Class<T> staticClassOfT, @Nonnull final Collection<? extends BindingMatcher<?>> downstreams) {
        this.staticClassOfT = staticClassOfT;
        this.downstreams = ImmutableList.copyOf(downstreams);
    }

    @Nonnull
    @Override
    public Class<T> getRootClass() {
        return staticClassOfT;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
        return downstreams
                .stream()
                .flatMap(extractingMatcher -> extractingMatcher.bindMatches(plannerConfiguration, outerBindings, in));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final String nestedIndentation = indentation + INDENTATION;
        final ImmutableList<String> downstreamIds = Streams.mapWithIndex(downstreams.stream(), (downstream, index) -> downstream.identifierFromMatcher() + index)
                .collect(ImmutableList.toImmutableList());

        return "all of {" + newLine(nestedIndentation) +
               Streams.zip(downstreams.stream(), downstreamIds.stream(),
                       (downstream, downstreamId) -> downstream.explainMatcher(atLeastType, boundId, nestedIndentation))
                       .collect(Collectors.joining(" && " + newLine(nestedIndentation))) + newLine(indentation) + "}";
    }

    public static <T> AnyOfMatcher<T> matchingAnyOf(@Nonnull final Class<T> staticClassOfT,
                                                    @Nonnull final Collection<? extends BindingMatcher<?>> matchingExtractors) {
        return new AnyOfMatcher<>(staticClassOfT, matchingExtractors);
    }
}
