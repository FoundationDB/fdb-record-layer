/*
 * AllOfMatcher.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A matcher that only matches anything if all of its downstream matchers produce bindings. This matcher is
 * intended to be used to express a logical <em>and</em> between matchers on an object.
 *
 * As an example
 *
 * {@code
 * matchingAllOf(greaterThan(0), divisibleBy(2)).matches(2))
 * }
 *
 * produces a stream of one binding which binds to {@code 2}.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class AllOfMatcher<T> implements BindingMatcher<T> {
    @Nonnull
    private final Class<T> staticClassOfT;
    @Nonnull
    private final List<BindingMatcher<?>> downstreams;

    private AllOfMatcher(@Nonnull final Class<T> staticClassOfT, @Nonnull final Collection<? extends BindingMatcher<?>> downstreams) {
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
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull PlannerBindings outerBindings, @Nonnull T in) {
        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());

        for (final BindingMatcher<?> downstream : downstreams) {
            bindingStream = bindingStream
                    .flatMap(bindings -> downstream.bindMatches(outerBindings, in)
                            .map(bindings::mergedWith));
        }

        return bindingStream;
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

    public static <T> AllOfMatcher<T> matchingAllOf(@Nonnull final Class<T> staticClassOfT,
                                                    @Nonnull final BindingMatcher<?>... matchingExtractors) {
        return new AllOfMatcher<>(staticClassOfT, Arrays.asList(matchingExtractors));
    }

    public static <T> AllOfMatcher<T> matchingAllOf(@Nonnull final Class<T> staticClassOfT,
                                                    @Nonnull final Collection<? extends BindingMatcher<?>> matchingExtractors) {
        return new AllOfMatcher<>(staticClassOfT, matchingExtractors);
    }
}
