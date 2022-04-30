/*
 * MatchOneAndRestMatcher.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A collection matcher that attempts to match exactly one object in a collection with one matcher and then all
 * remaining objects (as a collection) against another matcher called the rest matcher.
 *
 * @param <T> the type of object that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class MatchOneAndRestMatcher<T> implements CollectionMatcher<T> {
    @Nonnull
    private final BindingMatcher<?> selectedDownstream;
    @Nonnull
    private final CollectionMatcher<?> remainingDownstream;

    private MatchOneAndRestMatcher(@Nonnull final BindingMatcher<?> selectedDownstream,
                                   @Nonnull final CollectionMatcher<?> remainingDownstream) {
        this.selectedDownstream = selectedDownstream;
        this.remainingDownstream = remainingDownstream;
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
    @SuppressWarnings("java:S3958")
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull PlannerBindings outerBindings, @Nonnull Collection<T> in) {
        final Stream.Builder<Stream<PlannerBindings>> streams = Stream.builder();

        for (int i = 0; i < in.size(); i ++) {
            @Nullable T selectedMaybe = null;
            final ImmutableList.Builder<T> remainingBuilder = ImmutableList.builder();
            final Iterator<? extends T> iterator = in.iterator();
            for (int j = 0; iterator.hasNext(); j ++) {
                final T t = iterator.next();
                if (j == i) {
                    selectedMaybe = t;
                } else {
                    remainingBuilder.add(t);
                }
            }
            final T selected = Objects.requireNonNull(selectedMaybe);

            final Stream<PlannerBindings> selectedStream = selectedDownstream.bindMatches(outerBindings, selected);
            final Stream<PlannerBindings> remainingStream = remainingDownstream.bindMatches(outerBindings, remainingBuilder.build());

            streams.add(selectedStream.flatMap(selectedBindings -> remainingStream.map(selectedBindings::mergedWith)));
        }
        return streams.build().flatMap(Function.identity());
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final String nestedIndentation = indentation + INDENTATION;
        final String selectedNestedId = selectedDownstream.identifierFromMatcher();
        final String remainingNestedId = remainingDownstream.identifierFromMatcher();

        return "one of " + selectedNestedId + " in " + boundId + " {" + newLine(nestedIndentation) +
               selectedDownstream.explainMatcher(Object.class, selectedNestedId, nestedIndentation) + newLine(indentation) +
               "} and then remaining " + remainingNestedId + " in " + boundId +  " {" +
               remainingDownstream.explainMatcher(Object.class, selectedNestedId, nestedIndentation) + newLine(indentation) +
               "}";
    }

    @Nonnull
    public static <T> MatchOneAndRestMatcher<T> matchOneAndRest(@Nonnull final BindingMatcher<? super T> selectedDownstream,
                                                                @Nonnull final CollectionMatcher<? super T> remainingDownstream) {
        return new MatchOneAndRestMatcher<>(selectedDownstream, remainingDownstream);
    }
}
