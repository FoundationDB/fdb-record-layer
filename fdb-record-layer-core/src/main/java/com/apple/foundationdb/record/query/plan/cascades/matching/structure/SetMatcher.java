/*
 * SetMatcher.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A multi matcher that binds a sub collection of the collection it is being matched by pairing up the items in the
 * collection (in iteration order) with a list of downstream matchers.
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class SetMatcher<T> implements CollectionMatcher<T> {
    @Nonnull
    private final Collection<? extends BindingMatcher<? extends T>> downstreams;

    private SetMatcher(@Nonnull final Collection<? extends BindingMatcher<? extends T>> downstreams) {
        this.downstreams = downstreams;
    }

    @Nonnull
    @Override
    @SuppressWarnings("java:S3958")
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<T> in) {
        if (in.size() != downstreams.size()) {
            return Stream.empty();
        }

        //
        // We don't know what kind of collection "in" is, however, we now need a set in order to form the permutations.
        // That may be dangerous as the items in the collection may or may not implement proper hash code/equality.
        // In an attempt to support both, we can use an identity-wrapped set.
        //
        final var identity = Equivalence.identity();
        final var inAsWrappedSet = in.stream()
                .map(identity::wrap)
                .collect(ImmutableSet.toImmutableSet());

        final var permutations = TopologicalSort.permutations(inAsWrappedSet);
        return StreamSupport.stream(permutations.spliterator(), false)
                .flatMap(permutation -> bindMatchesForPermutation(plannerConfiguration, outerBindings, permutation));
    }

    @Nonnull
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    @SuppressWarnings({"java:S3958", "UnstableApiUsage"})
    public Stream<PlannerBindings> bindMatchesForPermutation(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final List<Equivalence.Wrapper<T>> permutation) {
        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());
        final var downstreamIterator = downstreams.iterator();
        for (final var wrappedItem : permutation) {
            final var item = Objects.requireNonNull(wrappedItem.get());
            final var downstream = downstreamIterator.next();
            final var individualBindingsIterator = downstream.bindMatches(plannerConfiguration, outerBindings, item).iterator();
            if (!individualBindingsIterator.hasNext()) {
                return Stream.empty();
            } else {
                bindingStream = bindingStream.flatMap(existing -> Streams.stream(individualBindingsIterator).map(existing::mergedWith));
            }
        }
        return bindingStream;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final var nestedIndentation = indentation + INDENTATION;

        final var downstreamIds =
                Streams.mapWithIndex(downstreams.stream(), (downstream, index) -> downstream.identifierFromMatcher() + index)
                        .collect(ImmutableList.toImmutableList());

        return "(" + String.join(", ", downstreamIds) + ") in permutations(" + boundId + ") match all {" + newLine(nestedIndentation) +
               Streams.zip(downstreams.stream(), downstreamIds.stream(),
                       (downstream, downstreamId) -> downstream.explainMatcher(Object.class, downstreamId, nestedIndentation) + "," + newLine(nestedIndentation))
                       .collect(Collectors.joining()) + newLine(indentation) + "}";
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> SetMatcher<T> exactlyInAnyOrder(@Nonnull final BindingMatcher<? extends T>... downstreams) {
        return new SetMatcher<>(Arrays.asList(downstreams));
    }

    @Nonnull
    public static <T> SetMatcher<T> exactlyInAnyOrder(@Nonnull final Collection<? extends BindingMatcher<? extends T>> downstreams) {
        return new SetMatcher<>(downstreams);
    }
}
