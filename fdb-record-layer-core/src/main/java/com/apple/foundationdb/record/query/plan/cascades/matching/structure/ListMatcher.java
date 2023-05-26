/*
 * ListMatcher.java
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A collection matcher that binds a sub collection of the collection it is being matched by pairing up the items in the
 * collection (in iteration order) with a list of downstream matchers.
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class ListMatcher<T> implements CollectionMatcher<T> {
    @Nonnull
    private final List<? extends BindingMatcher<? extends T>> downstreams;

    private ListMatcher(@Nonnull final List<? extends BindingMatcher<? extends T>> downstreams) {
        Preconditions.checkArgument(!downstreams.isEmpty());
        this.downstreams = downstreams;
    }

    @Nonnull
    @Override
    @SuppressWarnings("java:S3958")
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<T> in) {
        if (in.size() != downstreams.size()) {
            return Stream.empty();
        }

        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());
        final Iterator<? extends BindingMatcher<?>> downstreamIterator = downstreams.iterator();
        for (final T item : in) {
            final BindingMatcher<?> downstream = downstreamIterator.next();
            final List<PlannerBindings> individualBindings = downstream.bindMatches(plannerConfiguration, outerBindings, item).collect(Collectors.toList());
            if (individualBindings.isEmpty()) {
                return Stream.empty();
            } else {
                bindingStream = bindingStream.flatMap(existing -> individualBindings.stream().map(existing::mergedWith));
            }
        }
        return bindingStream;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final String nestedIndentation = indentation + INDENTATION;

        final ImmutableList<String> downstreamIds = Streams.mapWithIndex(downstreams.stream(), (downstream, index) -> downstream.identifierFromMatcher() + index)
                .collect(ImmutableList.toImmutableList());

        return "(" + String.join(", ", downstreamIds) + ") in " + boundId + " match all {" + newLine(nestedIndentation) +
               Streams.zip(downstreams.stream(), downstreamIds.stream(),
                       (downstream, downstreamId) -> downstream.explainMatcher(Object.class, downstreamId, nestedIndentation))
                       .collect(Collectors.joining(" && " + newLine(nestedIndentation))) + newLine(indentation) + "}";
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> ListMatcher<T> exactly(@Nonnull final BindingMatcher<? extends T>... downstreams) {
        return new ListMatcher<>(Arrays.asList(downstreams));
    }

    @Nonnull
    public static <T> ListMatcher<T> exactly(@Nonnull final List<? extends BindingMatcher<? extends T>> downstreams) {
        return new ListMatcher<>(downstreams);
    }

    @Nonnull
    public static <T> ListMatcher<T> only(@Nonnull final BindingMatcher<? extends T> downstreams) {
        return new ListMatcher<>(ImmutableList.of(downstreams));
    }
}
