/*
 * MultiMatcher.java
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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A multi matcher is a {@link CollectionMatcher} and by extension also a {@link BindingMatcher} that binds to a sub
 * collection of objects in the presented collection. The multi matcher is abstract and only implemented by
 * {@link AllMatcher} and {@link SomeMatcher} that provided different semantics in the way they deal with non-matching
 * downstreams.
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public abstract class MultiMatcher<T> implements CollectionMatcher<T> {
    private final BindingMatcher<T> downstream;

    protected MultiMatcher(final BindingMatcher<T> downstream) {
        this.downstream = downstream;
    }

    protected BindingMatcher<T> getDownstream() {
        return downstream;
    }

    @Override
    public Stream<PlannerBindings> bindMatchesSafely(RecordQueryPlannerConfiguration plannerConfiguration, PlannerBindings outerBindings, Collection<T> in) {
        final ImmutableList.Builder<T> items = ImmutableList.builder();
        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());

        // The children need to be merged in the same order that they appear to satisfy the contract of
        // PlannerBindings.getAll().
        int counter = 0;
        for (final T item : in) {
            final List<PlannerBindings> individualBindings = downstream.bindMatches(plannerConfiguration, outerBindings, item).collect(Collectors.toList());
            if (individualBindings.isEmpty()) {
                final Optional<Stream<PlannerBindings>> onEmptyStreamOptional = onEmptyIndividualBindings(bindingStream);
                if (onEmptyStreamOptional.isEmpty()) {
                    return Stream.empty();
                } else {
                    bindingStream = onEmptyStreamOptional.get();
                }
            } else {
                items.add(item);
                counter++;
                bindingStream = bindingStream.flatMap(existing -> individualBindings.stream().map(existing::mergedWith));
            }
        }

        bindingStream = bindingStream.flatMap(existing -> Stream.of(PlannerBindings.from(this, items.build()).mergedWith(existing)));

        return onNumberOfMatches(counter, bindingStream);
    }

    protected abstract Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(Stream<PlannerBindings> accumulatedStream);

    protected abstract Stream<PlannerBindings> onNumberOfMatches(int numberOfMatches, Stream<PlannerBindings> accumulatedStream);

    /**
     * A multi matcher that binds a sub collection of objects of the collection it is being matched. That includes the empty
     * collection which is the minimal sub collection this matcher binds, that is if no down stream matchers match
     * this matcher still binds the empty collection.
     * @param <T> type param
     */
    public static class SomeMatcher<T> extends MultiMatcher<T> {
        private SomeMatcher(final BindingMatcher<T> downstream) {
            super(downstream);
        }

        @Override
        protected Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(final Stream<PlannerBindings> accumulatedStream) {
            return Optional.of(accumulatedStream);
        }

        @Override
        public String explainMatcher(final Class<?> atLeastType, final String boundId, final String indentation) {
            final String nestedIndentation = indentation + INDENTATION;
            final String nestedId = getDownstream().identifierFromMatcher();
            return "some " + nestedId + " in " + boundId + " that match {" + newLine(nestedIndentation) +
                   getDownstream().explainMatcher(Object.class, nestedId, nestedIndentation) + newLine(indentation) +
                   "}";
        }

        @Override
        protected Stream<PlannerBindings> onNumberOfMatches(final int numberOfMatches,
                                                            final Stream<PlannerBindings> accumulatedStream) {
            return accumulatedStream;
        }
    }

    /**
     * A multi matcher that binds to all objects in the collection it is being matched or it does not match anything at all,
     * i.e. nothing is bound.
     * @param <T> type param
     */
    public static class AllMatcher<T> extends MultiMatcher<T> {
        private AllMatcher(final BindingMatcher<T> downstream) {
            super(downstream);
        }

        @Override
        protected Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(final Stream<PlannerBindings> accumulatedStream) {
            return Optional.empty();
        }

        @Override
        public String explainMatcher(final Class<?> atLeastType, final String boundId, final String indentation) {
            final String nestedIndentation = indentation + INDENTATION;
            final String nestedId = getDownstream().identifierFromMatcher();
            return "all " + nestedId + " in " + boundId + " {" + newLine(nestedIndentation) +
                   getDownstream().explainMatcher(Object.class, nestedId, nestedIndentation) + newLine(indentation) +
                   "}";
        }

        @Override
        protected Stream<PlannerBindings> onNumberOfMatches(final int numberOfMatches,
                                                            final Stream<PlannerBindings> accumulatedStream) {
            return accumulatedStream;
        }
    }

    /**
     * A multi matcher that binds a sub collection of objects of the collection it is being matched if the number of
     * objects exceeds a certain threshold.
     * @param <T> type param
     */
    public static class AtLeastMatcher<T> extends MultiMatcher<T> {

        private final int minNumberOfRequiredMatches;

        private AtLeastMatcher(final BindingMatcher<T> downstream, int minNumberOfRequiredMatches) {
            super(downstream);
            Verify.verify(minNumberOfRequiredMatches >= 0);
            this.minNumberOfRequiredMatches = minNumberOfRequiredMatches;
        }

        @Override
        protected Optional<Stream<PlannerBindings>> onEmptyIndividualBindings(final Stream<PlannerBindings> accumulatedStream) {
            return Optional.of(accumulatedStream);
        }

        @Override
        public String explainMatcher(final Class<?> atLeastType, final String boundId, final String indentation) {
            final String nestedIndentation = indentation + INDENTATION;
            final String nestedId = getDownstream().identifierFromMatcher();
            return "atLeast(" + minNumberOfRequiredMatches + ")" + nestedId + " in " + boundId + " that match {" + newLine(nestedIndentation) +
                    getDownstream().explainMatcher(Object.class, nestedId, nestedIndentation) + newLine(indentation) +
                    "}";
        }

        @Override
        protected Stream<PlannerBindings> onNumberOfMatches(final int numberOfMatches,
                                                            final Stream<PlannerBindings> accumulatedStream) {
            if (numberOfMatches >= minNumberOfRequiredMatches) {
                return accumulatedStream;
            }
            return Stream.empty();
        }
    }

    public static <T> AllMatcher<T> all(final BindingMatcher<T> downstream) {
        return new AllMatcher<>(downstream);
    }

    public static <T> SomeMatcher<T> some(final BindingMatcher<T> downstream) {
        return new SomeMatcher<>(downstream);
    }

    public static <T> AtLeastMatcher<T> atLeastOne(final BindingMatcher<T> downstream) {
        return new AtLeastMatcher<>(downstream, 1);
    }

    public static <T> AtLeastMatcher<T> atLeastTwo(final BindingMatcher<T> downstream) {
        return new AtLeastMatcher<>(downstream, 2);
    }
}
