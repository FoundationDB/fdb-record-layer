/*
 * CollectionMatcher.java
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
import com.apple.foundationdb.record.query.combinatorics.ChooseK;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Tag interface used for overloads for matchers that bind to collections of values/objects.
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public interface CollectionMatcher<T> extends ContainerMatcher<T, Collection<T>> {
    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    default Class<Collection<T>> getRootClass() {
        // the usual Java shenanigans to get a properly typed class object out of the class object
        return (Class<Collection<T>>)(Class<?>)Collection.class;
    }

    static <T> CollectionMatcher<T> empty() {
        return new CollectionMatcher<T>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<T> in) {
                return in.isEmpty() ? Stream.of(PlannerBindings.from(this, in)) : Stream.empty();
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                if (Collection.class.isAssignableFrom(atLeastType)) {
                    return "case " + boundId + " if " + boundId + " isEmpty() => success";
                } else {
                    return "case " + boundId + ":Collection if " + boundId + " isEmpty() => success";
                }
            }
        };
    }

    @Nonnull
    static <E> CollectionMatcher<E> fromBindingMatcher(@Nonnull final BindingMatcher<Collection<E>> matcher) {
        return new CollectionMatcher<E>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<E> in) {
                return matcher.bindMatchesSafely(plannerConfiguration, outerBindings, in);
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return matcher.explainMatcher(atLeastType, boundId, indentation);
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    static <E> CollectionMatcher<E> choose(@Nonnull final BindingMatcher<? extends E> downstream0,
                                           @Nonnull final BindingMatcher<? extends E> downstream1,
                                           @Nonnull final BindingMatcher<? extends E>... downstreamTail) {
        return choose(ImmutableList.<BindingMatcher<? extends E>>builder()
                .add(downstream0)
                .add(downstream1)
                .addAll(Arrays.asList(downstreamTail))
                .build());
    }

    @SuppressWarnings("unchecked")
    static <E> CollectionMatcher<E> choose(@Nonnull final List<? extends BindingMatcher<? extends E>> downstreams) {
        return CollectionMatcher.fromBindingMatcher(
                TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<E>>)(Class<?>)Collection.class,
                        Extractor.of(element -> ChooseK.chooseK(element, Math.min(element.size(), downstreams.size())), name -> "choose(" + name + ", " + downstreams.size() + ")"),
                        AnyMatcher.anyInIterable(SetMatcher.exactlyInAnyOrder(downstreams))));
    }

    static <E> CollectionMatcher<E> combinations(@Nonnull final CollectionMatcher<? extends E> downstream) {
        return combinations(downstream, collection -> 0, Collection::size);
    }

    @SuppressWarnings("unchecked")
    static <E> CollectionMatcher<E> combinations(@Nonnull final CollectionMatcher<? extends E> downstream,
                                                 @Nonnull final Function<Collection<E>, Integer> startInclusiveFunction,
                                                 @Nonnull final Function<Collection<E>, Integer> endExclusiveFunction) {
        return CollectionMatcher.fromBindingMatcher(
                TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<E>>)(Class<?>)Collection.class,
                        Extractor.of(collection -> ChooseK.chooseK(collection, startInclusiveFunction.apply(collection), endExclusiveFunction.apply(collection)), name -> "combinations(" + name + ")"),
                        AnyMatcher.anyInIterable(downstream)));
    }

    @SuppressWarnings("unchecked")
    static <E> CollectionMatcher<E> combinations(@Nonnull final CollectionMatcher<? extends E> downstream,
                                                 @Nonnull final BiFunction<RecordQueryPlannerConfiguration, Collection<E>, Integer> startInclusiveFunction,
                                                 @Nonnull final BiFunction<RecordQueryPlannerConfiguration, Collection<E>, Integer> endExclusiveFunction) {
        return CollectionMatcher.fromBindingMatcher(
                TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<E>>)(Class<?>)Collection.class,
                        Extractor.of((plannerConfiguration, collection) -> ChooseK.chooseK(collection, startInclusiveFunction.apply(plannerConfiguration, collection), endExclusiveFunction.apply(plannerConfiguration, collection)), name -> "combinations(" + name + ")"),
                        AnyMatcher.anyInIterable(downstream)));
    }
}
