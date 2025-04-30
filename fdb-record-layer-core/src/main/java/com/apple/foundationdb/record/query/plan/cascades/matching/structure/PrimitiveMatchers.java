/*
 * PrimitiveMatchers.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Binding matchers ({@link BindingMatcher}) for primitive types.
 */
public class PrimitiveMatchers {

    private PrimitiveMatchers() {
        // do not instantiate
    }

    /**
     * Matcher that matches if the object passed in is equal to the object being matched.
     * @param object some object to match against
     * @param <T> type of the object
     * @return a new matcher
     */
    @Nonnull
    public static <T> BindingMatcher<T> equalsObject(@Nonnull final T object) {
        return testObject(object, Object::equals);
    }

    /**
     * Matcher that matches if the object passed in and the object being matched satisfy a predicate.
     * @param object some object to match against
     * @param testBiPredicate a predicate invoked on the object and the to-be-tested object
     * @param <T> type of the object
     * @return a new matcher
     */
    @Nonnull
    public static <T> BindingMatcher<T> testObject(@Nonnull final T object, @Nonnull BiPredicate<Object, T> testBiPredicate) {
        return new BindingMatcher<>() {
            @Nonnull
            @Override
            public Class<T> getRootClass() {
                // Note: We should have the caller pass in a class object as we do for many other matchers. However,
                // this is not needed as this matcher is not used on top level purposes. Thus, we can avoid that additional
                // parameter for the sake of readability of the matcher API.
                throw new RecordCoreException("this should return T.class");
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
                // The normal contract for all binding matchers is that only bindMatches() or an override of this method
                // should ever invoke this method. As we also override bindMatches(), this place is not reachable without
                // breaking that mentioned contract.
                throw new RecordCoreException("this should never be called");
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatches(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Object in) {
                return
                        Stream.of(PlannerBindings.from(this, object))
                                .flatMap(bindings -> {
                                    if (testBiPredicate.test(in, object)) {
                                        return Stream.of(bindings);
                                    } else {
                                        return Stream.empty();
                                    }
                                });
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return "match " + boundId + " { case test(" + object + ") => success }";
            }
        };
    }

    @Nonnull
    public static <T> CollectionMatcher<T> containsAll(@Nonnull final Set<? extends T> elements) {
        return new CollectionMatcher<>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<T> in) {
                return Stream.of(PlannerBindings.from(this, in))
                        .flatMap(bindings -> {
                            if (in.containsAll(elements)) {
                                return Stream.of(bindings);
                            } else {
                                return Stream.empty();
                            }
                        });
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return "match " + boundId + " { case {" + elements.stream().map(Object::toString).collect(Collectors.joining(", ")) + "} in " + boundId + " => success }";
            }
        };
    }

    @Nonnull
    public static <T> BindingMatcher<T> satisfies(@Nonnull final Predicate<T> predicate) {
        return new BindingMatcher<>() {
            @Nonnull
            @Override
            public Class<T> getRootClass() {
                // Note: We should have the caller pass in a class object as we do for many other matchers. However,
                // this is not needed as this matcher is not used on top level purposes. Thus, we can avoid that additional
                // parameter for the sake of readability of the matcher API.
                throw new RecordCoreException("this should return T.class");
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public Stream<PlannerBindings> bindMatches(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Object object) {
                final T in = (T)object;
                return bindMatchesSafely(plannerConfiguration, outerBindings, in);
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
                return Stream.of(PlannerBindings.from(this, in))
                        .flatMap(bindings -> {
                            if (predicate.test(in)) {
                                return Stream.of(bindings);
                            } else {
                                return Stream.empty();
                            }
                        });
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return "match " + boundId + " { case { predicate.test(" + boundId + ") => success }";
            }
        };
    }

    @Nonnull
    public static <T, T1> BindingMatcher<T> satisfiesWithOuterBinding(@Nonnull final BindingMatcher<T1> outerBindingMatcher,
                                                                      @Nonnull final BiPredicate<T, T1> predicate) {
        return new BindingMatcher<>() {
            @Nonnull
            @Override
            public Class<T> getRootClass() {
                // Note: We should have the caller pass in a class object as we do for many other matchers. However,
                // this is not needed as this matcher is not used on top level purposes. Thus, we can avoid that additional
                // parameter for the sake of readability of the matcher API.
                throw new RecordCoreException("this should return T.class");
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public Stream<PlannerBindings> bindMatches(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                                       @Nonnull final PlannerBindings outerBindings,
                                                       @Nonnull final Object object) {
                final T in = (T)object;
                return bindMatchesSafely(plannerConfiguration, outerBindings, in);
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                                             @Nonnull final PlannerBindings outerBindings,
                                                             @Nonnull final T in) {
                final T1 outerBinding = outerBindings.get(outerBindingMatcher);
                return Stream.of(PlannerBindings.from(this, in))
                        .flatMap(bindings -> {
                            if (predicate.test(in, outerBinding)) {
                                return Stream.of(bindings);
                            } else {
                                return Stream.empty();
                            }
                        });
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return "match " + boundId + " { case { predicate.test(" + boundId + " with outer binding) => success }";
            }
        };
    }

    @Nonnull
    public static <T> BindingMatcher<T> anyObject() {
        return satisfies(t -> true);
    }
}
