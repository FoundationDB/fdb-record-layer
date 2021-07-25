/*
 * PrimitiveMatchers.java
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

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
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
        return new BindingMatcher<T>() {
            @Nonnull
            @Override
            public Class<T> getRootClass() {
                // Note: We should have the caller pass in a class object as we for many other matchers. However,
                // this is not needed as this matcher is not used on top level purposes. Thus, we can avoid that additional
                // parameter for the sake of readability of the matcher API.
                throw new RecordCoreException("this should return T.class");
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
                // The normal contract for all binding matchers is that only bindMatches() or an override of this method
                // should ever invoke this method. As we also override bindMatches(), this place is not reachable without
                // breaking that mentioned contract. It would be better if bindMatchesSafely() were declared protected
                // in the BindMatcher interface. TODO: Do this when we go to Java 9.
                throw new RecordCoreException("this should never be called");
            }

            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatches(@Nonnull final PlannerBindings outerBindings, @Nonnull final Object in) {
                return
                        Stream.of(PlannerBindings.from(this, object))
                                .flatMap(bindings -> {
                                    if (in.equals(object)) {
                                        return Stream.of(bindings);
                                    } else {
                                        return Stream.empty();
                                    }
                                });
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return "match " + boundId + " { case " + object.toString() + " => success }";
            }
        };
    }

    @Nonnull
    public static <T> CollectionMatcher<T> containsAll(@Nonnull final Set<? extends T> elements) {
        return new CollectionMatcher<T>() {
            @SuppressWarnings("SuspiciousMethodCalls")
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<? extends T> in) {
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
}
