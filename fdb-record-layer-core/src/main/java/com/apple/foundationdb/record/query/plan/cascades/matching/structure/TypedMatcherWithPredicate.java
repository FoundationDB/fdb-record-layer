/*
 * TypedMatcherWithPredicate.java
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

import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@code TypeMatcherWithPredicate} extends {@link TypedMatcher} in a way that it still only matches objects that
 * are at least of a certain type but that can be rejected by applying a given predicate to the object that is
 * attempted ot be matched.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypedMatcherWithPredicate<T> extends TypedMatcher<T> {
    private final Predicate<T> predicate;

    protected TypedMatcherWithPredicate(final Class<T> bindableClass,
                                        final Predicate<T> predicate) {
        super(bindableClass);
        this.predicate = predicate;
    }

    public Predicate<T> getPredicate() {
        return predicate;
    }

    @Override
    public Stream<PlannerBindings> bindMatchesSafely(final RecordQueryPlannerConfiguration plannerConfiguration, final PlannerBindings outerBindings, final T in) {
        if (predicate.test(in)) {
            return Stream.of(PlannerBindings.from(this, in));
        } else {
            return Stream.empty();
        }
    }

    @Override
    public String explainMatcher(final Class<?> atLeastType, final String boundId, final String indentation) {
        if (getRootClass().isAssignableFrom(atLeastType)) {
            return "case _ if predicate => success ";
        } else {
            return "case _: " + getRootClass().getSimpleName() + " if predicate => success ";
        }
    }

    public static <S, T extends S> TypedMatcherWithPredicate<T> typedMatcherWithPredicate(final Class<T> bindableClass,
                                                                                          final Predicate<T> predicate) {
        return new TypedMatcherWithPredicate<>(bindableClass, predicate);
    }
}
