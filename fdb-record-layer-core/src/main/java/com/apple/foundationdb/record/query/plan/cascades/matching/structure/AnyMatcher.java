/*
 * AnyMatcher.java
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

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A binding matcher that is a {@link CollectionMatcher} that binds to individual elements contained in a collection
 * separately.
 *
 * As an example
 *
 * {@code
 * any(equalsObject(5)).matches(ImmutableList.of(1, 5, 2, 3, 5))
 * }
 *
 * produces a stream of two bindings which both bind to {@code 5} each.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class AnyMatcher<T> implements ContainerMatcher<T, Iterable<? extends T>> {
    @Nonnull
    private final BindingMatcher<T> downstream;

    public AnyMatcher(@Nonnull final BindingMatcher<T> downstream) {
        this.downstream = downstream;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull PlannerBindings outerBindings, @Nonnull Iterable<? extends T> in) {
        return StreamSupport.stream(in.spliterator(), false)
                .flatMap(item -> downstream.bindMatches(plannerConfiguration, outerBindings, item));
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final String downstreamId = downstream.identifierFromMatcher();
        final String nestedIndentation = indentation + INDENTATION;
        final String doubleNestedIndentation = nestedIndentation + INDENTATION;
        return "case " + boundId + ": collection =>" + newLine(nestedIndentation) +
               "match any " + downstreamId + " in " + boundId + " {" + newLine(doubleNestedIndentation) +
               downstream.explainMatcher(Object.class, downstreamId, nestedIndentation) + newLine(nestedIndentation) + "}";
    }

    @Nonnull
    public static <T> CollectionMatcher<T> any(@Nonnull final BindingMatcher<T> downstream) {
        final AnyMatcher<T> anyMatcher = new AnyMatcher<>(downstream);
        return new CollectionMatcher<T>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<T> in) {
                return anyMatcher.bindMatchesSafely(plannerConfiguration, outerBindings, in);
            }

            @Override
            public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
                return anyMatcher.explainMatcher(atLeastType, boundId, indentation);
            }
        };
    }

    @Nonnull
    public static <T> AnyMatcher<T> anyInIterable(@Nonnull final BindingMatcher<T> downstream) {
        return new AnyMatcher<>(downstream);
    }
}
