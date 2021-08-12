/*
 * CollectionMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Tag interface used for overloads for matchers.
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public interface CollectionMatcher<T> extends ContainerMatcher<T, Collection<? extends T>> {
    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    default Class<Collection<? extends T>> getRootClass() {
        // the usual Java shenanigans to get a properly typed class object out of the class object
        return (Class<Collection<? extends T>>)(Class<?>)Collection.class;
    }

    static <T> CollectionMatcher<T> empty() {
        return new CollectionMatcher<T>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final PlannerBindings outerBindings, @Nonnull final Collection<? extends T> in) {
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
}
