/*
 * PartialOrder.java
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

package com.apple.foundationdb.record.query.combinatorics;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

public class PartialOrder<T> {
    @Nonnull
    private final ImmutableSet<T> set;
    @Nonnull
    private final ImmutableSetMultimap<T, T> dependencyMap;

    public PartialOrder(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependencyMap) {
        this.set = ImmutableSet.copyOf(set);
        this.dependencyMap = ImmutableSetMultimap.copyOf(dependencyMap);
    }

    @Nonnull
    public ImmutableSet<T> getSet() {
        return set;
    }


    @Nonnull
    public SetMultimap<T, T> getDependencyMap() {
        return dependencyMap;
    }

    int size() {
        return set.size();
    }

    public PartialOrder<T> invertDependencies() {
        return PartialOrder.of(set, dependencyMap.inverse());
    }

    public static <T> PartialOrder<T> of(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependencyMap) {
        return new PartialOrder<>(set, dependencyMap);
    }

    public static <T> PartialOrder.Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        @Nonnull
        private final ImmutableSet.Builder<T> setBuilder;
        @Nonnull
        private final ImmutableSetMultimap.Builder<T, T> dependencyMapBuilder;

        private Builder() {
            this.setBuilder = ImmutableSet.builder();
            this.dependencyMapBuilder = ImmutableSetMultimap.builder();
        }

        public Builder<T> addSet(@Nonnull final Set<T> additionalElements) {
            setBuilder.addAll(additionalElements);
            return this;
        }

        public Builder<T> addListWithDependencies(@Nonnull final List<T> additionalElements) {
            setBuilder.addAll(additionalElements);

            final var iterator = additionalElements.iterator();
            if (iterator.hasNext()) {
                final var last = iterator.next();
                while (iterator.hasNext()) {
                    final var current = iterator.next();
                    dependencyMapBuilder.put(current, last);
                }
            }

            return this;
        }

        public PartialOrder<T> build() {
            return PartialOrder.of(setBuilder.build(), dependencyMapBuilder.build());
        }
    }
}
