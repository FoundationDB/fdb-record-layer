/*
 * LinkedIdentitySet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.google.common.base.Equivalence;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A set of items that uses reference equality to determine equivalence for the
 * purposes of set membership, rather than the {@link #equals(Object)} method used by the Java {@link Set} interface.
 * @param <T> the type contained in the set
 */
public class LinkedIdentitySet<T> extends AbstractSet<T> {
    @Nonnull
    private static final Equivalence<Object> identity = Equivalence.identity();

    @Nonnull
    private final Set<Equivalence.Wrapper<T>> members;

    public LinkedIdentitySet() {
        this.members = Sets.newLinkedHashSet();
    }

    public LinkedIdentitySet(@Nonnull final Iterable<? extends T> collection) {
        this.members = Sets.newLinkedHashSet();
        collection.forEach(element -> this.members.add(identity.wrap(element)));
    }

    @Override
    public boolean add(@Nonnull T expression) {
        return members.add(identity.wrap(expression));
    }

    public void addAll(@Nonnull LinkedIdentitySet<T> otherSet) {
        members.addAll(otherSet.members);
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        } else {
            return members.contains(identity.wrap(o));
        }
    }

    @Override
    public boolean remove(@Nonnull Object expression) {
        return members.remove(identity.wrap(expression));
    }

    @Override
    public void clear() {
        members.clear();
    }

    @Override
    public boolean isEmpty() {
        return members.isEmpty();
    }

    @Override
    public int size() {
        return members.size();
    }

    @Nonnull
    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            @Nonnull
            private final Iterator<Equivalence.Wrapper<T>> innerIterator = members.iterator();

            @Override
            public boolean hasNext() {
                return innerIterator.hasNext();
            }

            @Override
            public T next() {
                return innerIterator.next().get();
            }

            @Override
            public void remove() {
                innerIterator.remove();
            }
        };
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof LinkedIdentitySet)) {
            return false;
        }
        return members.equals(((LinkedIdentitySet<?>)o).members);
    }

    @Override
    public int hashCode() {
        return members.hashCode();
    }

    public static <T> Collector<T, ?, Set<T>> toLinkedIdentitySet() {
        return new SetCollector<>(
                Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.UNORDERED,
                        Collector.Characteristics.IDENTITY_FINISH)));
    }

    @SuppressWarnings("unchecked")
    public static <T> LinkedIdentitySet<T> of(@Nonnull final T... args) {
        final var newSet = new LinkedIdentitySet<T>();
        Collections.addAll(newSet, args);
        return newSet;
    }

    public static <T> LinkedIdentitySet<T> copyOf(@Nonnull Iterable<T> iterable) {
        if (iterable instanceof LinkedIdentitySet<?>) {
            return (LinkedIdentitySet<T>)iterable;
        }
        final var newSet = new LinkedIdentitySet<T>();
        iterable.forEach(newSet::add);
        return newSet;
    }

    /**
     * Simple implementation class for {@code Collector}.
     *
     * @param <T> the type of elements to be collected
     */
    private static class SetCollector<T> implements Collector<T, LinkedIdentitySet<T>, Set<T>> {
        private final Set<Characteristics> characteristics;

        private SetCollector(@Nonnull final Set<Characteristics> characteristics) {
            this.characteristics = characteristics;
        }

        @Override
        public BiConsumer<LinkedIdentitySet<T>, T> accumulator() {
            return LinkedIdentitySet::add;
        }

        @Override
        public Supplier<LinkedIdentitySet<T>> supplier() {
            return LinkedIdentitySet::new;
        }

        @Override
        public BinaryOperator<LinkedIdentitySet<T>> combiner() {
            return (left, right) -> {
                if (left.size() < right.size()) {
                    right.addAll(left);
                    return right;
                } else {
                    left.addAll(right);
                    return left;
                }
            };
        }

        @Override
        public Function<LinkedIdentitySet<T>, Set<T>> finisher() {
            return s -> s;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }
}
