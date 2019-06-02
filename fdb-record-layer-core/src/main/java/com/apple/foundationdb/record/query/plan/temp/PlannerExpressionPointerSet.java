/*
 * PlannerExpressionPointerSet.java
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

package com.apple.foundationdb.record.query.plan.temp;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A set of {@link PlannerExpression}s that uses reference ("pointer") equality to determine equivalence for the
 * purposes of set membership, rather than the {@link #equals(Object)} method used by the Java {@link Set} interface.
 * This is important for implementing the memo data structure in {@link GroupExpressionRef}
 * @param <T> the planner expression type contained in the set
 */
public class PlannerExpressionPointerSet<T extends PlannerExpression> implements Iterable<T> {
    @Nonnull
    private final Set<Wrapper<T>> members;

    public PlannerExpressionPointerSet() {
        this.members = new HashSet<>();
    }

    private PlannerExpressionPointerSet(@Nonnull Set<Wrapper<T>> members) {
        this.members = members;
    }

    public void add(@Nonnull T expression) {
        members.add(new Wrapper<>(expression));
    }

    public void addAll(@Nonnull PlannerExpressionPointerSet<T> otherSet) {
        members.addAll(otherSet.members);
    }

    public boolean contains(@Nonnull T expression) {
        return members.contains(new Wrapper<>(expression));
    }

    public boolean remove(@Nonnull T expression) {
        return members.remove(new Wrapper<>(expression));
    }

    public void clear() {
        members.clear();
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }

    public int size() {
        return members.size();
    }

    @Nonnull
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Nonnull
            private final Iterator<Wrapper<T>> innerIterator = members.iterator();

            @Override
            public boolean hasNext() {
                return innerIterator.hasNext();
            }

            @Override
            public T next() {
                return innerIterator.next().getExpression();
            }
        };
    }

    /**
     * The {@code Wrapper} exists only to provide {@link #equals(Object)} and {@link #hashCode()} implementations that
     * use reference ("pointer") inequality on the objects of type {@code T} instead of the usual {@link #equals(Object)}.
     * This allows us to build a set containing several elements that might be equal according to a custom equality
     * method but are not the same object.
     * @param <T> the type of object being wrapped
     */
    private static class Wrapper<T extends PlannerExpression> {
        @Nonnull
        private final T expression;

        public Wrapper(@Nonnull T expression) {
            this.expression = expression;
        }

        @Nonnull
        public T getExpression() {
            return expression;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Wrapper<?> wrapper = (Wrapper<?>)o;

            return expression == wrapper.expression;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(expression);
        }

        @Override
        public String toString() {
            return expression.toString();
        }
    }
}
