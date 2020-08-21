/*
 * RelationalExpressionPointerSet.java
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

import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A set of {@link RelationalExpression}s that uses reference ("pointer") equality to determine equivalence for the
 * purposes of set membership, rather than the {@link #equals(Object)} method used by the Java {@link Set} interface.
 * This is important for implementing the memo data structure in {@link GroupExpressionRef}
 * @param <T> the planner expression type contained in the set
 */
public class RelationalExpressionPointerSet<T extends RelationalExpression> extends AbstractCollection<T> {
    @Nonnull
    private final Set<Wrapper<T>> members;

    public RelationalExpressionPointerSet() {
        this.members = new HashSet<>();
    }

    private RelationalExpressionPointerSet(@Nonnull Set<Wrapper<T>> members) {
        this.members = members;
    }

    @Override
    public boolean add(@Nonnull T expression) {
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(expression);
        return members.add(new Wrapper<>(expression));
    }

    public void addAll(@Nonnull RelationalExpressionPointerSet<T> otherSet) {
        members.addAll(otherSet.members);
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof RelationalExpression)) { // also handles null check
            return false;
        } else {
            return members.contains(new Wrapper<>((RelationalExpression) o));
        }

    }

    public boolean contains(@Nonnull T expression) {
        return members.contains(new Wrapper<>(expression));
    }

    public boolean remove(@Nonnull T expression) {
        return members.remove(new Wrapper<>(expression));
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
    private static class Wrapper<T extends RelationalExpression> {
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
