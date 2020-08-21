/*
 * EnumeratingIterable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.google.common.collect.AbstractIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Iterable that provides special iterators of type {@link EnumeratingIterator}.
 * @param <T> type
 */
public interface EnumeratingIterable<T> extends Iterable<List<T>> {
    @Nonnull
    @Override
    EnumeratingIterator<T> iterator();

    static <T> EnumeratingIterable<T> emptyIterable() {
        return new EmptyIterable<>();
    }

    /**
     * An implementation of {@link EnumeratingIterable} that is optimized to work for empty
     * input sets.
     * Iterators created by this class, avoid to build complex state objects during their lifecycle.
     *
     * @param <T> type
     */
    class EmptyIterable<T> implements EnumeratingIterable<T> {
        private EmptyIterable() {
            // use the static factory method
        }

        private class EmptyIterator extends AbstractIterator<List<T>> implements EnumeratingIterator<T> {
            @Override
            public void skip(final int level) {
                throw new UnsupportedOperationException("cannot skip on empty iterator");
            }

            @Nullable
            @Override
            protected List<T> computeNext() {
                return endOfData();
            }
        }

        @Nonnull
        @Override
        public EnumeratingIterator<T> iterator() {
            return new EmptyIterator();
        }
    }
}
