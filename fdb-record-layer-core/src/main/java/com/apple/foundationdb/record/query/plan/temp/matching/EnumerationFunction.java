/*
 * EnumerationFunction.java
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

package com.apple.foundationdb.record.query.plan.temp.matching;

import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterator;

import java.util.Iterator;
import java.util.List;

/**
 * Functional interface to allow implementors to provide use case-specific logic when permutations are enumerated by
 * {@link BaseMatcher#match}. The magic logic then calls {@link #apply} with an {@link EnumeratingIterator} for
 * ordered aliases on this side and <em>one</em> stable non-violating ordering o the other side. The result of this
 * method is an {@link Iterable} of the result type {@code R}.
 *
 * @param <R> result type
 */
@FunctionalInterface
public interface EnumerationFunction<R> {
    /**
     * Called directly or indirectly by matching logic in {@link BaseMatcher#match} using an {@link EnumeratingIterator}
     * for ordered aliases on this side and <em>one</em> stable non-violating ordering o the other side. The result of this
     * method is an {@link Iterable} of the result type {@code R}.
     * @param iterator {@link EnumeratingIterator} over all possible non-violating possible orderings of aliases on
     *        this side
     * @param otherOrdered one possible non-violating ordering of aliases on the other side
     * @return {@link Iterable} of type {@code R}
     */
    Iterator<R> apply(EnumeratingIterator<CorrelationIdentifier> iterator,
                      List<CorrelationIdentifier> otherOrdered);
}
