/*
 * EnumeratingIterator.java
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

import java.util.Iterator;
import java.util.List;

/**
 * An iterator extending {@link Iterator} providing the ability to skip a certain prefix.
 * @param <T> type
 */
public interface EnumeratingIterator<T> extends Iterator<List<T>> {
    /**
     * Instructs the iterator to advance to the next possible ordering using the given zero-indexed level.
     *
     * Example 1: If the last returned ordering of the iterator {@code it} is {@code (e0, e1, e2, e3)} and
     *            {@code it.skip(2)} is called, the state of the iterator is advanced in a way that either
     *            reaches the end of iteration or the next item that is returned is {@code (e0', e1', e3', e4')}
     *            where the prefix {@code (e1', e2', e3')} is not equal to {code (e1, e2, e3)}.
     * Example 2: If the last returned ordering of the iterator {@code it} is {@code (e1, e2, e3, e4)} and
     *            {@code it.skip(1)} is called, the state of the iterator is advanced in a way that either reaches
     *            the end of iteration or the next item that is returned is {@code (e1', e2', e3', e4')} where
     *            the prefix {@code (e1', e2')} is not equal to {code (e1, e2)}.
     *
     * @param level skip level
     */
    void skip(int level);
}
