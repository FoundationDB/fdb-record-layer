/*
 * EnumeratingIterator.java
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
     * Example 1: Consider the last returned ordering of the iterator {@code it} is {@code (e0, e1, e2, e3)}.
     *            If {@code it.skip(1)} is called, the state of the iterator is advanced in a way that it either
     *            <ul>
     *                <li> reaches the end of iteration and a subsequent call to {@code it.hasNext()} returns {@code false} or</li>
     *                <li> the item being returned upon a subsequent call to {@code it.next()} denoted as
     *                     {@code (e0', e1', e2', e3')} is the next item occurring in the enumeration
     *                     where {@code (e0', e1')} is greater than {@code (e0, e1)}.
     *                </li>
     *            </ul>
     * Example 2: Consider the last returned ordering of the iterator {@code it} is {@code (e0, e1, e2, e3)}.
     *            If {@code it.skip(2)} is called, the state of the iterator is advanced in a way that it either
     *            <ul>
     *                <li> reaches the end of iteration and a subsequent call to {@code it.hasNext()} returns {@code false} or</li>
     *                <li> the item being returned upon a subsequent call to {@code it.next()} denoted as
     *                     {@code (e0', e1', e2', e3')} is the next item occurring in the enumeration
     *                     where {@code (e0', e1', e2')} is greater than {@code (e0, e1, e2)}.
     *                </li>
     *            </ul>
     *
     * @param level skip level
     */
    void skip(int level);
}
