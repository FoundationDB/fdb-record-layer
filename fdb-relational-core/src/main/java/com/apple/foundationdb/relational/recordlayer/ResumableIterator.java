/*
 * ResumableIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.Iterator;

public interface ResumableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close() throws RelationalException;

    /**
     * Returns a {@code Continuation} that can be used to resume the iteration.
     *
     * A {@code Continuation} always points to the element <i>next</i> to the current
     * {@code Iterator} element with the following caveats:
     *   <ul>
     *       <li>
     *           If the {@code Iterator} is in its initials state (i.e. no {@code next()} is called yet, then
     *           calling this method returns a {@code Continuation} that points to the first element of the
     *           {@code Iterator}.
     *       </li>
     *       <li>
     *           If the {@code Iterator} is pointing to an element whose index is (x), then calling this method
     *           returns a {@code Continuation} that points to element with index (x+1).
     *       </li>
     *       <li>
     *           If the {@code Iterator} is pointing to the last element, then calling this method returns a
     *           {@code Continuation} indicating there are no more elements to resume upon (i.e. its {@code atEnd}
     *           will return {@code true}).
     *       </li>
     *   </ul>
     *
     * @return A {@code Continuation} that can be used to resume the iteration over the elements.
     * @throws RelationalException if the continuation cannot be retrieved
     */
    Continuation getContinuation() throws RelationalException;

    /**
     * Returns an indication on whether the iterator has stopped returning elements before reaching the final element.
     *
     * @return Returns {@code true} if the {@code Iterator} can not return more elements because of an internal
     * or user-configured boundary condition is met. In this case, it is possible to call {@code getContinuation}
     * method to retrieve a {@code Continuation} object that can be used to resume the iteration.
     */
    boolean terminatedEarly();

    RecordCursor.NoNextReason getNoNextReason();

    boolean isClosed();
}
