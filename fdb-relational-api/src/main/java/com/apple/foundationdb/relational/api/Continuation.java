/*
 * Continuation.java
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

package com.apple.foundationdb.relational.api;

import javax.annotation.Nullable;

public interface Continuation {

    /**
     * Reason why the continuation was generated in the first place.
     */
    enum Reason {
        /**
         * The continuation was generated as a result of a call to getContinuation but the ResultSet was not exhausted.
         */
        USER_REQUESTED_CONTINUATION,
        /**
         * Reached a transaction limit, such as byte scan limit, row scan limit or time limit.
         */
        TRANSACTION_LIMIT_REACHED,
        /**
         * Reached a query execution limit, such as the maximum number of rows allowed in a result set.
         */
        QUERY_EXECUTION_LIMIT_REACHED,
        /**
         * All rows were returned.
         */
        CURSOR_AFTER_LAST,
    }

    /**
     * Serialize the continuation (such that it can be transferred across the wire).
     * This will create a serialized version of the continuation's entire state, that can be later restored.
     *
     * @return the serialized state of the continuation
     */
    byte[] serialize();

    /**
     * Return the continuation's underlying (cursor) state.
     * This is the state that is representing the continuation's underlying cursor state.
     *
     * @return the cursor state (if exists)
     */
    @Nullable
    byte[] getExecutionState();

    default boolean atBeginning() {
        return getExecutionState() == null;
    }

    default boolean atEnd() {
        byte[] bytes = getExecutionState();
        return bytes != null && bytes.length == 0;
    }

    /**
     * Returns the reason why the continuation was generated in the first place.
     * @return the reason
     */
    Reason getReason();
}
