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
    byte[] getUnderlyingBytes();

    default boolean atBeginning() {
        return getUnderlyingBytes() == null;
    }

    default boolean atEnd() {
        byte[] bytes = getUnderlyingBytes();
        return bytes != null && bytes.length == 0;
    }
}
