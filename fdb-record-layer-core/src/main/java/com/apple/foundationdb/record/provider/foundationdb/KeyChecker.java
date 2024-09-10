/*
 * KeyChecker.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

/**
 * Hook for checking keys passed to a {@code Transaction}.
 * @see KeyCheckingTransaction
 */
public interface KeyChecker {
    /**
     * Check a single key.
     *
     * @param key the operation's key
     * @param write whether the operation is a mutation
     */
    void checkKey(byte[] key, boolean write);

    /**
     * Check a key range.
     *
     * @param keyBegin the operation's key range begin
     * @param keyEnd the operation's key range end
     * @param write whether the operation is a mutation
     */
    default void checkKeyRange(final byte[] keyBegin, final byte[] keyEnd, boolean write) {
        checkKey(keyBegin, write);
        checkKey(keyEnd, write);
    }

    /**
     * Called when transaction it closed.
     */
    default void close() {
    }
}
