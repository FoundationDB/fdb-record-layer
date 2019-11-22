/*
 * DuplicateEliminatingIteratorAsync.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2019 Apple Inc. and the FoundationDB project authors
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

package com.geophile.z.async;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous version of {@code com.geophile.z.spatialjoin.DuplicateEliminatingIterator}.
 * @param <T> element type
 */
class DuplicateEliminatingIteratorAsync<T> implements IteratorAsync<T> {
    // Object state

    private final IteratorAsync<T> input;
    private final Set<T> seen = new HashSet<>();
    private T next;

    @Override
    public CompletableFuture<T> nextAsync() {
        return CompletableFutures.whileTrue(() -> {
            return input.nextAsync().thenApply(next -> {
                if (next == null) {
                    return false;
                }
                if (seen.add(next)) {
                    this.next = next;
                    return false;
                }
                return true;
            });
        }).thenApply(vignore -> next);
    }

    // DuplicateEliminatingIterator interface

    public DuplicateEliminatingIteratorAsync(IteratorAsync<T> input) {
        this.input = input;
    }
}
