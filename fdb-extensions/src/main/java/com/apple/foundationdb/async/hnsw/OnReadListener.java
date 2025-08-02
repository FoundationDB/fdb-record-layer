/*
 * OnReadListener.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Function interface for a call back whenever we read the slots for a node.
 */
public interface OnReadListener {
    OnReadListener NOOP = new OnReadListener() {
    };

    default <N extends NodeReference> CompletableFuture<Node<N>> onAsyncRead(@Nonnull CompletableFuture<Node<N>> future) {
        return future;
    }

    default void onNodeRead(int layer, @Nonnull Node<? extends NodeReference> node) {
        // nothing
    }

    default void onKeyValueRead(int layer,
                                @Nonnull byte[] key,
                                @Nonnull byte[] value) {
        // nothing
    }
}
