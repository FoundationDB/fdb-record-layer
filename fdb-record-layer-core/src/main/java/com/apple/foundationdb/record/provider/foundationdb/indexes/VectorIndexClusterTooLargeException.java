/*
 * VectorIndexClusterTooLargeException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Signals that an insert into a Guardiann vector index would grow a cluster beyond its configured hard cap
 * ({@code guardiannPrimaryClusterHardMax}) while the index is not draining deferred maintenance tasks in the writing
 * transaction. It is the vector-index analogue of
 * {@link com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue.PendingWritesQueueTooLargeException}:
 * a terminal back-pressure signal that fails the write so the caller slows down until a background merge has drained the
 * split backlog. Retrying immediately will not help until that backlog shrinks.
 */
@SuppressWarnings("serial")
public class VectorIndexClusterTooLargeException extends RecordCoreException {
    public VectorIndexClusterTooLargeException(@Nonnull final String msg, @Nullable final Throwable cause) {
        super(msg, cause);
    }
}
