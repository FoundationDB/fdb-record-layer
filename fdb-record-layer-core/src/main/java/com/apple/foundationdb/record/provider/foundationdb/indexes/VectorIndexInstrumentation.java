/*
 * VectorIndexInstrumentation.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared instrumentation for vector index engines. Both the HNSW and Guardiann read/write listeners see the same raw
 * key/value events from the underlying FDB layer, so the generic index byte/key accounting lives here and is reused by
 * each engine's listener implementation. Engine-specific counters (HNSW node reads, Guardiann task/vector reads) stay
 * in the respective listeners.
 */
final class VectorIndexInstrumentation {
    private VectorIndexInstrumentation() {
    }

    /**
     * Records a single key/value read against the generic index-load counters.
     *
     * @param timer the timer to record against
     * @param key the key that was read
     * @param value the value that was read, or {@code null} if absent
     */
    static void recordKeyValueRead(@Nonnull final FDBStoreTimer timer, @Nonnull final byte[] key,
                                   @Nullable final byte[] value) {
        final int keyLength = key.length;
        final int valueLength = value == null ? 0 : value.length;

        timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY);
        timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, keyLength);
        timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, valueLength);
    }

    /**
     * Records a single key/value write against the generic index-save counters.
     *
     * @param timer the timer to record against
     * @param key the key that was written
     * @param value the value that was written
     */
    static void recordKeyValueWritten(@Nonnull final FDBStoreTimer timer, @Nonnull final byte[] key,
                                      @Nonnull final byte[] value) {
        timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY);
        timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES, key.length);
        timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES, value.length);
    }
}
