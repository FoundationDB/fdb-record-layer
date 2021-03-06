/*
 * MemorySortAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.Comparator;

/**
 * Provide various options for {@link MemorySorter} and {@link MemorySortCursor}.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public interface MemorySortAdapter<K, V> extends Comparator<K> {
    /**
     * Generate a sorting key for the given value.
     * @param value value to be sorted
     * @return the key for {@code value}
     */
    @Nonnull
    K generateKey(V value);

    /**
     * Convert key into a byte array.
     * The desired sort order of keys must be congruent to unsigned byte order of the serialized form, up to
     * {@link #isSerializedOrderReversed}.
     * @param key the key
     * @return a byte array encoding {@code key}
     */
    @Nonnull
    byte[] serializeKey(K key);

    /**
     * Get whether the unsigned byte comparison order of values returned by {@link #serializeKey} is the same
     * as {@link #compare}'s order or its reverse.
     * @return {@code true} if the byte comparison order is the reverse of the key order
     */
    boolean isSerializedOrderReversed();

    /**
     * Convert serialized key back to key.
     * @param key the serialized form of a key
     * @return the original key
     */
    @Nonnull
    K deserializeKey(@Nonnull byte[] key);

    /**
     * Convert value into a byte array.
     * @param value the value
     * @return a byte array encoding {@code value}
     */
    @Nonnull
    byte[] serializeValue(V value);

    /**
     * Convert serialized value back into value.
     * @param value the serialized form of a value
     * @return the original value
     */
    @Nonnull
    V deserializeValue(@Nonnull byte[] value);

    /**
     * Get the maximum allowed size of the in-memory map.
     * @return the maximum size
     */
    int getMaxMapSize();

    /**
     * Get action to perform when in-memory map is full.
     * @return size limit mode
     */
    @Nonnull
    MemorySorter.SizeLimitMode getSizeLimitMode();
}
