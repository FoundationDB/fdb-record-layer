/*
 * MemorySortAdapter.java
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    int getMaxRecordCountInMemory();

    /**
     * Get action to perform when in-memory map is full.
     * @return size limit mode
     */
    @Nonnull
    MemorySorter.RecordCountInMemoryLimitMode getRecordCountInMemoryLimitMode();
    
    @Nonnull
    MemorySortComparator<K> getComparator(@Nullable K minimumKey);

    /**
     * An extended version of a comparator that can be stateful.
     * @param <K> the type of the key to be compared
     */
    interface MemorySortComparator<K> extends Comparator<K> {
        int compareToMinimumKey(@Nonnull K key);
        
        @Nullable
        K nextMinimumKey();
    }

    /**
     * Stateful comparator to support orderings based on regular comparisons delegated to a {@link Comparator}.
     * @param <K> the type of key
     */
    class OrderComparator<K> implements MemorySortComparator<K> {
        @Nonnull
        final Comparator<K> comparator;

        @Nullable
        final K minimumKey;

        public OrderComparator(@Nonnull Comparator<K> comparator, @Nullable final K minimumKey) {
            this.comparator = comparator;
            this.minimumKey = minimumKey;
        }

        @Override
        public int compareToMinimumKey(@Nonnull final K key) {
            if (minimumKey == null) {
                return 1;
            }
            return comparator.compare(key, minimumKey);
        }

        @Nullable
        @Override
        public K nextMinimumKey() {
            return minimumKey;
        }

        @Override
        public int compare(@Nonnull final K o1, @Nonnull final K o2) {
            return comparator.compare(o1, o2);
        }
    }

    /**
     * Stateful comparator supporting insertion order and delegating to a regular {@link Comparator} to establish
     * equality between the minimum key and a probe.
     * @param <K> the type of key
     */
    class InsertionOrderComparator<K> implements MemorySortComparator<K> {
        @Nonnull
        final Comparator<K> comparator;

        @Nullable
        final K minimumKey;

        boolean hasSeenMinimumKey;

        public InsertionOrderComparator(@Nonnull Comparator<K> comparator, @Nullable final K minimumKey) {
            this.comparator = comparator;
            this.minimumKey = minimumKey;
            this.hasSeenMinimumKey = false;
        }

        @Override
        public int compareToMinimumKey(@Nonnull final K key) {
            if (minimumKey == null) {
                return 1;
            }
            int result = comparator.compare(key, minimumKey);
            if (result == 0) {
                hasSeenMinimumKey = true;
                return -1;
            }
            return hasSeenMinimumKey ? 1 : -1;
        }

        @Nullable
        @Override
        public K nextMinimumKey() {
            return hasSeenMinimumKey ? null : minimumKey;
        }

        @Override
        public int compare(@Nonnull final K o1, @Nonnull final K o2) {
            throw new RecordCoreException("insertion order does not rely on a Comparator");
        }
    }
}
