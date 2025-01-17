/*
 * MemoryDam.java
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
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Collect keyed values into a {@link LinkedHashMap} so that they end up sorted in insertion order.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class MemoryDam<K, V> extends MemoryScratchpad<K, V, Map<K, V>> {
    public MemoryDam(@Nonnull final MemorySortAdapter<K, V> adapter, @Nullable final StoreTimer timer) {
        super(adapter, new LinkedHashMap<>(), timer);
    }

    @Override
    public void removeLast(@Nonnull final K currentKey) {
        // insertion order -- we remove the current one
        getMap().remove(currentKey);
    }

    @Nonnull
    @Override
    public Collection<V> tailValues(@Nullable final K minimumKey) {
        return new AbstractCollection<>() {
            private final Supplier<Set<V>> filteredEntriesSupplier = Suppliers.memoize(() -> filteredEntries(getMap(), minimumKey));

            @Override
            public Iterator<V> iterator() {
                return filteredEntriesSupplier.get().iterator();
            }

            @Override
            public int size() {
                return filteredEntriesSupplier.get().size();
            }
        };
    }

    private static <K, V, M extends Map<K, V>> Set<V> filteredEntries(@Nonnull M map, @Nullable final K minimumKey) {
        final LinkedHashSet<V> filteredEntries = new LinkedHashSet<>();
        boolean hasSeenMinimumKey = false;
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            if (hasSeenMinimumKey) {
                filteredEntries.add(entry.getValue());
            } else {
                if (minimumKey == null || entry.getKey().equals(minimumKey)) {
                    hasSeenMinimumKey = true;
                    // do not add entry for minimum key
                }
            }
        }
        return filteredEntries;
    }
}
