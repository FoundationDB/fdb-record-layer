/*
 * LinkedIdentitySet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A map of items that uses reference equality to determine equivalence for the
 * purposes of set membership, rather than the {@link #equals(Object)} method used by the Java {@link Set} interface.
 * @param <K> key type
 * @param <V> value type
 */
public final class LinkedIdentityMap<K, V> extends AbstractMap<K, V> {
    @Nonnull
    private static final Equivalence<Object> identity = Equivalence.identity();

    @Nonnull
    private final Map<Equivalence.Wrapper<K>, V> map;

    @Nonnull
    private final Supplier<Set<Entry<K, V>>> entrySetSupplier;

    public LinkedIdentityMap() {
        this(ImmutableMap.of());
    }

    public LinkedIdentityMap(@Nonnull final Map<K, V> sourceMap) {
        this.map = Maps.newLinkedHashMap();
        this.entrySetSupplier = Suppliers.memoize(this::computeEntrySet);
        putAll(sourceMap);
    }

    @Nonnull
    private Set<Entry<K, V>> computeEntrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<Equivalence.Wrapper<K>, V>> iterator = map.entrySet().iterator();

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    @Nonnull
                    public Entry<K, V> next() {
                        final Entry<Equivalence.Wrapper<K>, V> next = iterator.next();
                        return new SimpleEntry<>(next.getKey().get(), next.getValue());
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }

            @Override
            public int size() {
                return map.size();
            }

            @Override
            public boolean isEmpty() {
                return map.isEmpty();
            }

            @Override
            public void clear() {
                map.clear();
            }

            @Override
            public boolean contains(Object k) {
                return map.containsKey(identity.wrap(k));
            }
        };
    }

    @Override
    public boolean containsKey(final Object key) {
        return map.containsKey(identity.wrap(key));
    }

    @Override
    public V get(final Object key) {
        return map.get(identity.wrap(key));
    }

    @Override
    public V put(final K key, final V value) {
        return map.put(identity.wrap(key), value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return entrySetSupplier.get();
    }

    @Override
    public void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function) {
        map.replaceAll((wrappedK, v) -> function.apply(wrappedK.get(), v));
    }

    @Override
    public V remove(final Object key) {
        return map.remove(identity.wrap(key));
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof LinkedIdentityMap)) {
            return false;
        }
        return map.equals(((LinkedIdentityMap<?, ?>)o).map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }
}
