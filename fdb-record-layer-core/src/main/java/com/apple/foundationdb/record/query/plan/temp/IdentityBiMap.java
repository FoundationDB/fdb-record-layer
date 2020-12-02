/*
 * IdentityBiMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.google.common.base.Equivalence;
import com.google.common.base.Equivalence.Wrapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * BiMap based on identities of types {@code K} and {@code V}.
 * @param <K> key type
 * @param <V> value type
 */
public class IdentityBiMap<K, V> implements BiMap<Wrapper<K>, Wrapper<V>> {

    private static final Equivalence<Object> identity = Equivalence.identity();

    private final BiMap<Wrapper<K>, Wrapper<V>> delegate;

    private IdentityBiMap(final BiMap<Wrapper<K>, Wrapper<V>> delegate) {
        this.delegate = delegate;
    }

    protected BiMap<Wrapper<K>, Wrapper<V>> getDelegate() {
        return delegate;
    }

    @Override
    @CanIgnoreReturnValue
    @Nullable
    public Wrapper<V> put(@Nullable final Wrapper<K> key,
                          @Nullable final Wrapper<V> value) {
        return getDelegate().put(key, value);
    }

    @CanIgnoreReturnValue
    @Nullable
    public V putUnwrapped(@Nullable final K key, @Nullable final V value) {
        return unwrap(getDelegate().put(wrap(key), wrap(value)));
    }

    @Override
    @CanIgnoreReturnValue
    @Nullable
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE") // false positive due to Equivalence#wrap using a non-standard Nonnull in guava
    public Wrapper<V> forcePut(@Nullable final Wrapper<K> key,
                               @Nullable final Wrapper<V> value) {
        return getDelegate().forcePut(key, value);
    }

    @CanIgnoreReturnValue
    @Nullable
    public V forcePutUnwrapped(@Nullable final K key, @Nullable final V value) {
        return unwrap(getDelegate().forcePut(wrap(key), wrap(value)));
    }

    @Override
    public void putAll(@Nonnull final Map<? extends Wrapper<K>, ? extends Wrapper<V>> map) {
        getDelegate().putAll(map);
    }

    @Override
    @Nonnull
    public Set<Wrapper<V>> values() {
        return getDelegate().values();
    }

    @Override
    @Nonnull
    public IdentityBiMap<V, K> inverse() {
        return create(getDelegate().inverse());
    }

    @Override
    public int size() {
        return getDelegate().size();
    }

    @Override
    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    @Override
    public boolean containsKey(@Nullable final Object key) {
        return getDelegate().containsKey(key);
    }

    public boolean containsKeyUnwrapped(@Nullable final Object key) {
        return getDelegate().containsKey(wrap(key));
    }

    @Override
    public boolean containsValue(@Nullable final Object value) {
        return getDelegate().containsValue(value);
    }

    @Override
    @Nullable
    public Wrapper<V> get(@Nullable final Object key) {
        return getDelegate().get(key);
    }

    @Nullable
    public V getUnwrapped(@Nullable final Object key) {
        return unwrap(getDelegate().get(wrap(key)));
    }

    @Override
    @Nullable
    public Wrapper<V> remove(final Object key) {
        return getDelegate().remove(key);
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    @Nonnull
    public Set<Wrapper<K>> keySet() {
        return getDelegate().keySet();
    }

    @Override
    @Nonnull
    public Set<Entry<Wrapper<K>, Wrapper<V>>> entrySet() {
        return getDelegate().entrySet();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION")
    @Override
    public boolean equals(@Nullable final Object o) {
        return getDelegate().equals(o);
    }

    @Override
    public int hashCode() {
        return getDelegate().hashCode();
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    @Nullable
    public Wrapper<V> getOrDefault(@Nullable final Object key, @Nullable final Wrapper<V> defaultValue) {
        return getDelegate().getOrDefault(key, defaultValue);
    }

    @Nullable
    public V getOrDefaultUnwrapped(@Nullable final Object key, @Nullable final V defaultValue) {
        final Wrapper<Object> wrappedKey = wrap(key);
        final Wrapper<V> wrappedValue = get(wrappedKey);
        return wrappedValue == null ? defaultValue : wrappedValue.get();
    }

    @Override
    public void forEach(@Nonnull final BiConsumer<? super Wrapper<K>, ? super Wrapper<V>> action) {
        getDelegate().forEach(action);
    }

    public void forEachUnwrapped(@Nonnull final BiConsumer<? super K, ? super V> action) {
        getDelegate().forEach((wrappedKey, wrappedValue) -> action.accept(unwrap(wrappedKey), unwrap(wrappedValue)));
    }

    @Override
    public void replaceAll(@Nonnull final BiFunction<? super Wrapper<K>, ? super Wrapper<V>, ? extends Wrapper<V>> function) {
        getDelegate().replaceAll(function);
    }

    @Override
    @Nullable
    public Wrapper<V> putIfAbsent(@Nullable final Wrapper<K> key, final Wrapper<V> value) {
        return getDelegate().putIfAbsent(key, value);
    }

    @Nullable
    public Wrapper<V> putIfAbsentUnwrapped(@Nullable final K key, final V value) {
        return getDelegate().putIfAbsent(wrap(key), wrap(value));
    }

    @Override
    public boolean replace(@Nullable final Wrapper<K> key, @Nullable final Wrapper<V> oldValue, @Nullable final Wrapper<V> newValue) {
        return getDelegate().replace(key, oldValue, newValue);
    }

    @Override
    @Nullable
    public Wrapper<V> replace(@Nullable final Wrapper<K> key, final Wrapper<V> value) {
        return getDelegate().replace(key, value);
    }

    @Override
    @Nullable
    public Wrapper<V> computeIfAbsent(@Nullable final Wrapper<K> key, @Nonnull final Function<? super Wrapper<K>, ? extends Wrapper<V>> mappingFunction) {
        return getDelegate().computeIfAbsent(key, mappingFunction);
    }

    @Override
    @Nullable
    public Wrapper<V> computeIfPresent(@Nullable final Wrapper<K> key, @Nonnull final BiFunction<? super Wrapper<K>, ? super Wrapper<V>, ? extends Wrapper<V>> remappingFunction) {
        return getDelegate().computeIfPresent(key, remappingFunction);
    }

    @Override
    @Nullable
    public Wrapper<V> compute(@Nullable final Wrapper<K> key, @Nonnull final BiFunction<? super Wrapper<K>, ? super Wrapper<V>, ? extends Wrapper<V>> remappingFunction) {
        return getDelegate().compute(key, remappingFunction);
    }

    @Override
    @Nullable
    public Wrapper<V> merge(@Nullable final Wrapper<K> key, @Nonnull final Wrapper<V> value, @Nonnull final BiFunction<? super Wrapper<V>, ? super Wrapper<V>, ? extends Wrapper<V>> remappingFunction) {
        return getDelegate().merge(key, value, remappingFunction);
    }

    @Nonnull
    public IdentityBiMap<K, V> toImmutable() {
        // this only copies when needed
        return new IdentityBiMap<>(ImmutableBiMap.copyOf(delegate));
    }

    @Nonnull
    public static <K, V> IdentityBiMap<K, V> create() {
        return create(HashBiMap.create());
    }

    @Nonnull
    public static <K, V> IdentityBiMap<K, V> create(final BiMap<Wrapper<K>, Wrapper<V>> delegate) {
        return new IdentityBiMap<>(delegate);
    }

    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE") // false positive due to Equivalence#wrap using a non-standard Nonnull in guava
    @Nonnull
    public static <T> Wrapper<T> wrap(@Nullable final T reference) {
        return identity.wrap(reference);
    }

    @Nullable
    public static <T> T unwrap(@Nullable final Wrapper<T> wrapper) {
        return wrapper == null ? null : wrapper.get();
    }

    public static <T, K, V> Collector<T, ?, IdentityBiMap<K, V>> toImmutableIdentityBiMap(@Nonnull final Function<? super T, ? extends K> keyMapper,
                                                                                          @Nonnull final Function<? super T, ? extends V> valueMapper,
                                                                                          @Nonnull final BinaryOperator<V> mergeFunction) {
        return new Collector<T, IdentityBiMap<K, V>, IdentityBiMap<K, V>>() {
            @Override
            public Supplier<IdentityBiMap<K, V>> supplier() {
                return IdentityBiMap::create;
            }

            @Override
            public BiConsumer<IdentityBiMap<K, V>, T> accumulator() {
                return (map, t) -> {
                    final K key = keyMapper.apply(t);
                    final V value = valueMapper.apply(t);
                    map.merge(wrap(key),
                            wrap(value),
                            (wrappedValue1, wrappedValue2) -> wrap(mergeFunction.apply(unwrap(wrappedValue1), unwrap(wrappedValue2))));
                };
            }

            @Override
            public BinaryOperator<IdentityBiMap<K, V>> combiner() {
                return (map1, map2) -> {
                    map2.forEach((wrappedKey, wrappedValue) ->
                            map1.merge(wrappedKey,
                                    wrappedValue,
                                    (wrappedValue1, wrappedValue2) -> wrap(mergeFunction.apply(unwrap(wrappedValue1), unwrap(wrappedValue2)))));
                    return map1;
                };
            }

            @Override
            public Function<IdentityBiMap<K, V>, IdentityBiMap<K, V>> finisher() {
                return IdentityBiMap::toImmutable;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return ImmutableSet.of(Characteristics.UNORDERED);
            }
        };
    }
}
