/*
 * CorrelationIdentifier.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.SymbolDebugger;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * A correlation identifier is an immutable object that is created with a string uniquely identifying it.
 */
@API(API.Status.EXPERIMENTAL)
public class CorrelationIdentifier {

    @Nonnull
    private final String id;

    /**
     * Create a new correlation identifier using the given string. It is the callers responsibility to only use
     * unique string values in order to avoid clashes.
     * @param id the identifier string
     * @return a new {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier of(@Nonnull final String id) {
        return new CorrelationIdentifier(id);
    }

    /**
     * Create a new correlation identifier. The returned correlation identifier can be assumed
     * to be unique.
     * @return a new unique {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier uniqueId() {
        return uniqueId(CorrelationIdentifier.class);
    }

    /**
     * Create a new correlation identifier. The returned correlation identifier can be assumed
     * to be unique.
     * @param clazz to specify the kind of entity this identifier is going to be used for. This is really only useful
     *        if a {@link Debugger} is set.
     * @return a new unique {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier uniqueId(@Nonnull final Class<?> clazz) {
        return uniqueId(clazz, clazz.getSimpleName().substring(0, 1).toLowerCase(Locale.ROOT));
    }

    /**
     * Create a new correlation identifier. The returned correlation identifier can be assumed
     * to be unique for the given class.
     * @param clazz to specify the kind of entity this identifier is going to be used for. This is really only useful
     *        if a {@link Debugger} is set.
     * @param prefix a prefix for the returned identifier
     * @return a new unique {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier uniqueId(@Nonnull final Class<?> clazz, @Nonnull final String prefix) {
        final CorrelationIdentifier id =
                SymbolDebugger.getIndexOptional(clazz)
                        .map(i -> CorrelationIdentifier.of(prefix + i))
                        .orElseGet(() -> new CorrelationIdentifier(ProtoUtils.uniqueName(prefix)));
        SymbolDebugger.updateIndex(clazz, i -> i + 1);
        return id;
    }

    /**
     * Create a new correlation identifier. The returned correlation identifier can be assumed to be unique and stable
     * for subsequent calls. The advantage of using this approach for singletons is that this method returns rather
     * readable identifier names when a debugger is installed.
     * @param singleton to specify the singleton this identifier is going to be used for. This is really only useful
     *        if a {@link Debugger} is set.
     * @param prefix a prefix for the returned identifier
     * @return a new unique {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier uniqueSingletonID(@Nonnull final UUID singleton, @Nonnull final String prefix) {
        return SymbolDebugger.getOrRegisterSingleton(singleton)
                .map(index -> new CorrelationIdentifier(prefix + index))
                .orElseGet(() -> new CorrelationIdentifier(singleton.toString()));
    }

    private CorrelationIdentifier(@Nonnull final String id) {
        this.id = id;
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CorrelationIdentifier that = (CorrelationIdentifier)o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Returns the backing string id.
     * @return the string backing this {@link CorrelationIdentifier}
     */
    @Override
    public String toString() {
        return id;
    }

    /**
     * Return a mapping between {@code a -> a} for all a in the given set as a view on the original set.
     * @param aliases set to compute the identity mappings for
     * @return a view on the set that maps each element in {@code aliases} to itself.
     */
    public static Map<CorrelationIdentifier, CorrelationIdentifier> identityMappingMap(@Nonnull final Set<CorrelationIdentifier> aliases) {
        return new Map<CorrelationIdentifier, CorrelationIdentifier>() {
            @Override
            public int size() {
                return aliases.size();
            }

            @Override
            public boolean isEmpty() {
                return aliases.isEmpty();
            }

            @SuppressWarnings("SuspiciousMethodCalls")
            @Override
            public boolean containsKey(final Object key) {
                return aliases.contains(key);
            }

            @SuppressWarnings("SuspiciousMethodCalls")
            @Override
            public boolean containsValue(final Object value) {
                return aliases.contains(value);
            }

            @SuppressWarnings("java:S1905")
            @Nullable
            @Override
            public CorrelationIdentifier get(final Object key) {
                if (containsKey(key)) {
                    return (CorrelationIdentifier)key;
                }
                return null;
            }

            @Override
            public CorrelationIdentifier put(final CorrelationIdentifier key, final CorrelationIdentifier value) {
                throw new UnsupportedOperationException("mutation is not allowed");
            }

            @Override
            public CorrelationIdentifier remove(final Object key) {
                throw new UnsupportedOperationException("mutation is not allowed");
            }

            @Override
            public void putAll(@Nonnull final Map<? extends CorrelationIdentifier, ? extends CorrelationIdentifier> m) {
                throw new UnsupportedOperationException("mutation is not allowed");
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("mutation is not allowed");
            }

            @Nonnull
            @Override
            public Set<CorrelationIdentifier> keySet() {
                return aliases;
            }

            @Nonnull
            @Override
            public Collection<CorrelationIdentifier> values() {
                return aliases;
            }

            @Nonnull
            @Override
            public Set<Entry<CorrelationIdentifier, CorrelationIdentifier>> entrySet() {
                return aliases.stream()
                        .map(element -> new AbstractMap.SimpleImmutableEntry<>(element, element))
                        .collect(ImmutableSet.toImmutableSet());
            }
        };
    }
}
