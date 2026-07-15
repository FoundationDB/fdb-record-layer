/*
 * VectorOptionKey.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.Index;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * A single logical vector option, identified by a stable set of wire names: one <em>canonical</em> name plus zero or
 * more <em>legacy</em> aliases that all refer to the same option. This is the single place a (current, legacy) name
 * pairing is declared; readers resolve any name to this key (preferring the canonical) and writers emit only the
 * canonical name, so a value stored long ago under a legacy name is still read while new values are written under the
 * current name.
 * <p>
 * The same key type serves both option surfaces:
 * <ul>
 * <li><b>index-time options</b> — index metadata values are strings, so {@link #read(Index)} resolves the option off an
 *     {@link Index} and parses it to {@code T} using the {@link Function parser} bundled with the key's type;</li>
 * <li><b>query-time scan options</b> — values are already stored typed, so
 *     {@link com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions} uses {@link #getType()} to cast
 *     and (de)serialize and {@link #allNames()} to resolve any wire name (including legacy aliases) back to this key.</li>
 * </ul>
 * Keys are value-typed on their canonical name: two keys are equal iff their canonical names are equal, which keeps a
 * key deserialized from a legacy wire name identical to the freshly declared canonical key (so options maps, equality,
 * and plan hashes are stable across the alias). Instances are created through the typed factories
 * ({@link #ofInteger}, {@link #ofDouble}, {@link #ofBoolean}, {@link #ofMetric}) and are intended to be declared once as
 * {@code static final} catalog entries.
 *
 * @param <T> the parsed/stored value type of the option
 */
@API(API.Status.EXPERIMENTAL)
public final class VectorOptionKey<T> {
    @Nonnull
    private final String canonicalName;
    @Nonnull
    private final ImmutableList<String> aliases;
    @Nonnull
    private final Class<T> type;
    @Nonnull
    private final Function<String, T> parser;

    private VectorOptionKey(@Nonnull final String canonicalName, @Nonnull final ImmutableList<String> aliases,
                            @Nonnull final Class<T> type, @Nonnull final Function<String, T> parser) {
        this.canonicalName = canonicalName;
        this.aliases = aliases;
        this.type = type;
        this.parser = parser;
    }

    /**
     * The canonical wire name — the only name ever written.
     * @return the canonical name
     */
    @Nonnull
    public String getCanonicalName() {
        return canonicalName;
    }

    /**
     * The canonical wire name. Alias of {@link #getCanonicalName()} kept for the scan-option surface, whose callers
     * historically read an option's serialized name via {@code getOptionName()}.
     * @return the canonical name
     */
    @Nonnull
    public String getOptionName() {
        return canonicalName;
    }

    /**
     * The value type of the option, used by the scan-option surface to cast and (de)serialize stored values.
     * @return the value type
     */
    @Nonnull
    public Class<T> getType() {
        return type;
    }

    /**
     * All wire names for this option — the canonical name first, then any legacy aliases. Used to register every name
     * that must resolve to this key and to enumerate the names to inspect when validating option changes.
     * @return the canonical name followed by the aliases
     */
    @Nonnull
    public ImmutableList<String> allNames() {
        return ImmutableList.<String>builderWithExpectedSize(aliases.size() + 1)
                .add(canonicalName)
                .addAll(aliases)
                .build();
    }

    /**
     * Reads and parses this option off an index's metadata, preferring the canonical name and falling back to each
     * legacy alias in order. Returns {@code null} when none of the names is set, leaving the caller to apply a default.
     *
     * @param index the index whose options to read
     * @return the parsed value, or {@code null} if the option is not set under any of its names
     */
    @Nullable
    public T read(@Nonnull final Index index) {
        for (final String name : allNames()) {
            final String value = index.getOption(name);
            if (value != null) {
                return parser.apply(value);
            }
        }
        return null;
    }

    @Nonnull
    public static VectorOptionKey<Integer> ofInteger(@Nonnull final String canonicalName,
                                                     @Nonnull final String... aliases) {
        return new VectorOptionKey<>(canonicalName, ImmutableList.copyOf(aliases), Integer.class, Integer::parseInt);
    }

    @Nonnull
    public static VectorOptionKey<Double> ofDouble(@Nonnull final String canonicalName,
                                                   @Nonnull final String... aliases) {
        return new VectorOptionKey<>(canonicalName, ImmutableList.copyOf(aliases), Double.class, Double::parseDouble);
    }

    @Nonnull
    public static VectorOptionKey<Boolean> ofBoolean(@Nonnull final String canonicalName,
                                                     @Nonnull final String... aliases) {
        return new VectorOptionKey<>(canonicalName, ImmutableList.copyOf(aliases), Boolean.class, Boolean::parseBoolean);
    }

    @Nonnull
    public static VectorOptionKey<Metric> ofMetric(@Nonnull final String canonicalName,
                                                   @Nonnull final String... aliases) {
        return new VectorOptionKey<>(canonicalName, ImmutableList.copyOf(aliases), Metric.class, Metric::valueOf);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VectorOptionKey)) {
            return false;
        }
        final VectorOptionKey<?> that = (VectorOptionKey<?>)o;
        return canonicalName.equals(that.canonicalName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalName);
    }

    @Override
    public String toString() {
        return canonicalName;
    }
}
