/*
 * QueryPlanInfo.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class holds some additional information regarding the query plan that can be attached to the plan itself, without
 * impacting its structure. This info can be added during planning to help reason about the plan and the planning process.
 */
@API(API.Status.STABLE)
public class QueryPlanInfo {
    private final Map<QueryPlanInfoKey<?>, Object> info;

    private static final QueryPlanInfo EMPTY = new QueryPlanInfo(Collections.emptyMap());

    private QueryPlanInfo(@Nonnull final Map<QueryPlanInfoKey<?>, Object> infoMap) {
        info = infoMap;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public static QueryPlanInfo empty()  {
        return EMPTY;
    }

    @Nonnull
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Returns TRUE if the given key exists in the info table.
     *
     * @param key the key to look for
     * @param <T> The type of the value (not used in this method)
     * @return TRUE if the key exists in the table, FALSE otherwise
     */
    public <T> boolean containsKey(@Nonnull QueryPlanInfoKey<T> key) {
        return info.containsKey(key);
    }

    /**
     * Retrieve a value from the info table.
     *
     * @param key the key to look for
     * @param <T> the type of value returned (determined by the key generic type)
     * @return the value for the key, null if not found
     */
    @Nullable
    public <T> T get(@Nonnull QueryPlanInfoKey<T> key) {
        return key.narrow(info.get(key));
    }

    /**
     * Return TRUE if the info set is empty.
     * @return TRUE if the info set is empty
     */
    public boolean isEmpty() {
        return info.isEmpty();
    }

    /**
     * Return the Key Set for info map.
     * @return the Key Sey for the info map
     */
    @SuppressWarnings("java:S1452")
    @Nonnull
    public Set<QueryPlanInfoKey<?>> keySet() {
        return Collections.unmodifiableSet(info.keySet());
    }

    /**
     * An implementation of a type-safe Enum. This class can be used to qualify each of the Map entries (in the info
     * Map above) with a type, such that callers to the get() method can get a type-safe return value, defined by the
     * generic type of the {@link QueryPlanInfoKey} constant
     *
     * @param <T> the type of the value associated with the key constant value.
     */
    public static class QueryPlanInfoKey<T> {
        @Nonnull
        private final String name;

        public QueryPlanInfoKey(@Nonnull String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        // Suppress Unchecked Cast exception since all put() into the map use the right type for the value from the key.
        @SuppressWarnings("unchecked")
        public T narrow(@Nonnull Object o) {
            return (T) o;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueryPlanInfoKey<?> that = (QueryPlanInfoKey<?>) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * A Builder for the {@link QueryPlanInfo} class. Since the plan info is immutable, this builder is used to populate
     * the map with content. Once built, the {@link QueryPlanInfo} cannot be modified.
     */
    public static class Builder {
        private final Map<QueryPlanInfoKey<?>, Object> infoMap;

        private Builder() {
            infoMap = new HashMap<>();
        }

        private Builder(QueryPlanInfo source) {
            infoMap = new HashMap<>(source.info);
        }

        /**
         * Set a value for the given key.
         *
         * @param key   the key to use
         * @param value the value to associate with the key
         * @param <T>   the type of the value to set (determined by the Key generic type)
         * @return this
         */
        @Nonnull
        public <T> Builder put(@Nonnull QueryPlanInfoKey<T> key, @Nonnull T value) {
            infoMap.put(key, value);
            return this;
        }

        @Nullable
        public <T> T get(@Nonnull QueryPlanInfoKey<T> key) {
            return key.narrow(infoMap.get(key));
        }

        public QueryPlanInfo build() {
            return new QueryPlanInfo(infoMap);
        }
    }
}
