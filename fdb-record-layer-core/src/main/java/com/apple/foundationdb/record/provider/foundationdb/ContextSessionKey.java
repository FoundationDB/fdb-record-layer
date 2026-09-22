/*
 * ContextSessionKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A typed key for well-known values stored in the {@link FDBRecordContext} session data map.
 * Keys are defined as {@code public static final} constants in this class.
 * The constructor is package protected so no external code can introduce new keys.
 *
 * <p>Use {@link FDBRecordContext#getInSession(ContextSessionKey)} to retrieve values.</p>
 *
 * @param <T> the value type for this key
 */
@API(API.Status.EXPERIMENTAL)
public final class ContextSessionKey<T> {
    /**
     * Session key for the set of write-only index names updated in this transaction.
     * Value type: {@code Set<String>}. Returns {@code null} if no write-only index was updated.
     * Useful for diagnosing conflicts that may happen when an index is updated by the indexer.
     */
    public static final ContextSessionKey<Set<String>> WRITE_ONLY_INDEXES_UPDATED = new ContextSessionKey<>("writeOnlyIndexesUpdated");
    /**
     * Session key for the set of write-only-with-queue index names updated in this transaction.
     * Value type: {@code Set<String>}. Returns {@code null} if no write-only-with-queue index was updated.
     * Useful for diagnosing conflicts that may happen when an index is updated by the indexer.
     */
    public static final ContextSessionKey<Set<String>> WRITE_ONLY_WITH_QUEUE_INDEXES_UPDATED = new ContextSessionKey<>("writeOnlyIndexesUpdated");
    /**
     * Session key for the set of readable index names updated in this transaction.
     * Note that this captures both {@link com.apple.foundationdb.record.IndexState#READABLE} and
     * {@link com.apple.foundationdb.record.IndexState#READABLE_UNIQUE_PENDING}.
     * Value type: {@code Set<String>}. Returns {@code null} if no readable index was updated.
     */
    public static final ContextSessionKey<Set<String>> READABLE_INDEXES_UPDATED = new ContextSessionKey<>("readableIndexesUpdated");

    @Nonnull
    private final String name;

    ContextSessionKey(@Nonnull String name) {
        this.name = name;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    T cast(@Nullable Object value) {
        return (T)value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContextSessionKey)) {
            return false;
        }
        final ContextSessionKey<?> that = (ContextSessionKey<?>)o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
