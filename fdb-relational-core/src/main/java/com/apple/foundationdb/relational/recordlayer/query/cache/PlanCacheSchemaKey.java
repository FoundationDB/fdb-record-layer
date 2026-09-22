/*
 * PlanCacheSchemaKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableSortedSet;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Primary key for {@link RelationalPlanCache}. Represents the set of schema template names that a
 * cached plan was compiled against. Single-schema queries produce a one-element set; cross-schema
 * queries produce a multi-element set.
 * <p>
 * The set is always sorted so that {@code {"a","b"}} and {@code {"b","a"}} map to the same bucket.
 */
@API(API.Status.EXPERIMENTAL)
public final class PlanCacheSchemaKey {

    @Nonnull
    private final ImmutableSortedSet<String> schemaNames;

    private final int memoizedHashCode;

    private PlanCacheSchemaKey(@Nonnull final ImmutableSortedSet<String> schemaNames) {
        this.schemaNames = schemaNames;
        this.memoizedHashCode = schemaNames.hashCode();
    }

    @Nonnull
    public ImmutableSortedSet<String> getSchemaNames() {
        return schemaNames;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return schemaNames.equals(((PlanCacheSchemaKey) other).schemaNames);
    }

    @Override
    public int hashCode() {
        return memoizedHashCode;
    }

    @Override
    public String toString() {
        return String.join("|", schemaNames);
    }

    @Nonnull
    public static PlanCacheSchemaKey of(@Nonnull final String schemaName) {
        return new PlanCacheSchemaKey(ImmutableSortedSet.of(schemaName));
    }

    @Nonnull
    public static PlanCacheSchemaKey of(@Nonnull final Collection<String> schemaNames) {
        return new PlanCacheSchemaKey(ImmutableSortedSet.copyOf(schemaNames));
    }
}
