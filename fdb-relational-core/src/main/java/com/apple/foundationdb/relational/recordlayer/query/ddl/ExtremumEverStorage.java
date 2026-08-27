/*
 * ExtremumEverStorage.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.metadata.IndexTypes;

import javax.annotation.Nonnull;

/**
 * Which form a {@code MIN_EVER} or {@code MAX_EVER} index stores its extremum in, and the index types that form implies.
 */
public enum ExtremumEverStorage {
    /**
     * The long-based index types, which take a numeric column only. What {@code LEGACY_EXTREMUM_EVER} asks for.
     */
    LONG(IndexTypes.MIN_EVER_LONG, IndexTypes.MAX_EVER_LONG),
    /**
     * The tuple-based index types, which take a column of any type.
     */
    TUPLE(IndexTypes.MIN_EVER_TUPLE, IndexTypes.MAX_EVER_TUPLE);

    @Nonnull
    private final String minEverIndexType;
    @Nonnull
    private final String maxEverIndexType;

    ExtremumEverStorage(@Nonnull final String minEverIndexType, @Nonnull final String maxEverIndexType) {
        this.minEverIndexType = minEverIndexType;
        this.maxEverIndexType = maxEverIndexType;
    }

    @Nonnull
    public String minEverIndexType() {
        return minEverIndexType;
    }

    @Nonnull
    public String maxEverIndexType() {
        return maxEverIndexType;
    }

    /**
     * Whether this form can only store a numeric column.
     */
    public boolean isNumericOnly() {
        return this == LONG;
    }

    /**
     * The storage form the {@code LEGACY_EXTREMUM_EVER} index attribute asks for.
     *
     * @param useLegacyExtremumEver whether the definition carried the attribute
     *
     * @return the storage form to use
     */
    @Nonnull
    public static ExtremumEverStorage ofLegacyAttribute(final boolean useLegacyExtremumEver) {
        return useLegacyExtremumEver ? LONG : TUPLE;
    }
}
