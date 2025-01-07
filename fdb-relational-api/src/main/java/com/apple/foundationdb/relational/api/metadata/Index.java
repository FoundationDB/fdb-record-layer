/*
 * Index.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import javax.annotation.Nonnull;

/**
 * An {@code Index} metadata that contains information an underlying index data structure of a {@link Table}.
 *
 * <p>
 * This currently targets only table-specific indexes and not universal indexes. However, it can be
 * extended to cover universal indexes if need be.
 * </p>
 */
public interface Index extends Metadata {

    /**
     * Returns the name of the {@link Table} that owns the {@code index}.
     *
     * @return The name of the {@link Table} that owns the {@code index}.
     */
    @Nonnull
    String getTableName();

    /**
     * Returns the type of the index. An index type could be one of the types defined in
     * {@code com.apple.foundationdb.record.metadata.IndexTypes}.
     *
     * @return the type of the index.
     *
     * @implNote (yhatem) this currently returns the type as a {@code String} which is error-prune.
     *           instead we should an {@code enum} structure.
     * TODO (yhatem) return {@code enum} instead.
     */
    @Nonnull
    String getIndexType();

    /**
     * Checks whether the index is unique.
     *
     * @return {@code True} if the index is unique, otherwise {@code False}.
     */
    boolean isUnique();

    /**
     * Checks whether the index is sparse or not.
     *
     * @return {@code True} if the index is sparse, otherwise {@code False}.
     */
    boolean isSparse();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);
    }
}
