/*
 * SyntheticTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
import java.util.Set;

/**
 * Metadata for a synthetic table: a named, virtual table whose rows are derived from those of one or more stored
 * {@link Table}s, and which carries {@link Index}es maintained from writes to those tables.
 * <p>
 */
public interface SyntheticTable extends Metadata {

    /**
     * Returns the indexes maintained on this synthetic table.
     *
     * @return the indexes of this synthetic table
     */
    @Nonnull
    Set<? extends Index> getIndexes();

    /**
     * Names of the stored tables this synthetic table is derived from. Its indexes are maintained from writes to those
     * tables, so this is how they are attributed in a table-keyed view of the metadata. An unnested synthetic table has
     * exactly one; a joined one would have several.
     *
     * @return the names of the underlying stored tables
     */
    @Nonnull
    Set<String> getUnderlyingTableNames();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);

        for (final var index : getIndexes()) {
            index.accept(visitor);
        }
    }
}
