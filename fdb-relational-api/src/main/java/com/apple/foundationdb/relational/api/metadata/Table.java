/*
 * Table.java
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
import java.util.Collection;
import java.util.Set;

/**
 * A Relational metadata of a table.
 */
public interface Table extends Metadata {

    @Nonnull
    Set<? extends Index> getIndexes();

    // TODO (yhatem) implement this.
//    @Nonnull
//    List<Column> getPrimaryKey();

    /**
     * Retrieves a list of the table {@link Column}s.
     *
     * @return A list of the table {@link Column}s.
     */
    @Nonnull
    Collection<? extends Column> getColumns();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);

        for (final var index : getIndexes()) {
            index.accept(visitor);
        }

        for (final var column : getColumns()) {
            column.accept(visitor);
        }
    }

    /**
     * Returns the {@link DataType} of the {@link Table}.
     *
     * @return The {@link DataType} of the {@link Table}.
     * @apiNote Because of our nested {@link DataType} model, {@link Table}s have a compliant {@link DataType} which
     * is a {@link DataType.StructType}.
     */
    @Nonnull
    DataType.StructType getDatatype();
}
