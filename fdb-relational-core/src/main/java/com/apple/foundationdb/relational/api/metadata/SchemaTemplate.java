/*
 * SchemaTemplate.java
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * A Relational schema template metadata.
 */
public interface SchemaTemplate extends Metadata {

    /**
     * Returns the version of the schema template.
     *
     * @return The version of the schema template.
     */
    int getVersion();

    /**
     * Returns {@code true} if long rows are permitted in tables of this template, else {@code false}.
     *
     * @return {@code true} if long rows are permitted in tables of this template, else {@code false}.
     */
    boolean isEnableLongRows();

    /**
     * Returns the {@link Table}s inside the schema template.
     *
     * @return The {@link Table}s inside the schema template.
     */
    @Nonnull
    Set<? extends Table> getTables();

    /**
     * Retrieves a {@link Table} by looking up its name.
     *
     * @param tableName The name of the {@link Table}.
     * @return An {@link Optional} containing the {@link Table} if it is found, otherwise {@code Empty}.
     */
    @Nonnull
    default Optional<Table> findTableByName(@Nonnull final String tableName) {
        for (final var table : getTables()) {
            if (table.getName().equals(tableName)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a list of all table-scoped {@link Index}es in the schema template.
     *
     * @return a multi-map whose key is the {@link Table} name, and value(s) is the {@link Index}.
     */
    @Nonnull
    default Multimap<String, String> getIndexes() {
        final var result = ImmutableSetMultimap.<String, String>builder();
        for (final var table : getTables()) {
            for (final var index : table.getIndexes()) {
                result.put(table.getName(), index.getName());
            }
        }
        return result.build();
    }

    /**
     * Creates a {@link Schema} instance using the specified.
     *
     * @param databaseId The ID of the database.
     * @param schemaName The name of the {@link Schema}.
     * @return A new {@link Schema} instance with the specified name, database Id, version containing the same set of
     *         {@link Table}s in {@code this} {@link SchemaTemplate}.
     */
    @Nonnull
    Schema generateSchema(@Nonnull final String databaseId, @Nonnull final String schemaName);

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.startVisit(this);

        visitor.visit(this);

        for (final var table : getTables()) {
            table.accept(visitor);
        }

        visitor.finishVisit(this);
    }

    @Nonnull
    <T extends SchemaTemplate> T unwrap(@Nonnull final Class<T> iface) throws RelationalException;
}
