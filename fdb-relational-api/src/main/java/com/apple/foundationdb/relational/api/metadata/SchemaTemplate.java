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

import com.google.common.collect.Multimap;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;
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
     * Returns {@code true} if each row is stored along with an monotonically increasing version. This
     * is required to make use of the {@code row_version()} function within queries and indexes.
     *
     * @return {@code true} if each row is stored along with a monotonically increasing {@code row_version()}.
     */
    boolean isStoreRowVersions();

    /**
     * Returns the {@link Table}s inside the schema template.
     *
     * @return The {@link Table}s inside the schema template.
     * @throws RelationalException if it is a NoOpSchemaTemplate
     */
    @Nonnull
    Set<? extends Table> getTables() throws RelationalException;

    /**
     * Returns the {@link View}s inside the schema template.
     *
     * @return The {@link View}s inside the schema template.
     * @throws RelationalException if it is a NoOpSchemaTemplate
     */
    @Nonnull
    Set<? extends View> getViews() throws RelationalException;

    /**
     * Retrieves a {@link Table} by looking up its name.
     *
     * @param tableName The name of the {@link Table}.
     * @return An {@link Optional} containing the {@link Table} if it is found, otherwise {@code Empty}.
     */
    @Nonnull
    Optional<Table> findTableByName(@Nonnull String tableName) throws RelationalException;

    @Nonnull
    Optional<? extends View> findViewByName(@Nonnull String viewName) throws RelationalException;

    @Nonnull
    Multimap<String, String> getTableIndexMapping() throws RelationalException;

    @Nonnull
    Set<String> getIndexes() throws RelationalException;

    /**
     * Returns a {@link BitSet} whose bits are set for each of passed index names.
     * <br>
     * <b>Example</b>
     * for a given schema template {@code st} with indexes {@code i1, i2, i3, i4} whose order are as-defined, calling
     * {@code st.getIndexEntriesAsBitset(Optiona.of(Set.of("i2", "i3"))} returns a bitset of {@code 0110}.
     *
     * @param readableIndexNames The readable index names, providing an {@link Optional#empty()} returns a bitset with
     *                           bits set to {@code 1}.
     * @return a bit set whose bits are set for each of the passed readable index names.
     * @throws RelationalException If a readable index is not found.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    BitSet getIndexEntriesAsBitset(@Nonnull Optional<Set<String>> readableIndexNames) throws RelationalException;

    /**
     * Returns all {@link InvokedRoutine}s defined in this schema template.
     *
     * @return A set of all {@link InvokedRoutine}s defined in this schema template.
     * @throws RelationalException If there was an error retrieving the invoked routines from the catalog.
     */
    @Nonnull
    Set<? extends InvokedRoutine> getInvokedRoutines() throws RelationalException;

    /**
     * Retrieves a {@link InvokedRoutine} by looking up its name.
     *
     * @param routineName The name of the {@link InvokedRoutine}.
     * @return An {@link Optional} containing the {@link InvokedRoutine} if it is found, otherwise {@code Empty}.
     */
    @Nonnull
    Optional<? extends InvokedRoutine> findInvokedRoutineByName(@Nonnull String routineName) throws RelationalException;

    @Nonnull
    Collection<? extends InvokedRoutine> getTemporaryInvokedRoutines() throws RelationalException;

    @Nonnull
    String getTransactionBoundMetadataAsString() throws RelationalException;

    /**
     * Creates a {@link Schema} instance using the specified.
     *
     * @param databaseId The ID of the database.
     * @param schemaName The name of the {@link Schema}.
     * @return A new {@link Schema} instance with the specified name, database Id, version containing the same set of
     * {@link Table}s in {@code this} {@link SchemaTemplate}.
     */
    @Nonnull
    Schema generateSchema(@Nonnull String databaseId, @Nonnull String schemaName);

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.startVisit(this);
        visitor.visit(this);
        visitor.finishVisit(this);
    }

    @Nonnull
    <T extends SchemaTemplate> T unwrap(@Nonnull Class<T> iface) throws RelationalException;
}
