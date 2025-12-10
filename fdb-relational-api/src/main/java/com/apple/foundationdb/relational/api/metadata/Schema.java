/*
 * Schema.java
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
import java.util.Set;

/**
 * Metadata for a Relational {@code Schema}.
 */
public interface Schema extends Metadata {

    /**
     * returns the {@link SchemaTemplate} from which {@code this} {@link Schema} is generated.
     *
     * @return The {@link SchemaTemplate} from which {@code this} {@link Schema} is generated.
     */
    @Nonnull
    SchemaTemplate getSchemaTemplate();

    /**
     * Returns the ID of the database which {@code this} {@link Schema} belong to.
     *
     * @return The ID of the database which {@code this} {@link Schema} belong to.
     */
    @Nonnull
    String getDatabaseName();

    /**
     * Returns the tables inside the {@code Schema}.
     *
     * @return The tables inside the {@code Schema}.
     * @throws RelationalException if the schema template is NoOpSchemaTemplate
     */
    @Nonnull
    default Set<? extends Table> getTables() throws RelationalException {
        return getSchemaTemplate().getTables();
    }

    /**
     * Returns the views inside the {@code Schema}.
     *
     * @return The views inside the {@code Schema}.
     * @throws RelationalException if the schema template is NoOpSchemaTemplate
     */
    @Nonnull
    default Set<? extends View> getViews() throws RelationalException {
        return getSchemaTemplate().getViews();
    }

    /**
     * Returns a list of all table-scoped {@link Index}es in this schema.
     *
     * @return a multi-map whose key is the {@link Table} name, and value(s) is the {@link Index}.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    default Multimap<String, String> getIndexes() throws RelationalException {
        return getSchemaTemplate().getTableIndexMapping();
    }

    /**
     * Returns all {@link InvokedRoutine}s defined in this schema.
     *
     * @return A set of all {@link InvokedRoutine}s defined in this schema.
     * @throws RelationalException If there was an error retrieving the invoked routines from the catalog.
     */
    @Nonnull
    default Set<? extends InvokedRoutine> getInvokedRoutines() throws RelationalException {
        return getSchemaTemplate().getInvokedRoutines();
    }

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);
    }
}
