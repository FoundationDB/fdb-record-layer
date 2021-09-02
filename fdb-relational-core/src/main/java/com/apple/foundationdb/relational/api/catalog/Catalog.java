/*
 * Catalog.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.RelationalException;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Representation of a metadata catalog.
 */
public interface Catalog {

    /**
     * Get the Schema Template uniquely identified by the templateId.
     *
     * @param templateId the unique id of the template.
     * @return the SchemaTemplate.
     * @throws RelationalException If there is no schema with the given id, or if something systemic goes
     *                           wrong. If there is no schema template with the given id, then the error code is NO_SUCH_SCHEMA_TEMPLATE
     */
    @Nonnull
    SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException;

    /**
     * Get a database at the specified location.
     *
     * @param dbUrl the url to locate a unique database instance, in object list data structure representing the cluster and keySpace directory for the database
     * @return the Database for that id.
     * @throws RelationalException if the Database doesn't exist, or if something else goes wrong.
     */
    @Nonnull
    RelationalDatabase getDatabase(@Nonnull URI dbUrl) throws RelationalException;

    void deleteDatabase(@Nonnull URI dbUrl) throws RelationalException;
}
