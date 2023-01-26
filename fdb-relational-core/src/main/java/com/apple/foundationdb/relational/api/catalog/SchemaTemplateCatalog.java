/*
 * SchemaTemplateCatalog.java
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

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;

import javax.annotation.Nonnull;

/**
 * A Catalog for holding Schema Templates.
 */
public interface SchemaTemplateCatalog {

    /**
     * Checks if the schema template exists.
     *
     * @param txn        the transaction
     * @param templateName the name of the template to be checked
     * @return whether the template exists.
     * @throws RelationalException if there is an exception with error code other than {@link ErrorCode#UNKNOWN_SCHEMA_TEMPLATE}
     */
    boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException;

    /**
     * Checks if a specific version of the schema template exists.
     *
     * @param txn        the transaction
     * @param templateName the name of the template to be checked
     * @param version the version of the template to be checked
     * @return whether the template exists.
     * @throws RelationalException if there is an exception with error code other than {@link ErrorCode#UNKNOWN_SCHEMA_TEMPLATE}
     */
    boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException;

    /**
     * Load the latest version of the schema template.
     *
     * @param txn        the transaction
     * @param templateName the name of the template to be loaded
     * @return whether the template exists.
     * @throws RelationalException with {@link ErrorCode#UNKNOWN_SCHEMA_TEMPLATE} if the template cannot be found,
     *                           or other error code if something else goes wrong.
     */
    @Nonnull
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException;

    /**
     * Load a specific version of the schema template.
     *
     * @param txn        the transaction
     * @param templateName the name of the template to be loaded
     * @param version the version of the template to be loaded
     * @return whether the template exists.
     * @throws RelationalException with {@link ErrorCode#UNKNOWN_SCHEMA_TEMPLATE} if the template cannot be found,
     *                           or other error code if something else goes wrong.
     */@Nonnull
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException;

    /**
     * Update the Schema template in the catalog associated with the templateId.
     *
     * <p>
     *     If no template with the specified name exists, it will be created with the version number specified in the template.
     *
     * @param txn the transaction to use
     * @param newTemplate the template to update with
     * @throws RelationalException if something goes wrong, with an appropriate error code.
     */
    void updateTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate newTemplate) throws RelationalException;

    RelationalResultSet listTemplates(@Nonnull Transaction txn);

    /**
     * Deletes all versions of the schema template.
     *
     * @param txn the transaction to use
     * @param templateName the template to update with
     */
    void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateName);

    /**
     * Deletes a specific version of the schema template, if exists.
     *
     * @param txn the transaction to use
     * @param templateName the template to update with
     * @throws RelationalException if something goes wrong, with an appropriate error code.
     */
    void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException;
}
