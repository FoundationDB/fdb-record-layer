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
     * Load the specified schema template.
     *
     * @param txn        the transaction
     * @param templateName the name of the template to be loaded
     * @return whether the template exists.
     * @throws RelationalException with {@link ErrorCode#UNKNOWN_SCHEMA_TEMPLATE} if the template cannot be found,
     *                           or other error code if something else goes wrong.
     */
    boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException;

    @Nonnull
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateId) throws RelationalException;

    @Nonnull
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateId, long version) throws RelationalException;

    /**
     * Update the Schema template in the catalog associated with the templateId.
     *
     * <p>
     *     If no template with the specified name exists, then one will be created with version 1. Otherwise
     *     the version number of the existing setup will be incremented by 1
     *
     * @param txn the transaction to use
     * @param templateId the unique id for the template to update
     * @param newTemplate the template to update with
     * @throws RelationalException if something goes wrong, with an appropriate error code.
     */
    void updateTemplate(@Nonnull Transaction txn, @Nonnull String templateId, @Nonnull SchemaTemplate newTemplate) throws RelationalException;

    RelationalResultSet listTemplates(@Nonnull Transaction txn);

    void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateId);
}
