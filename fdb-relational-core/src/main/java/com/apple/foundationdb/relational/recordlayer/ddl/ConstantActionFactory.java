/*
 * ConstantActionFactory.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;

@ThreadSafe
public interface ConstantActionFactory {

    @Nonnull
    ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties);

    @Nonnull
    ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull DatabaseTemplate template, @Nonnull Options constantActionOptions);

    @Nonnull
    ConstantAction getCreateSchemaConstantAction(@Nonnull URI schemaUrl, @Nonnull String templateId, Options constantActionOptions);

    @Nonnull
    ConstantAction getDeleteDatabaseContantAction(@Nonnull URI dbUrl, @Nonnull Options options);

    @Nonnull
    ConstantAction getDropSchemaConstantAction(@Nonnull URI schemaUrl, @Nonnull Options options);

    /**
     * Create a ConstantAction which maps the schema at the specified URI to the specified template.
     *
     * This is primarily for the use case where the underlying schema metadata store is not persistent, and we
     * need to rebuild the individual mappings dynamically (i.e. not for true production use).
     *
     * Note that if the schema is already mapped to a template, then this will throw an error.
     *
     * @param schemaUrl the path to the schema to map
     * @param templateId the unique name of the template which is to be mapped.
     * @param constantActionOptions any options required for the mapping.
     * @return a constant action which will map the schema to the template.
     * @throws com.apple.foundationdb.relational.api.exceptions.RelationalException if the schema is already mapped to a template, if the template
     * does not exist, or if something else goes wrong.
     */
    @Nonnull
    ConstantAction getMapSchemaToTemplateConstantAction(@Nonnull URI schemaUrl, @Nonnull String templateId, Options constantActionOptions);
}
