/*
 * AbstractMetadataOperationsFactory.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Skeleton implementation of a ConstantActionFactory.
 */
@ExcludeFromJacocoGeneratedReport //excluded because it doesn't do anything by default, so there's nothing to test
public abstract class AbstractMetadataOperationsFactory implements MetadataOperationsFactory {
    @Nonnull
    @Override
    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateSchemaTemplateConstantAction(template, templateProperties);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath,  @Nonnull Options constantActionOptions) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath,  constantActionOptions);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateSchemaConstantAction(dbUri, schemaName, templateId, constantActionOptions);
    }

    @Nonnull
    @Override
    public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropDatabaseConstantAction(dbUrl, throwIfDoesNotExist, options);
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schema, @Nonnull Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropSchemaConstantAction(dbPath, schema, options);
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropSchemaTemplateConstantAction(templateId, throwIfDoesNotExist, options);
    }

}
