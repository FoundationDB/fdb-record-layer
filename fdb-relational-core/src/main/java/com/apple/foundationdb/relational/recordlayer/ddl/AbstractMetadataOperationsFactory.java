/*
 * AbstractMetadataOperationsFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Skeleton implementation of a ConstantActionFactory.
 */
public abstract class AbstractMetadataOperationsFactory implements MetadataOperationsFactory {
    @Nonnull
    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        return NoOpMetadataOperationsFactory.INSTANCE.getSaveSchemaTemplateConstantAction(template, templateProperties);
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

    @Nonnull
    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull final SchemaTemplate template, boolean throwIfExists,
                                                                   @Nonnull final RecordLayerInvokedRoutine invokedRoutine) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateTemporaryFunctionConstantAction(template, throwIfExists, invokedRoutine);
    }

    @Nonnull
    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists,
                                                                 @Nonnull final String temporaryFunctionName) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropTemporaryFunctionConstantAction(throwIfNotExists, temporaryFunctionName);
    }
}
