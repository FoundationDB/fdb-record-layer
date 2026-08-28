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

import java.net.URI;

/**
 * Skeleton implementation of a ConstantActionFactory.
 */
public abstract class AbstractMetadataOperationsFactory implements MetadataOperationsFactory {
    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(SchemaTemplate template, Options templateProperties) {
        return NoOpMetadataOperationsFactory.INSTANCE.getSaveSchemaTemplateConstantAction(template, templateProperties);
    }

    @Override
    public ConstantAction getCreateDatabaseConstantAction(URI dbPath,  Options constantActionOptions) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath,  constantActionOptions);
    }

    @Override
    public ConstantAction getCreateSchemaConstantAction(URI dbUri, String schemaName, String templateId, Options constantActionOptions) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateSchemaConstantAction(dbUri, schemaName, templateId, constantActionOptions);
    }

    @Override
    public ConstantAction getDropDatabaseConstantAction(URI dbUrl, boolean throwIfDoesNotExist, Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropDatabaseConstantAction(dbUrl, throwIfDoesNotExist, options);
    }

    @Override
    public ConstantAction getDropSchemaConstantAction(URI dbPath, String schema, Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropSchemaConstantAction(dbPath, schema, options);
    }

    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(String templateId, boolean throwIfDoesNotExist, Options options) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropSchemaTemplateConstantAction(templateId, throwIfDoesNotExist, options);
    }

    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(final SchemaTemplate template, boolean throwIfExists,
                                                                   final RecordLayerInvokedRoutine invokedRoutine) {
        return NoOpMetadataOperationsFactory.INSTANCE.getCreateTemporaryFunctionConstantAction(template, throwIfExists, invokedRoutine);
    }

    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists,
                                                                 final String temporaryFunctionName) {
        return NoOpMetadataOperationsFactory.INSTANCE.getDropTemporaryFunctionConstantAction(throwIfNotExists, temporaryFunctionName);
    }
}
