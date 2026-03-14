/*
 * RecordLayerMetadataOperationsFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.ddl.SaveSchemaTemplateConstantAction;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;

import javax.annotation.Nonnull;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerMetadataOperationsFactory implements MetadataOperationsFactory {
    private final RecordLayerConfig rlConfig;
    private final StoreCatalog catalog;
    private final KeySpace baseKeySpace;

    public RecordLayerMetadataOperationsFactory(RecordLayerConfig rlConfig, StoreCatalog catalog, KeySpace baseKeySpace) {
        this.rlConfig = rlConfig;
        this.catalog = catalog;
        this.baseKeySpace = baseKeySpace;
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return txn -> {
            catalog.getSchemaTemplateCatalog().deleteTemplate(txn, templateId, throwIfDoesNotExist);
        };
    }

    @Nonnull
    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        return new SaveSchemaTemplateConstantAction(template, catalog.getSchemaTemplateCatalog());
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
        return new CreateDatabaseConstantAction(dbPath, catalog, baseKeySpace);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
        return new RecordLayerCreateSchemaConstantAction(dbUri, schemaName, templateId, rlConfig, baseKeySpace, catalog);
    }

    @Nonnull
    @Override
    public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return new DropDatabaseConstantAction(dbUrl, throwIfDoesNotExist, catalog, this, options);
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schema, @Nonnull Options options) {
        return new DropSchemaConstantAction(dbPath, schema, baseKeySpace, catalog);
    }

    @Nonnull
    public ConstantAction getSetStoreStateConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName) {
        return new RecordLayerSetStoreStateConstantAction(dbUri, schemaName, rlConfig, baseKeySpace, catalog);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull SchemaTemplate template,
                                                                   boolean throwIfExists,
                                                                   @Nonnull RecordLayerInvokedRoutine invokedRoutine) {
        return new CreateTemporaryFunctionConstantAction(template, throwIfExists, invokedRoutine);
    }

    @Nonnull
    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists,
                                                                 @Nonnull final String temporaryFunctionName) {
        return new DropTemporaryFunctionConstantAction(throwIfNotExists, temporaryFunctionName);
    }

    public static class Builder {
        protected StoreCatalog storeCatalog;
        protected RecordLayerConfig rlConfig;
        protected KeySpace baseKeySpace;

        public Builder setStoreCatalog(StoreCatalog storeCatalog) {
            this.storeCatalog = storeCatalog;
            return this;
        }

        public Builder setRlConfig(RecordLayerConfig rlConfig) {
            this.rlConfig = rlConfig;
            return this;
        }

        public Builder setBaseKeySpace(KeySpace baseKeySpace) {
            this.baseKeySpace = baseKeySpace;
            return this;
        }

        public RecordLayerMetadataOperationsFactory build() {
            return new RecordLayerMetadataOperationsFactory(rlConfig, storeCatalog, baseKeySpace);
        }
    }

    public static Builder defaultFactory() {
        return new Builder();
    }
}
