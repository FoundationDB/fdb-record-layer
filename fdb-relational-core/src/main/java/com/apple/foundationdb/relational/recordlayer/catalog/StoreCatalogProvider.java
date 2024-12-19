/*
 * StoreCatalogProvider.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.NoOpSchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;

import javax.annotation.Nonnull;
import java.net.URI;

public class StoreCatalogProvider {

    public static StoreCatalog getCatalog(Transaction txn, KeySpace keySpace) throws RelationalException {
        final var schemaTemplateCatalog = new InMemorySchemaTemplateCatalog();
        final var storeCatalog = new RecordLayerStoreCatalogImpl(keySpace, schemaTemplateCatalog);
        storeCatalog.initialize(txn);
        return storeCatalog;
    }

    // This is used to support all our catalog migration efforts.
    public static StoreCatalog getCatalogWithNoTemplateOperations(Transaction txn) throws RelationalException {
        final var schemaTemplateCatalog = new NoOpSchemaTemplateCatalog();

        // Do not allow LoadSchema and repairSchema operations because they cannot work with NoOpSchemaTemplateCatalog
        // owing to its inability to load a requested schema template.
        final var storeCatalog = new RecordLayerStoreCatalogImpl(RelationalKeyspaceProvider.getKeySpace(), schemaTemplateCatalog) {
            @Override
            @Nonnull
            public RecordLayerSchema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
                throw new OperationUnsupportedException("This store catalog does not support loading schema.");
            }

            @Override
            public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
                throw new OperationUnsupportedException("This store catalog does not support repairing schema.");
            }
        };

        storeCatalog.initialize(txn);
        return storeCatalog;
    }
}
