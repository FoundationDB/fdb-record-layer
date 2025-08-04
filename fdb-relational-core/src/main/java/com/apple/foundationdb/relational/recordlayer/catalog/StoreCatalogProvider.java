/*
 * StoreCatalogProvider.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.NoOpSchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;

import javax.annotation.Nonnull;

/**
 * {@link StoreCatalog} supplier.
 * Offers variations on {@link StoreCatalog} that differ by how they implement
 * {@link com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog}. The 'default' persists
 * {@link com.apple.foundationdb.relational.api.metadata.SchemaTemplate}s. The other implementation offers
 * a {@link StoreCatalog} that does no {@link com.apple.foundationdb.relational.api.metadata.SchemaTemplate}'ing.
 * @see StoreCatalog
 */
@API(API.Status.EXPERIMENTAL)
public class StoreCatalogProvider {
    /**
     * Get {@link StoreCatalog} instance.
     */
    public static StoreCatalog getCatalog(Transaction txn, KeySpace keySpace) throws RelationalException {
        return new RecordLayerStoreCatalog(keySpace).initialize(txn);
    }

    /**
     * Used to support all our Catalog migration efforts.
     */
    public static StoreCatalog getCatalogWithNoTemplateOperations(Transaction txn) throws RelationalException {
        // Do not allow repairing Schema operation because they cannot work with NoOpSchemaTemplateCatalog.
        final var storeCatalog = new RecordLayerStoreCatalog(RelationalKeyspaceProvider.instance().getKeySpace()) {
            @Override
            public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName)
                    throws RelationalException {
                throw new OperationUnsupportedException("This store catalog does not support repairing schema.");
            }
        };

        return storeCatalog.initialize(txn, new NoOpSchemaTemplateCatalog());
    }
}
