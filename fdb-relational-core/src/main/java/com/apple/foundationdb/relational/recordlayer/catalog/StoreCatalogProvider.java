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

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;

public class StoreCatalogProvider {

    public static StoreCatalog getCatalog(Transaction txn) throws RelationalException {
        final var schemaTemplateCatalog = new InMemorySchemaTemplateCatalog();
        final var storeCatalog = new RecordLayerStoreCatalogImpl(RelationalKeyspaceProvider.getKeySpace(), schemaTemplateCatalog);
        storeCatalog.initialize(txn);
        return storeCatalog;
    }
}
