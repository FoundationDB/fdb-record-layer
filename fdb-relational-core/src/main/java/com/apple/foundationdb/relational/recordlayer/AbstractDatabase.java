/*
 * AbstractDatabase.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

public abstract class AbstractDatabase implements RelationalDatabase {

    RecordStoreConnection connection;
    final Map<String, RecordLayerSchema> schemas = new HashMap<>();

    public AbstractDatabase() {
    }

    protected void setConnection(@Nonnull RecordStoreConnection conn) {
        this.connection = conn;
    }

    @Override
    public @Nonnull RecordLayerSchema loadSchema(@Nonnull String schemaId, @Nonnull Options options) throws RelationalException {
        RecordLayerSchema schema = schemas.get(schemaId);
        boolean putBack = false;
        if (schema == null) {
            // The SchemaExistenceCheck from the options is only taken when the schema is created firstly
            // It is an immutable parameter for the schema and the options for the following operations on that schema are ignored
            schema = new RecordLayerSchema(schemaId, this, connection, options);
            putBack = true;
        }

        if (putBack) {
            schemas.put(schemaId, schema);
            this.connection.transaction.unwrap(RecordContextTransaction.class).addTerminationListener(() -> {
                RecordLayerSchema rlSchema = schemas.remove(schemaId);
                try {
                    if (rlSchema != null) {
                        rlSchema.close();
                    }
                } catch (RelationalException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return schema;
    }

    public abstract FDBRecordStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException;

    public abstract URI getURI();

    public abstract TransactionManager getTransactionManager();
}
