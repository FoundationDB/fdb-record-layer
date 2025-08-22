/*
 * AbstractDatabase.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractDatabase implements RelationalDatabase {

    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;

    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;
    @Nullable
    protected EmbeddedRelationalConnection connection;
    final Map<String, RecordLayerSchema> schemas = new HashMap<>();
    @Nullable
    private final RelationalPlanCache planCache;
    @Nonnull
    protected Options options;

    public AbstractDatabase(@Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                            @Nonnull DdlQueryFactory ddlQueryFactory,
                            @Nullable RelationalPlanCache planCache,
                            @Nonnull Options options) {
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.planCache = planCache;
        this.options = options;
    }

    protected void setConnection(@Nonnull EmbeddedRelationalConnection conn) {
        this.connection = conn;
    }

    @Nonnull
    protected Transaction getCurrentTransaction() throws RelationalException {
        if (connection == null) {
            throw new RelationalException("Connection not set!", ErrorCode.INTERNAL_ERROR);
        }
        return connection.getTransaction();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public @Nonnull RecordLayerSchema loadSchema(@Nonnull String schemaId) throws RelationalException {
        RecordLayerSchema schema = schemas.get(schemaId);
        boolean putBack = false;
        if (schema == null) {
            // The SchemaExistenceCheck from the options is only taken when the schema is created firstly
            // It is an immutable parameter for the schema and the options for the following operations on that schema are ignored
            schema = new RecordLayerSchema(schemaId, this, connection);
            putBack = true;
        }

        if (putBack) {
            schemas.put(schemaId, schema);
            getCurrentTransaction().unwrap(RecordContextTransaction.class).addTerminationListener(() -> {
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

    @Nonnull
    @Override
    public MetadataOperationsFactory getDdlFactory() {
        return metadataOperationsFactory;
    }

    @Nonnull
    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    public abstract BackingStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException;

    public abstract URI getURI();

    public abstract TransactionManager getTransactionManager();

    @Nullable
    public RelationalPlanCache getPlanCache() {
        return planCache;
    }

    @Nonnull
    public Options getOptions() {
        return options;
    }

    public void setOption(@Nonnull Options.Name name, Object value) throws SQLException {
        options = options.withOption(name, value);
    }
}
