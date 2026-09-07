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

import org.jspecify.annotations.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractDatabase implements RelationalDatabase {

    private final MetadataOperationsFactory metadataOperationsFactory;

    private final DdlQueryFactory ddlQueryFactory;
    @Nullable
    protected EmbeddedRelationalConnection connection;
    final Map<String, RecordLayerSchema> schemas = new HashMap<>();
    @Nullable
    private final RelationalPlanCache planCache;
    protected Options options;

    public AbstractDatabase(final MetadataOperationsFactory metadataOperationsFactory,
                            DdlQueryFactory ddlQueryFactory,
                            @Nullable RelationalPlanCache planCache,
                            Options options) {
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.planCache = planCache;
        this.options = options;
    }

    protected void setConnection(EmbeddedRelationalConnection conn) {
        this.connection = conn;
    }

    protected Transaction getCurrentTransaction() throws RelationalException {
        if (connection == null) {
            throw new RelationalException("Connection not set!", ErrorCode.INTERNAL_ERROR);
        }
        return connection.getTransaction();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public RecordLayerSchema loadSchema(String schemaId) throws RelationalException {
        RecordLayerSchema schema = schemas.get(schemaId);
        boolean putBack = false;
        if (schema == null) {
            // The SchemaExistenceCheck from the options is only taken when the schema is created firstly
            // It is an immutable parameter for the schema and the options for the following operations on that schema are ignored
            // connection is set via setConnection(...) as part of database construction, before any
            // schema is ever loaded, so it is always present by the time loadSchema() is called.
            schema = new RecordLayerSchema(schemaId, this, Objects.requireNonNull(connection, "connection not set on database before loadSchema() was called"));
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

    @Override
    public MetadataOperationsFactory getDdlFactory() {
        return metadataOperationsFactory;
    }

    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    public abstract BackingStore loadRecordStore(String schemaId, FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException;

    public abstract URI getURI();

    public abstract TransactionManager getTransactionManager();

    @Nullable
    public RelationalPlanCache getPlanCache() {
        return planCache;
    }

    public Options getOptions() {
        return options;
    }

    public void setOption(Options.Name name, Object value) throws SQLException {
        options = options.withOption(name, value);
    }
}
