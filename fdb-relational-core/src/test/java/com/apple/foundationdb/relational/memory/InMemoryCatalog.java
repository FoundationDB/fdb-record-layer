/*
 * InMemoryCatalog.java
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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCatalog implements StoreCatalog {

    Map<URI, List<InMemorySchema>> dbToSchemas = new ConcurrentHashMap<>();
    SchemaTemplateCatalog schemaTemplateCatalog = new InMemorySchemaTemplateCatalog();

    @Override
    public SchemaTemplateCatalog getSchemaTemplateCatalog() {
        return schemaTemplateCatalog;
    }

    @Nonnull
    @Override
    public Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        final List<InMemorySchema> schemas = dbToSchemas.get(databaseId);
        if (schemas == null) {
            throw new RelationalException("No such database <" + databaseId + ">", ErrorCode.UNDEFINED_SCHEMA);
        }
        for (InMemorySchema schema : schemas) {
            if (schema.schema.getName().equalsIgnoreCase(schemaName)) {
                return schema.schema;
            }
        }
        throw new RelationalException("No such schema <" + schemaName + ">", ErrorCode.UNDEFINED_SCHEMA);
    }

    @Override
    public void saveSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite, boolean createDatabaseIfNecessary) throws RelationalException {
        final URI key = URI.create(dataToWrite.getDatabaseName());
        List<InMemorySchema> schemas = dbToSchemas.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));

        for (InMemorySchema schema : schemas) {
            if (schema.schema.getName().equalsIgnoreCase(dataToWrite.getName())) {
                schema.schema = dataToWrite;
                schema.createTables();
            }
        }

        //we need to create the schema
        InMemorySchema schema = new InMemorySchema();
        schema.schema = dataToWrite;
        schema.createTables();
        schemas.add(schema);
    }

    @Override
    public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
        throw new RelationalException("No such schema", ErrorCode.UNDEFINED_SCHEMA);
    }

    @Override
    public void createDatabase(@Nonnull Transaction txn, @Nonnull URI dbUri) {
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) {
        return null;
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) {
        return null;
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) {
        return null;
    }

    @Override
    public void deleteSchema(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) {
        final var schemas = dbToSchemas.getOrDefault(dbUri, new ArrayList<>());
        for (var schema : schemas) {
            if (schema.schema.getName().equals(schemaName)) {
                schemas.remove(schema);
                return;
            }
        }
    }

    @Override
    public boolean doesDatabaseExist(@Nonnull Transaction txn, @Nonnull URI dbUrl) {
        return dbToSchemas.containsKey(dbUrl);
    }

    @Override
    public boolean doesSchemaExist(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) {
        return doesDatabaseExist(txn, dbUri) && dbToSchemas.get(dbUri).stream().map(s -> s.schema.getName()).anyMatch(schemaName::equals);
    }

    @Override
    public boolean deleteDatabase(@Nonnull Transaction txn, @Nonnull URI dbUrl, boolean throwIfDoesNotExist) throws RelationalException {
        if (dbToSchemas.remove(dbUrl) == null && throwIfDoesNotExist) {
            throw new RelationalException("Cannot delete unknown database " + dbUrl, ErrorCode.UNKNOWN_DATABASE);
        }
        return true;
    }

    @Nonnull
    @Override
    public KeySpace getKeySpace() throws RelationalException {
        // TODO probably the tests can support this
        throw new OperationUnsupportedException("This store is in memory and does not have a keySpace.");
    }

    public InMemoryTable loadTable(URI database, String schemaName, String tableName) throws RelationalException {
        final List<InMemorySchema> inMemorySchemas = dbToSchemas.get(database);
        if (inMemorySchemas == null) {
            throw new RelationalException("No such database <" + database.getPath() + ">", ErrorCode.UNKNOWN_TYPE);
        }
        for (InMemorySchema schema : inMemorySchemas) {
            if (schema.schema.getName().equalsIgnoreCase(schemaName)) {
                InMemoryTable tbl = schema.tables.get(tableName);
                if (tbl == null) {
                    throw new RelationalException("No such table <" + tableName + ">", ErrorCode.UNKNOWN_TYPE);
                }
                return tbl;
            }
        }
        throw new RelationalException("No such Schema <" + schemaName + ">", ErrorCode.UNKNOWN_TYPE);
    }

    private static class InMemorySchema {
        Schema schema;
        private final Map<String, InMemoryTable> tables = new ConcurrentHashMap<>();

        public void createTables() throws RelationalException {
            tables.clear();

            final RecordMetaData recordMetaData;
            recordMetaData = schema.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata();
            for (Map.Entry<String, RecordType> typeEntry : recordMetaData.getRecordTypes().entrySet()) {
                tables.put(typeEntry.getKey(), new InMemoryTable(typeEntry.getValue()));
            }
        }
    }
}
