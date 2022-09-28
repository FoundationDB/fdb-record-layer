/*
 * InMemoryCatalog.java
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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.Schema;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

public class InMemoryCatalog implements StoreCatalog {

    Map<URI, List<InMemorySchema>> dbToSchemas = new ConcurrentHashMap<>();

    @Override
    @Nonnull
    public Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        final List<InMemorySchema> schemas = dbToSchemas.get(databaseId);
        if (schemas == null) {
            throw new RelationalException("No such database <" + databaseId + ">", ErrorCode.UNDEFINED_SCHEMA);
        }
        for (InMemorySchema schema : schemas) {
            if (schema.schema.getSchemaName().equalsIgnoreCase(schemaName)) {
                return schema.schema;
            }
        }
        throw new RelationalException("No such schema <" + schemaName + ">", ErrorCode.UNDEFINED_SCHEMA);
    }

    @Override
    public boolean updateSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite) {
        final URI key = URI.create(dataToWrite.getDatabaseId());
        List<InMemorySchema> schemas = dbToSchemas.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));

        for (InMemorySchema schema : schemas) {
            if (schema.schema.getSchemaName().equalsIgnoreCase(dataToWrite.getSchemaName())) {
                schema.schema = dataToWrite;
                schema.createTables();
                return true;
            }
        }

        //we need to create the schema
        InMemorySchema schem = new InMemorySchema();
        schem.schema = dataToWrite;
        schem.createTables();
        schemas.add(schem);
        return true;
    }

    @Override
    @Nonnull
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException {
        throw new RelationalException("No such schema template", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
    }

    @Override
    @Nonnull
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        throw new RelationalException("No such schema template", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        return false;
    }

    @Override
    public boolean updateSchemaTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate dataToWrite) {
        return false;
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
    public void deleteSchema(Transaction txn, URI dbUri, String schemaName) {

    }

    @Override
    public boolean doesDatabaseExist(Transaction txn, URI dbUrl) {
        return dbToSchemas.containsKey(dbUrl);
    }

    @Override
    public boolean doesSchemaExist(Transaction txn, URI dbUri, String schemaName) {
        return doesDatabaseExist(txn, dbUri) && dbToSchemas.get(dbUri).stream().map(s -> s.schema.getSchemaName()).anyMatch(schemaName::equals);
    }

    @Override
    public void deleteDatabase(Transaction txn, URI dbUrl) {
        dbToSchemas.remove(dbUrl);
    }

    public InMemoryTable loadTable(URI database, String schemaName, String tableName) throws RelationalException {
        final List<InMemorySchema> inMemorySchemas = dbToSchemas.get(database);
        if (inMemorySchemas == null) {
            throw new RelationalException("No such database <" + database.getPath() + ">", ErrorCode.UNKNOWN_TYPE);
        }
        for (InMemorySchema schema : inMemorySchemas) {
            if (schema.schema.getSchemaName().equalsIgnoreCase(schemaName)) {
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

        public void createTables() {
            tables.clear();

            final RecordMetaData recordMetaData = RecordMetaData.build(schema.getMetaData());
            for (Map.Entry<String, RecordType> typeEntry : recordMetaData.getRecordTypes().entrySet()) {
                tables.put(typeEntry.getKey(), new InMemoryTable(typeEntry.getValue()));
            }
        }
    }

}
