/*
 * NoOpSchemaTemplate.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.StoredQuery;
import com.apple.foundationdb.relational.api.metadata.Table;

import com.apple.foundationdb.relational.api.metadata.View;
import com.google.common.collect.Multimap;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Implementation of Schema template that holds (name, version) and no tables.
 */
@API(API.Status.EXPERIMENTAL)
public class NoOpSchemaTemplate implements SchemaTemplate {
    private final String name;
    private final int version;

    public NoOpSchemaTemplate(String name, int version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public boolean isEnableLongRows() {
        return false;
    }

    @Override
    public boolean isStoreRowVersions() {
        return false;
    }

    @Override
    public Schema generateSchema(final String databaseId, final String schemaName) {
        return new RecordLayerSchema(schemaName, databaseId, this);
    }

    @Override
    public Set<? extends Table> getTables() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have tables!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Set<? extends View> getViews() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have views!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Optional<Table> findTableByName(final String tableName) throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have tables!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Optional<? extends View> findViewByName(final String viewName) throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have views!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Multimap<String, String> getTableIndexMapping() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have indexes!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Set<String> getIndexes() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have indexes!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public BitSet getIndexEntriesAsBitset(final Optional<Set<String>> readableIndexNames) throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have indexes!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Set<InvokedRoutine> getInvokedRoutines() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have invoked routines!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Optional<InvokedRoutine> findInvokedRoutineByName(final String routineName) throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have invoked routines!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Collection<InvokedRoutine> getTemporaryInvokedRoutines() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have temporary invoked routines!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public Map<String, StoredQuery> getStoredQueries() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have stored queries!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public String getTransactionBoundMetadataAsString() throws RelationalException {
        throw new RelationalException("NoOpSchemaTemplate doesn't have temporary invoked routines!", ErrorCode.INVALID_PARAMETER);
    }

    @Override
    public <T extends SchemaTemplate> T unwrap(final Class<T> iface) throws RelationalException {
        return iface.cast(this);
    }
}
