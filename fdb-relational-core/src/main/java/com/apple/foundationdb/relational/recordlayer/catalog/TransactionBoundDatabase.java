/*
 * TransactionBoundDatabase.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.HollowTransactionManager;
import com.apple.foundationdb.relational.recordlayer.RecordStoreAndRecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.CreateTemporaryFunctionConstantAction;
import com.apple.foundationdb.relational.recordlayer.ddl.DropTemporaryFunctionConstantAction;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.storage.BackingRecordStore;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.transactionbound.catalog.HollowStoreCatalog;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;

/**
 * There can only be 1 Database object per Connection instance, and its lifecycle is managed by the connection
 * itself.
 * This database cannot create new transaction and its transaction manager will throw {@link com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException}
 * if any of its methods are called.
 * A {@link RecordStoreAndRecordContextTransaction} object needs to be passed to {@link com.apple.foundationdb.relational.api.catalog.RelationalDatabase#connect(Transaction)}.
 * This object should contain a valid FDBRecordStoreBase and a FDBRecordContext whose scope is larger than the scope of this class.
 *
 * Note: this database doesn't support DDL statements.
 * Note: this is only a temporary workaround to finish the first step of integration with customers through direct API.
 */
@API(API.Status.EXPERIMENTAL)
public class TransactionBoundDatabase extends AbstractDatabase {
    BackingStore store;
    URI uri;

    private static final MetadataOperationsFactory onlyTemporaryFunctionOperationsFactory = new AbstractMetadataOperationsFactory() {
        @Nonnull
        @Override
        public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull final SchemaTemplate template, final boolean throwIfExists,
                                                                       @Nonnull final RecordLayerInvokedRoutine invokedRoutine) {
            return new CreateTemporaryFunctionConstantAction(template, throwIfExists, invokedRoutine);
        }

        @Nonnull
        @Override
        public ConstantAction getDropTemporaryFunctionConstantAction(final boolean throwIfNotExists, @Nonnull final String temporaryFunctionName) {
            return new DropTemporaryFunctionConstantAction(throwIfNotExists, temporaryFunctionName);
        }
    };

    public TransactionBoundDatabase(URI uri, @Nonnull Options options, @Nullable RelationalPlanCache planCache) {
        super(onlyTemporaryFunctionOperationsFactory, NoOpQueryFactory.INSTANCE, planCache, options);
        this.uri = uri;
    }

    @Override
    public RelationalConnection connect(@Nullable Transaction transaction) throws RelationalException {
        if (!(transaction instanceof RecordStoreAndRecordContextTransaction)) {
            throw new RelationalException("TransactionBoundDatabase.connect expects a RecordStoreAndRecordContextTransaction", ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);
        }
        final var recordStoreAndRecordContextTx = transaction.unwrap(RecordStoreAndRecordContextTransaction.class);
        store = BackingRecordStore.fromTransactionWithStore(recordStoreAndRecordContextTx);
        final var boundSchemaTemplate = recordStoreAndRecordContextTx.getBoundSchemaTemplate();
        EmbeddedRelationalConnection connection = new EmbeddedRelationalConnection(this, new HollowStoreCatalog(boundSchemaTemplate), ((RecordStoreAndRecordContextTransaction) transaction).getRecordContextTransaction(), options);
        setConnection(connection);
        return connection;
    }

    @Override
    public BackingStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        return store;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return HollowTransactionManager.INSTANCE;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public void close() throws RelationalException {

    }
}
