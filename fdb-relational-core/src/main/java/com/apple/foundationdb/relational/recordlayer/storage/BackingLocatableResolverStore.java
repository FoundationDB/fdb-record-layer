/*
 * BackingLocatableResolverStore.java
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.FutureCursor;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.QueryPropertiesUtils;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class BackingLocatableResolverStore implements BackingStore {
    private final LocatableResolver locatableResolver;
    private final ResolverCreateHooks resolverCreateHooks;
    private final Transaction txn;
    private final LocatableResolverMetaDataProvider metaDataProvider;

    private BackingLocatableResolverStore(LocatableResolver locatableResolver,
                                          ResolverCreateHooks resolverCreateHooks,
                                          Transaction txn,
                                          LocatableResolverMetaDataProvider metaDataProvider) {
        this.locatableResolver = locatableResolver;
        this.resolverCreateHooks = resolverCreateHooks;
        this.txn = txn;
        this.metaDataProvider = metaDataProvider;
    }

    @Override
    public Optional<RelationalResultSet> executeQuery(EmbeddedRelationalConnection conn, String query, Options options) throws RelationalException {
        throw new OperationUnsupportedException("Cannot execute query from interning layer store");
    }

    @Nullable
    @Override
    public Row get(Row key, Options options) throws RelationalException {
        try {
            FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
            Object typeKey = key.getObject(0);
            if (metaDataProvider.getInterningTypeKey().equals(typeKey)) {
                String name = key.getString(1);
                CompletableFuture<ResolverResult> resultFuture = locatableResolver.resolveWithMetadata(context, name, resolverCreateHooks);
                ResolverResult result = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, resultFuture);
                return result == null ? null : new MessageTuple(metaDataProvider.wrapResolverResult(name, result));
            } else if (metaDataProvider.getResolverStateTypeKey().equals(typeKey)) {
                Integer version = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, locatableResolver.getVersion(context.getTimer()));
                return version == null ? null : new MessageTuple(metaDataProvider.wrapResolverVersion(version));
            } else {
                throw new TypeNotPresentException("" + typeKey, null);
            }
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Nullable
    @Override
    public Row getFromIndex(Index index, Row key, Options options) throws RelationalException {
        if (!index.getName().equals("reverse_interning")) {
            throw new InternalErrorException("invalid index for resolver store");
        }
        final FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
        long value = key.getLong(0);
        String name = context.asyncToSync(FDBStoreTimer.Waits.WAIT_REVERSE_DIRECTORY_LOOKUP, locatableResolver.reverseLookup(context, value));
        if (name == null) {
            return null;
        }
        return new MessageTuple(metaDataProvider.wrapInterning(name, value, null));
    }

    @Override
    public boolean delete(Row key) throws RelationalException {
        throw new OperationUnsupportedException("Cannot delete entry from interning layer store");
    }

    @Override
    public void deleteRange(Map<String, Object> prefix, @Nullable String tableName) throws RelationalException {
        throw new OperationUnsupportedException("Cannot delete range from interning layer store");
    }

    @Override
    public boolean insert(String tableName, Message message, boolean replaceOnDuplicate) throws RelationalException {
        throw new OperationUnsupportedException("Cannot insert value into interning layer store");
    }

    @Override
    public RecordCursor<FDBStoredRecord<Message>> scanType(RecordType type, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException {
        if (continuation != null && continuation.atEnd()) {
            return RecordCursor.empty();
        }
        final FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
        if (!range.equals(TupleRange.allOf(Tuple.from(type.getRecordTypeKey())))) {
            // todo: make this error message better
            throw new InternalErrorException("unsupported range");
        }

        if (type.getName().equals(LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME)) {
            CompletableFuture<FDBStoredRecord<Message>> future = locatableResolver.getVersion(context.getTimer())
                    .thenApply(version -> {
                        Message msg = metaDataProvider.wrapResolverVersion(version);
                        return FDBStoredRecord.newBuilder(msg)
                                .setRecordType(type)
                                .setPrimaryKey(TupleHelpers.EMPTY)
                                .build();
                    });
            return new FutureCursor<>(context.getExecutor(), future);
        } else if (type.getName().equals(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME)) {
            byte[] continuationBytes = continuation == null ? null : continuation.getUnderlyingBytes();
            return locatableResolver.scan(context, continuationBytes, QueryPropertiesUtils.getScanProperties(options))
                    .map(resolverKeyValue -> {
                        Message msg = metaDataProvider.wrapResolverResult(resolverKeyValue.getKey(), resolverKeyValue.getValue());
                        return FDBStoredRecord.newBuilder(msg)
                                .setRecordType(type)
                                .setPrimaryKey(Tuple.from(resolverKeyValue.getKey()))
                                .build();
                    });
        } else {
            throw new TypeNotPresentException(type.getName(), null);
        }
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(Index index, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException {
        throw new OperationUnsupportedException("Cannot scan indexes in interning layer store");
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaDataProvider.getRecordMetaData();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws InternalErrorException {
        if (LocatableResolver.class.isAssignableFrom(type)) {
            return type.cast(locatableResolver);
        }
        return BackingStore.super.unwrap(type);
    }

    public static BackingStore create(LocatableResolver resolver, ResolverCreateHooks resolverCreateHooks, Transaction txn) throws RelationalException {
        return new BackingLocatableResolverStore(resolver, resolverCreateHooks, txn, LocatableResolverMetaDataProvider.instance());
    }
}
