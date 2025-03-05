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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.QueryPropertiesUtils;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

@API(API.Status.EXPERIMENTAL)
public final class BackingLocatableResolverStore implements BackingStore {
    private final LocatableResolver locatableResolver;
    private final Transaction txn;
    private final LocatableResolverMetaDataProvider metaDataProvider;

    private BackingLocatableResolverStore(LocatableResolver locatableResolver,
                                          Transaction txn,
                                          LocatableResolverMetaDataProvider metaDataProvider) {
        this.locatableResolver = locatableResolver;
        this.txn = txn;
        this.metaDataProvider = metaDataProvider;
    }

    @Nullable
    @Override
    @SuppressWarnings("PMD.CloseResource") // context is not owned by this method should it should not be closed here
    public Row get(Row key, Options options) throws RelationalException {
        try {
            FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
            Object typeKey = key.getObject(0);
            if (metaDataProvider.getInterningTypeKey().equals(typeKey)) {
                String name = key.getString(1);
                CompletableFuture<ResolverResult> resultFuture = locatableResolver.readInTransaction(context, name);
                ResolverResult result = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, resultFuture);
                return result == null ? null : new MessageTuple(metaDataProvider.wrapResolverResult(name, result));
            } else if (metaDataProvider.getResolverStateTypeKey().equals(typeKey)) {
                ResolverStateProto.State state = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, locatableResolver.loadResolverState(context));
                return state == null ? null : new MessageTuple(metaDataProvider.wrapResolverState(state));
            } else {
                throw new TypeNotPresentException("" + typeKey, null);
            }
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Nullable
    @Override
    @SuppressWarnings("PMD.CloseResource") // context is not owned by this method should it should not be closed here
    public Row getFromIndex(Index index, Row key, Options options) throws RelationalException {
        if (!"reverse_interning".equals(index.getName())) {
            throw new InternalErrorException("invalid index for resolver store");
        }
        final FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
        long value = key.getLong(0);
        try {
            String name = context.asyncToSync(FDBStoreTimer.Waits.WAIT_REVERSE_DIRECTORY_LOOKUP, locatableResolver.reverseLookupInTransaction(context, value));
            if (name == null) {
                return null;
            }
            return new MessageTuple(metaDataProvider.wrapInterning(name, value, null));
        } catch (NoSuchElementException noSuchElementException) {
            return null;
        }
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
    @SuppressWarnings("PMD.CloseResource") // context is not owned by this method should it should not be closed here
    public boolean insert(String tableName, Message message, boolean replaceOnDuplicate) throws RelationalException {
        if (LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME.equals(tableName)) {
            // The "Interning" type is the main type mapping string keys to long values. Each key also can have an
            // associated "meta-data" byte[] field. This value can be cached in in-memory caches maintained by the
            // FDBDirectory, and to avoid reading stale data from the cache, the ability to update existing records
            // is restricted. Instead, this limits the writes to (1) new keys (i.e., keys that do not have pre-existing
            // mappings) with (2) unset values (i.e., values that will be allocated by the underlying data structure).
            // The values are all guaranteed to be unique (once committed), and they will be generally increasing
            // in size over time.
            Descriptors.Descriptor interningDescriptor = message.getDescriptorForType();
            String name = (String) message.getField(interningDescriptor.findFieldByName(LocatableResolverMetaDataProvider.KEY_FIELD_NAME));
            long value = (Long) message.getField(interningDescriptor.findFieldByName(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME));
            ByteString metaDataByteString = (ByteString) message.getField(interningDescriptor.findFieldByName(LocatableResolverMetaDataProvider.META_DATA_FIELD_NAME));
            byte[] metaData = metaDataByteString.isEmpty() ? null : metaDataByteString.toByteArray();

            FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
            ResolverResult result = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, locatableResolver.readInTransaction(context, name));

            if (result != null) {
                if (replaceOnDuplicate) {
                    throw new RelationalException("Cannot update table <" + tableName + "> as entry with key " + name + " already exists", ErrorCode.UNSUPPORTED_OPERATION);
                } else {
                    throw new RelationalException("Duplicate primary key for message (" + message + ") on table <" + tableName + ">", ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
                }
            }

            // Insert a new value into the store. This will allocate a new value for the mapping
            if (value != 0L)  {
                throw new RelationalException("Must use automatically allocated value for " + tableName + " rows", ErrorCode.UNSUPPORTED_OPERATION);
            }

            // Use the meta-data hook to ensure the value is created with the specified meta-data value
            ResolverCreateHooks.MetadataHook metadataHook = ignore -> metaData;
            ResolverCreateHooks createHooks = new ResolverCreateHooks(ResolverCreateHooks.DEFAULT_CHECK, metadataHook);
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, locatableResolver.createInTransaction(context, name, createHooks));

            return true;
        } else if (LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME.equals(tableName)) {
            // Resolver state record is more straightforward. There is one resolver state per LocatableResolver, which
            // contains information like whether the resolver has been locked (i.e., should not accept additional
            // writes while data is copied) or retired (i.e., is not expected to be used again). It also contains
            // a "version", which is used as a cache invalidation key
            if (!replaceOnDuplicate) {
                throw new RelationalException("Duplicate primary key for message (" + message + ") on table <" + tableName + ">", ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            }
            Descriptors.Descriptor stateDescriptor = message.getDescriptorForType();
            int version = (Integer) message.getField(stateDescriptor.findFieldByName(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME));
            Descriptors.EnumValueDescriptor lockEnum = (Descriptors.EnumValueDescriptor) message.getField(stateDescriptor.findFieldByName(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME));
            String lock = lockEnum == null ? null : lockEnum.getName();

            var builder = ResolverStateProto.State.newBuilder()
                    .setVersion(version);
            if (lock != null) {
                builder.setLock(ResolverStateProto.WriteLock.valueOf(lock));
            }
            ResolverStateProto.State protoState = builder.build();

            FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOCATABLE_RESOLVER_MAPPING_COPY, locatableResolver.saveResolverState(context, protoState));
            return true;
        } else {
            throw new TypeNotPresentException(tableName, null);
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") // context is not owned by this method should it should not be closed here
    public RecordCursor<FDBStoredRecord<Message>> scanType(RecordType type, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException {
        if (continuation != null && continuation.atEnd()) {
            return RecordCursor.empty();
        }
        final FDBRecordContext context = txn.unwrap(FDBRecordContext.class);
        if (!range.equals(TupleRange.allOf(Tuple.from(type.getRecordTypeKey())))) {
            // todo: make this error message better
            throw new InternalErrorException("unsupported range");
        }

        final byte[] continuationBytes = continuation == null ? null : continuation.getExecutionState();
        final ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(options);
        if (type.getName().equals(LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME)) {
            // todo: this does not use the scanProperties to track runtime information like rows scanned
            final ExecuteProperties executeProperties = scanProperties.getExecuteProperties();
            return RecordCursor.fromFuture(context.getExecutor(),
                    () -> locatableResolver.loadResolverState(context).thenApply(state -> {
                        Message msg = metaDataProvider.wrapResolverState(state);
                        return FDBStoredRecord.newBuilder(msg)
                                .setRecordType(type)
                                .setPrimaryKey(TupleHelpers.EMPTY)
                                .build();
                    }),
                    continuationBytes
            ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        } else if (type.getName().equals(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME)) {
            return locatableResolver.scan(context, continuationBytes, scanProperties)
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

    public static BackingStore create(LocatableResolver resolver, Transaction txn) throws RelationalException {
        return new BackingLocatableResolverStore(resolver, txn, LocatableResolverMetaDataProvider.instance());
    }
}
