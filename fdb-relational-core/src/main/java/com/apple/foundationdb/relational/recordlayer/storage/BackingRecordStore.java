/*
 * BackingRecordStore.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.QueryPropertiesUtils;
import com.apple.foundationdb.relational.recordlayer.RecordStoreAndRecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.TupleUtils;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.base.Throwables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class BackingRecordStore implements BackingStore {
    private final Transaction transaction;
    private final FDBRecordStoreBase<Message> recordStore;

    private BackingRecordStore(Transaction transaction, FDBRecordStoreBase<Message> recordStore) {
        this.transaction = transaction;
        this.recordStore = recordStore;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws InternalErrorException {
        if (FDBRecordStoreBase.class.isAssignableFrom(type)) {
            return type.cast(recordStore);
        }
        return BackingStore.super.unwrap(type);
    }

    @Nullable
    @Override
    public Row get(Row key, Options options) throws RelationalException {
        try {
            final FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(TupleUtils.toFDBTuple(key));
            if (storedRecord == null) {
                return null;
            }
            return new MessageTuple(storedRecord.getRecord());
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Nullable
    @Override
    public Row getFromIndex(Index index, Row key, Options options) throws RelationalException {
        ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(options);
        scanProperties = new ScanProperties(scanProperties.getExecuteProperties().setReturnedRowLimit(1), scanProperties.isReverse());
        try {
            final RecordCursorIterator<IndexEntry> indexEntryRecordCursor = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(TupleUtils.toFDBTuple(key)), null, scanProperties).asIterator();
            IndexEntry entry;
            if (!indexEntryRecordCursor.hasNext()) {
                return null;
            }
            entry = Objects.requireNonNull(indexEntryRecordCursor.next());

            //TODO(bfines) pull the orphan behavior from the options
            final CompletableFuture<FDBIndexedRecord<Message>> indexRecord = recordStore.loadIndexEntryRecord(entry, IndexOrphanBehavior.ERROR);
            //TODO(bfines): add in store timing
            final FDBIndexedRecord<Message> storedRecord = transaction.unwrap(FDBRecordContext.class)
                    .asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, indexRecord);
            if (storedRecord == null) {
                return null;
            }
            return new MessageTuple(storedRecord.getRecord());
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public boolean delete(Row key) throws RelationalException {
        try {
            return recordStore.deleteRecord(TupleUtils.toFDBTuple(key));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public void deleteRange(Map<String, Object> prefix, @Nullable String tableName) throws RelationalException {
        List<QueryComponent> queryFields = prefix.entrySet().stream()
                .map(entry -> Query.field(entry.getKey()).equalsValue(entry.getValue()))
                .collect(Collectors.toList());
        if (tableName != null) {
            queryFields.add(new RecordTypeKeyComparison(tableName));
        }
        QueryComponent query;
        switch (queryFields.size()) {
            case 0:
                throw new RelationalException("Delete range with empty key range is only supported on tables with RecordTypeKeys", ErrorCode.INVALID_PARAMETER);
            case 1:
                query = queryFields.get(0);
                break;
            default:
                query = Query.and(queryFields);
                break;
        }
        try {
            recordStore.deleteRecordsWhere(query);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we are intentionally destroying the stack trace here
    public boolean insert(String tableName, Message message, boolean replaceOnDuplicate) throws RelationalException {
        try {
            if (!recordStore.getRecordMetaData().getRecordType(tableName).getDescriptor().equals(message.getDescriptorForType())) {
                throw new RelationalException("type of message <" + message.getClass() + "> does not match the required type for table <" + tableName + ">", ErrorCode.INVALID_PARAMETER);
            }
            if (replaceOnDuplicate) {
                recordStore.saveRecord(message);
            } else {
                recordStore.insertRecord(message);
            }
        } catch (MetaDataException mde) {
            throw new RelationalException("type of message <" + message.getClass() + "> does not match the required type for table <" + tableName + ">", ErrorCode.INVALID_PARAMETER, mde);
        } catch (RecordAlreadyExistsException raee) {
            throw new RelationalException("Duplicate primary key for message (" + message + ") on table <" + tableName + ">", ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        //TODO(bfines) maybe this should return something other than boolean?
        return true;
    }

    @Override
    public RecordCursor<FDBStoredRecord<Message>> scanType(RecordType type, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException {
        try {
            final ScanProperties scanProps = QueryPropertiesUtils.getScanProperties(options);
            return recordStore.scanRecords(range, continuation == null ? null : continuation.getUnderlyingBytes(), scanProps)
                    .filter(record -> type.equals(record.getRecordType()));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(Index index, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException {
        //TODO(bfines) get scan type from Options and/or ScanProperties
        assert continuation == null || continuation instanceof ContinuationImpl;
        try {
            return recordStore.scanIndex(index, IndexScanType.BY_VALUE, range,
                    continuation == null ? null : continuation.getUnderlyingBytes(), QueryPropertiesUtils.getScanProperties(options));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return recordStore.getRecordMetaData();
    }

    public static BackingRecordStore fromTransactionWithStore(@Nonnull RecordStoreAndRecordContextTransaction txn) {
        return new BackingRecordStore(txn, txn.getRecordStore());
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    public static BackingRecordStore load(@Nonnull Transaction txn, @Nonnull StoreConfig config, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
        //TODO(bfines) error handling if this store doesn't exist
        try {
            FDBRecordStoreBase<Message> recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(config.getStorePath())
                    .setSerializer(config.getSerializer())
                    //TODO(bfines) replace this schema template with an actual mapping structure based on the storePath
                    .setMetaDataProvider(config.getMetaDataProvider())
                    .setUserVersionChecker(config.getUserVersionChecker())
                    .setFormatVersion(config.getFormatVersion())
                    .setContext(txn.unwrap(FDBRecordContext.class))
                    .createOrOpen(existenceCheck);
            return new BackingRecordStore(txn, recordStore);
        } catch (RecordCoreException rce) {
            Throwable cause = Throwables.getRootCause(rce);
            if (cause instanceof RecordStoreDoesNotExistException) {
                throw new RelationalException("Schema does not exist. Schema: <" + config.getSchemaName() + ">", ErrorCode.UNDEFINED_SCHEMA, cause);
            } else {
                throw ExceptionUtil.toRelationalException(rce);
            }
        }
    }
}
