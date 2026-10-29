/*
 * RecordChangesIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordChangesProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecordBuilder;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An index that records changes to a record on save.
 * The keys of the index would normally include {@link VersionKeyExpression Version},
 * so that the index represents a complete history.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordChangesIndexMaintainer extends StandardIndexMaintainer {
    public record RecordChangesEntry<M extends Message>(@Nonnull Tuple key,
                                                        @Nullable FDBStoredRecord<M> oldRecord,
                                                        @Nullable FDBStoredRecord<M> newRecord) {
    }

    private final int versionPosition;

    protected RecordChangesIndexMaintainer(IndexMaintainerState state) {
        super(state);
        final List<KeyExpression> keys = state.index.getRootExpression().normalizeKeyForPositions();
        int found = -1;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) instanceof VersionKeyExpression) {
                found = i;
                break;
            }
        }
        versionPosition = found;
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<RecordChangesEntry<Message>> scanChanges(@Nonnull TupleRange range,
                                                                 @Nullable byte[] continuation,
                                                                 @Nonnull ScanProperties scanProperties) {
        final RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.newBuilder(state.indexSubspace)
                .setContext(state.context)
                .setRange(range)
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return new SplitHelper.KeyValueUnsplitter(state.context, state.indexSubspace, keyValues, false, null, scanProperties)
                .map(this::deserializeChange);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<RecordChangesEntry<Message>> scanChanges(@Nonnull TupleRange range,
                                                                 @Nullable Tuple lastKey,
                                                                 @Nonnull ScanProperties scanProperties) {
        final RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.newBuilder(state.indexSubspace)
                .setContext(state.context)
                .setLow(lastKey == null ? range.getLow() : lastKey,
                        lastKey == null ? range.getLowEndpoint() : EndpointType.RANGE_EXCLUSIVE)
                .setHigh(range.getHigh(), range.getHighEndpoint())
                .setScanProperties(scanProperties)
                .build();
        return new SplitHelper.KeyValueUnsplitter(state.context, state.indexSubspace, keyValues, false, null, scanProperties)
                .map(this::deserializeChange);
    }

    @Override
    @Nonnull
    public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                              @Nullable final FDBIndexableRecord<M> newRecord) {
        // No entries are removed. Key should normally be the same, up to version, for both records.
        final FDBIndexableRecord<M> indexRecord;
        if (newRecord != null) {
            if (newRecord.hasVersion()) {
                indexRecord = newRecord;
            } else {
                indexRecord = ((FDBStoredRecord<M>)newRecord).withVersion(FDBRecordVersion.incomplete(state.context.claimLocalVersion()));
            }
        } else if (oldRecord != null) {
            indexRecord = ((FDBStoredRecord<M>)oldRecord).withVersion(FDBRecordVersion.incomplete(state.context.claimLocalVersion()));
        } else {
            return AsyncUtil.DONE;
        }
        final List<IndexEntry> indexEntries = filteredIndexEntries(indexRecord);
        if (indexEntries == null || indexEntries.isEmpty()) {
            return AsyncUtil.DONE;
        }
        final byte[] serializedChange = serializeChange(state.store.getRecordMetaData(), indexRecord.getRecordType(), indexRecord.getPrimaryKey(),
                newRecord == null ? null : newRecord.getRecord(),
                oldRecord == null ? null : oldRecord.getRecord(),
                oldRecord == null ? null : oldRecord.getVersion());
        for (IndexEntry indexEntry : indexEntries) {
            if (!indexEntry.getValue().isEmpty()) {
                throw new RecordCoreException("Index entry has conflicting value.");
            }
            final long startTime = System.nanoTime();
            final Tuple entryKey = indexEntryKey(indexEntry.getKey(), indexRecord.getPrimaryKey());
            // It might be simpler to pass indexRecord.getVersion(), but version cannot be in both key and value.
            SplitHelper.saveWithSplit(state.context, state.indexSubspace, entryKey, serializedChange, null);
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.SAVE_INDEX_ENTRY, startTime);
            }
        }
        return AsyncUtil.DONE;
    }

    @Override
    @Nonnull
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        throw new RecordCoreException("Cannot scan change index directly.");
    }

    @Nonnull
    @Override
    public RecordCursor<FDBIndexedRawRecord> scanRemoteFetch(@Nonnull final IndexScanBounds scanBounds,
                                                             @Nullable final byte[] continuation,
                                                             @Nonnull final ScanProperties scanProperties,
                                                             int commonPrimaryKeyLength) {
        throw new RecordCoreException("Cannot scan change index remotely.");
    }

    @Nullable
    @Override
    public IndexScrubbingTools<?> getIndexScrubbingTools(final IndexScrubbingTools.ScrubbingType type) {
        return null;
    }

    @Nonnull
    private byte[] serializeChange(@Nonnull RecordMetaData metaData, @Nonnull RecordType recordType, @Nonnull Tuple primaryKey,
                                   @Nullable Message newRecord, @Nullable Message oldRecord, @Nullable FDBRecordVersion oldVersion) {
        RecordChangesProto.RecordChange.Builder change = RecordChangesProto.RecordChange.newBuilder()
                .setPrimaryKey(ZeroCopyByteString.wrap(primaryKey.pack()));
        if (newRecord == null) {
            if (oldRecord != null) {
                change.setOldRecord(ZeroCopyByteString.wrap(state.store.getSerializer().serialize(metaData, recordType, oldRecord, state.context.getTimer())));
            }
        } else {
            change.setNewRecord(ZeroCopyByteString.wrap(state.store.getSerializer().serialize(metaData, recordType, newRecord, state.context.getTimer())));
            if (oldRecord != null) {
                Message.Builder changedBuilder = oldRecord.toBuilder();
                for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldEntry : newRecord.getAllFields().entrySet()) {
                    Descriptors.FieldDescriptor fieldDescriptor = fieldEntry.getKey();
                    if (!changedBuilder.hasField(fieldDescriptor)) {
                        change.addChangedEmptyFields(fieldDescriptor.getIndex());
                    } else if (changedBuilder.getField(fieldDescriptor).equals(fieldEntry.getValue())) {
                        changedBuilder.clearField(fieldDescriptor);
                    }
                }
                change.setOldRecord(ZeroCopyByteString.wrap(state.store.getSerializer().serialize(metaData, recordType, changedBuilder.build(), state.context.getTimer())));
            }
        }
        if (oldVersion != null) {
            change.setOldVersion(ZeroCopyByteString.wrap(oldVersion.toBytes(false)));
        }
        return change.build().toByteArray();
    }

    @Nonnull
    private RecordChangesEntry<Message> deserializeChange(@Nonnull FDBRawRecord unsplit) {
        final Tuple key = unsplit.getPrimaryKey();
        final FDBRecordVersion version;
        if (versionPosition < 0) {
            version = null;
        } else {
            version = FDBRecordVersion.fromVersionstamp(key.getVersionstamp(versionPosition));
        }
        final RecordChangesProto.RecordChange changeProto;
        try {
            changeProto = RecordChangesProto.RecordChange.parseFrom(unsplit.getRawRecord());
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("error parsing change entry", ex);
        }
        final RecordMetaData metaData = state.store.getRecordMetaData();
        final Tuple primaryKey = Tuple.fromBytes(changeProto.getPrimaryKey().toByteArray());
        final FDBStoredRecord<Message> newRecord;
        if (changeProto.hasNewRecord()) {
            final Message protoRecord = state.store.getSerializer().deserialize(metaData, key,
                    changeProto.getNewRecord().toByteArray(), state.context.getTimer());
            final FDBStoredRecordBuilder<Message> recordBuilder = FDBStoredRecord.newBuilder(protoRecord).setPrimaryKey(primaryKey);
            final RecordType recordType = metaData.getRecordTypeForDescriptor(protoRecord.getDescriptorForType());
            recordBuilder.setRecordType(recordType);
            if (metaData.isStoreRecordVersions()) {
                // If we could have it as inline value, this would be unsplit.getVersion().
                recordBuilder.setVersion(version);
            }
            newRecord = recordBuilder.build();
        } else {
            newRecord = null;
        }
        final FDBStoredRecord<Message> oldRecord;
        if (changeProto.hasOldRecord()) {
            final Message protoRecord = state.store.getSerializer().deserialize(metaData, key,
                    changeProto.getOldRecord().toByteArray(), state.context.getTimer());
            final FDBStoredRecordBuilder<Message> recordBuilder = FDBStoredRecord.newBuilder().setPrimaryKey(primaryKey);
            final RecordType recordType = metaData.getRecordTypeForDescriptor(protoRecord.getDescriptorForType());
            recordBuilder.setRecordType(recordType);
            if (newRecord == null) {
                recordBuilder.setRecord(protoRecord);
            } else {
                Message.Builder protoBuilder = protoRecord.toBuilder();
                BitSet changedEmpty = new BitSet();
                for (Integer fieldIndex : changeProto.getChangedEmptyFieldsList()) {
                    changedEmpty.set(fieldIndex);
                }
                // Fill in unchanged fields.
                for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldEntry : newRecord.getRecord().getAllFields().entrySet()) {
                    Descriptors.FieldDescriptor fieldDescriptor = fieldEntry.getKey();
                    if (!protoBuilder.hasField(fieldDescriptor) && !changedEmpty.get(fieldDescriptor.getIndex())) {
                        protoBuilder.setField(fieldDescriptor, fieldEntry.getValue());
                    }
                }
                recordBuilder.setRecord(protoBuilder.build());
            }
            if (changeProto.hasOldVersion()) {
                FDBRecordVersion recordVersion = FDBRecordVersion.fromBytes(changeProto.getOldVersion().toByteArray());
                if (!recordVersion.isComplete()) {
                    // Not able to complete it in the middle of the serialized protobuf, so deferred to here.
                    recordVersion = recordVersion.withCommittedVersion(version.getGlobalVersion());
                }
                recordBuilder.setVersion(recordVersion);
            }
            oldRecord = recordBuilder.build();
        } else {
            oldRecord = null;
        }
        return new RecordChangesEntry<>(key, oldRecord, newRecord);
    }
}
