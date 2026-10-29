/*
 * RecordStoreOperationContextTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreOperationContext.Operation;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of {@link RecordStoreOperationContext}, which do not need a database.
 */
class RecordStoreOperationContextTest {
    private static final SubspaceProvider SUBSPACE_PROVIDER = new SubspaceProviderBySubspace(new Subspace(Tuple.from("store")));

    @Nonnull
    private static RecordStoreOperationContext newContext(@Nonnull Operation operation,
                                            @Nullable RecordMetaDataProto.DataStoreInfo snapshot) {
        return new RecordStoreOperationContext(operation, null, snapshot, SUBSPACE_PROVIDER, Mockito.mock(FDBRecordContext.class));
    }

    @Nonnull
    private static Index index(@Nonnull String name, int addedVersion) {
        final Index index = new Index(name, Key.Expressions.field("field"));
        index.setAddedVersion(addedVersion);
        return index;
    }

    @Nonnull
    private static RecordMetaDataProto.DataStoreInfo header(int formatVersion, int metaDataVersion) {
        return RecordMetaDataProto.DataStoreInfo.newBuilder()
                .setFormatVersion(formatVersion)
                .setMetaDataversion(metaDataVersion)
                .build();
    }

    @Nonnull
    private static Map<String, String> fields(@Nullable KeyValueLogMessage message) {
        assertThat(message).isNotNull();
        return message.getKeyValueMap();
    }

    @Test
    void nothingRecordedEmitsNothing() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        assertThat(ctx.infoMessage(snapshot)).isNull();
        assertThat(ctx.debugMessage(snapshot)).isNull();
        assertThat(ctx.headerChangedFrom(snapshot.toBuilder())).isFalse();
    }

    @Test
    void headerChangeOnExistingStoreIsInfoOnly() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final RecordMetaDataProto.DataStoreInfo current = snapshot.toBuilder()
                .setMetaDataversion(6)
                .setUserVersion(3)
                .build();
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        assertThat(ctx.headerChangedFrom(current.toBuilder())).isTrue();
        assertThat(ctx.debugMessage(current)).isNull();
        final Map<String, String> fields = fields(ctx.infoMessage(current));
        assertThat(fields)
                .containsEntry(LogMessageKeys.OLD_META_DATA_VERSION.toString(), "5")
                .containsEntry(LogMessageKeys.NEW_META_DATA_VERSION.toString(), "6")
                .containsEntry(LogMessageKeys.OLD_USER_VERSION.toString(), "0")
                .containsEntry(LogMessageKeys.NEW_USER_VERSION.toString(), "3")
                .containsKey(LogMessageKeys.SUBSPACE.toString())
                .doesNotContainKey(LogMessageKeys.OLD_FORMAT_VERSION.toString())
                .doesNotContainKey(LogMessageKeys.NEW_FORMAT_VERSION.toString());
    }

    @Test
    void newStoreSplitsHeaderBetweenInfoAndDebug() {
        final RecordMetaDataProto.DataStoreInfo snapshot = RecordMetaDataProto.DataStoreInfo.getDefaultInstance();
        final RecordMetaDataProto.DataStoreInfo current = RecordMetaDataProto.DataStoreInfo.newBuilder()
                .setFormatVersion(10)
                .setMetaDataversion(5)
                .setUserVersion(2)
                .setCacheable(true)
                .build();
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        assertThat(ctx.isNewStore()).isTrue();

        final Map<String, String> info = fields(ctx.infoMessage(current));
        assertThat(info)
                .containsEntry(LogMessageKeys.NEW_STORE.toString(), "true")
                .containsEntry(LogMessageKeys.NEW_FORMAT_VERSION.toString(), "10")
                .containsEntry(LogMessageKeys.NEW_META_DATA_VERSION.toString(), "5")
                .doesNotContainKey(LogMessageKeys.NEW_USER_VERSION.toString())
                .doesNotContainKey(LogMessageKeys.NEW_CACHEABLE.toString())
                .doesNotContainKey(LogMessageKeys.OLD_FORMAT_VERSION.toString());

        final Map<String, String> debug = fields(ctx.debugMessage(current));
        assertThat(debug)
                .containsEntry(LogMessageKeys.NEW_USER_VERSION.toString(), "2")
                .containsEntry(LogMessageKeys.NEW_CACHEABLE.toString(), "true")
                .doesNotContainKey(LogMessageKeys.NEW_STORE.toString())
                .doesNotContainKey(LogMessageKeys.NEW_FORMAT_VERSION.toString());
    }

    @Test
    void inlineBuildCollapsesIntermediateState() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index existing = index("existing", 2);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexRebuilt(existing, false);
        ctx.indexStateChanged("existing", existing, IndexState.DISABLED, IndexState.WRITE_ONLY);
        ctx.indexStateChanged("existing", existing, IndexState.WRITE_ONLY, IndexState.READABLE);
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "existing:existing:built_inline:DISABLED:READABLE")
                .containsEntry(LogMessageKeys.INDEX_CHANGE_COUNT.toString(), "1");
    }

    @Test
    void newIndexPriorStateIsAbsent() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index added = index("added", 6);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        assertThat(ctx.isNewIndex(added)).isTrue();
        assertThat(ctx.isNewIndex(index("old", 5))).isFalse();
        ctx.indexStateChanged("added", added, IndexState.READABLE, IndexState.DISABLED);
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "added:added:-:-:DISABLED");
    }

    @Test
    void newIndexBuiltWithoutRecordsIsReadable() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index added = index("added", 6);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexRebuilt(added, true);
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "added:added:readable_no_records:-:READABLE");
    }

    @Test
    void indexesReadableWithoutRecordsInNewStoreAreDebug() {
        final RecordMetaDataProto.DataStoreInfo snapshot = RecordMetaDataProto.DataStoreInfo.getDefaultInstance();
        final RecordMetaDataProto.DataStoreInfo current = header(10, 5);
        final Index readable = index("readable", 1);
        final Index disabled = index("disabled", 1);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexRebuilt(readable, true);
        ctx.indexStateChanged("disabled", disabled, IndexState.READABLE, IndexState.DISABLED);
        assertThat(fields(ctx.debugMessage(current)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "readable:readable:readable_no_records:-:READABLE");
        assertThat(fields(ctx.infoMessage(current)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "disabled:disabled:-:-:DISABLED");
    }

    @Test
    void formerIndexRemovalsAreListedSeparately() {
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, header(10, 5));
        ctx.formerIndexRemoved("gone", "gone");
        ctx.formerIndexRemoved(null, 17L);
        assertThat(fields(ctx.infoMessage(header(10, 6))))
                .containsEntry(LogMessageKeys.REMOVED_FORMER_INDEX_COUNT.toString(), "2")
                .containsEntry(LogMessageKeys.REMOVED_FORMER_INDEXES.toString(), "-:17, gone:gone")
                .doesNotContainKey(LogMessageKeys.INDEX_CHANGES.toString());
    }

    @Test
    void formerNameReusedByNewIndexAppearsInBothLists() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index reused = index("reused", 6);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.formerIndexRemoved("reused", "reused");
        ctx.indexStateChanged("reused", reused, IndexState.READABLE, IndexState.DISABLED);
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGE_COUNT.toString(), "1")
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "reused:reused:-:-:DISABLED")
                .containsEntry(LogMessageKeys.REMOVED_FORMER_INDEXES.toString(), "reused:reused");
    }

    @Test
    void subspaceKeyIsRenderedAfterName() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index keyed = index("keyed", 2);
        keyed.setSubspaceKey(6L);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexStateChanged("keyed", keyed, IndexState.READABLE, IndexState.DISABLED);
        ctx.indexStateChanged("unknown", null, IndexState.READABLE, IndexState.DISABLED);
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "keyed:6:-:READABLE:DISABLED, unknown:-:-:READABLE:DISABLED");
    }

    @Test
    void delimitersAreRemovedFromNamesAndKeys() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index delimited = index("a:b,c", 2);
        delimited.setSubspaceKey("k:1,2");
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexStateChanged("a:b,c", delimited, IndexState.READABLE, IndexState.DISABLED);
        ctx.formerIndexRemoved("x:y", "z,w");
        assertThat(fields(ctx.infoMessage(snapshot)))
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "abc:k12:-:READABLE:DISABLED")
                .containsEntry(LogMessageKeys.REMOVED_FORMER_INDEXES.toString(), "xy:zw");
    }

    @Test
    void indexStateChangesAreSortedAndTruncated() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        final int count = RecordStoreOperationContext.MAX_LIST_ENTRIES + 5;
        for (int i = count - 1; i >= 0; i--) {
            final String name = String.format("index_%04d", i);
            ctx.indexStateChanged(name, index(name, 6), IndexState.READABLE, IndexState.DISABLED);
        }
        final Map<String, String> fields = fields(ctx.infoMessage(snapshot));
        assertThat(fields)
                .containsEntry(LogMessageKeys.INDEX_CHANGE_COUNT.toString(), Integer.toString(count))
                .containsEntry(LogMessageKeys.INDEX_CHANGES_TRUNCATED.toString(), "true");
        final String[] listed = fields.get(LogMessageKeys.INDEX_CHANGES.toString()).split(", ");
        assertThat(listed).hasSize(RecordStoreOperationContext.MAX_LIST_ENTRIES);
        assertThat(listed[0]).startsWith("index_0000:");
        assertThat(listed[listed.length - 1]).startsWith(String.format("index_%04d:", RecordStoreOperationContext.MAX_LIST_ENTRIES - 1));
    }

    @Test
    void failureActionsAreRendered() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(10, 5);
        final Index failed = index("failed", 2);
        final Index fallback = index("fallback", 2);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.indexRebuilt(failed, false);
        ctx.indexStateChanged("failed", failed, IndexState.READABLE, IndexState.WRITE_ONLY);
        ctx.indexRebuildFailed(failed);
        ctx.indexDisabledAfterFailure(fallback);
        assertThat(fields(ctx.infoMessage(snapshot)).get(LogMessageKeys.INDEX_CHANGES.toString()))
                .isEqualTo("failed:failed:build_failed:READABLE:WRITE_ONLY, fallback:fallback:disabled_after_failure:-:-");
    }

    @Test
    void scanOutcomeLevelDependsOnStoreAge() {
        final RecordMetaDataProto.DataStoreInfo existing = header(10, 5);
        final RecordStoreOperationContext nonEmpty = newContext(Operation.CHECK_VERSION, existing);
        nonEmpty.scanned(false);
        assertThat(fields(nonEmpty.infoMessage(existing))).containsEntry(LogMessageKeys.RECORDS_EMPTY.toString(), "false");

        final RecordStoreOperationContext empty = newContext(Operation.CHECK_VERSION, existing);
        empty.scanned(true);
        assertThat(fields(empty.infoMessage(existing))).containsEntry(LogMessageKeys.RECORDS_EMPTY.toString(), "true");

        final RecordMetaDataProto.DataStoreInfo newStore = RecordMetaDataProto.DataStoreInfo.getDefaultInstance();
        final RecordStoreOperationContext emptyNewStore = newContext(Operation.CHECK_VERSION, newStore);
        emptyNewStore.scanned(true);
        assertThat(fields(emptyNewStore.debugMessage(header(10, 5)))).containsEntry(LogMessageKeys.RECORDS_EMPTY.toString(), "true");
        assertThat(emptyNewStore.infoMessage(header(10, 5)).getKeyValueMap()).doesNotContainKey(LogMessageKeys.RECORDS_EMPTY.toString());
    }

    @Test
    void operationWithoutSnapshotNeverComparesHeaders() {
        final RecordStoreOperationContext ctx = newContext(Operation.MARK_INDEX_DISABLED, null);
        assertThat(ctx.isNewStore()).isFalse();
        assertThat(ctx.isNewIndex(index("any", 100))).isFalse();
        assertThat(ctx.headerChangedFrom(header(10, 5).toBuilder())).isFalse();
        ctx.indexStateChanged("any", index("any", 100), IndexState.READABLE, IndexState.DISABLED);
        final Map<String, String> fields = fields(ctx.infoMessage(header(10, 5)));
        assertThat(fields)
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "any:any:-:READABLE:DISABLED")
                .doesNotContainKey(LogMessageKeys.NEW_FORMAT_VERSION.toString())
                .doesNotContainKey(LogMessageKeys.NEW_STORE.toString())
                .doesNotContainKey(LogMessageKeys.OPERATION_FAILED.toString());
        assertThat(ctx.debugMessage(header(10, 5))).isNull();
    }

    @Test
    void failedOperationLogsWhatItChanged() {
        final RecordStoreOperationContext ctx = newContext(Operation.MARK_INDEX_DISABLED, null);
        ctx.operationFailed();
        assertThat(ctx.infoMessage(header(10, 5))).isNull();
        ctx.indexStateChanged("any", index("any", 100), IndexState.READABLE, IndexState.DISABLED);
        assertThat(fields(ctx.infoMessage(header(10, 5))))
                .containsEntry(LogMessageKeys.OPERATION_FAILED.toString(), "true")
                .containsEntry(LogMessageKeys.INDEX_CHANGES.toString(), "any:any:-:READABLE:DISABLED");
    }

    @Test
    void recordVersionMigrationIsInfo() {
        final RecordMetaDataProto.DataStoreInfo snapshot = header(2, 5);
        final RecordStoreOperationContext ctx = newContext(Operation.CHECK_VERSION, snapshot);
        ctx.recordVersionsMigrated();
        assertThat(fields(ctx.infoMessage(snapshot))).containsEntry(LogMessageKeys.RECORD_VERSIONS_MIGRATED.toString(), "true");
    }
}
