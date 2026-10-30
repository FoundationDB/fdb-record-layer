/*
 * RecordStoreOperationContext.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Utility class for consolidating logging by maintenance operations on an {@link FDBRecordStore}.
 *
 * <p>A context object tracks mutations to store headers and indexes and generates a compact summary log message on
 * demand. User version, cacheability, and index actions are logged at DEBUG for new stores and at INFO otherwise.
 * Everything else is unconditionally logged at INFO.</p>
 *
 * <p>
 * Summary format:
 * </p>
 *
 * <ol>
 *     <li>
 *         Store header changes are recorded as a pair {@code old_<field>} and {@code new_<field>} for each field that changed.
 *         A line for a new store has only {@code new_} values.
 *     </li>
 *     <li>
 *         Index changes are summarized in a single field {@code index_changes} as a comma-delimited list of entries of the form
 *         {@code index_name:subspace_key:action:old_state:new_state}. The action is an {@link IndexAction}. The
 *         states are {@link IndexState} names. A component that's inapplicable (e.g., old state for a new index) is
 *         {@code -}. Index names and subspace keys are logged with any {@code :} or {@code ,} removed. At most
 *         {@code MAX_LIST_ENTRIES} are listed; {@code index_changes_truncated="true"} when there are more.
 *     </li>
 *     <li>
 *         Miscellany:
 *         <ul>
 *             <li>{@code removed_former_indexes}: a comma-delimited list of the former indexes whose data was
 *             cleared, as {@code index_name:subspace_key}</li>
 *             <li>{@code record_versions_migrated}: true when record versions were moved from the legacy version
 *             subspace into the record subspace</li>
 *             <li>{@code records_empty}: whether the scan for records made when deciding how to build new indexes
 *             found none</li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <p>
 * Example:
 * </p>
 *
 * <pre>{@code
 *   ttl="record store state changed"
 *   index_change_count="3"
 *   index_changes="fooIndex:4:replaced:READABLE:DISABLED, barIndex:2:uniqueness_dropped:-:-,
 *       bazIndex:6:built_inline:-:READABLE"
 *   key_space_path="/environment:'prod'/store:42"
 *   new_cacheable="true"
 *   new_format_version="15"
 *   new_meta_data_version="7"
 *   new_omit_unsplit_record_suffix="true"
 *   new_record_count_key="RecordTypeKey"
 *   new_store="true"
 *   new_user_version="3"
 *   old_cacheable="false"
 *   old_format_version="4"
 *   old_meta_data_version="6"
 *   old_omit_unsplit_record_suffix="false"
 *   old_record_count_key="none"
 *   old_user_version="2"
 *   operation_failed="true"
 *   reason="EXPLICIT"
 *   record_versions_migrated="true"
 *   records_empty="false"
 *   removed_former_index_count="1"
 *   removed_former_indexes="quuxIndex:3"
 *   store_operation="CHECK_VERSION"
 * }</pre>
 *
 * <p>
 * Sample Splunk QL for working with index changes:
 * </p>
 *
 * <pre>{@code
 * index=... ttl="record store state changed" index_changes=*
 * | makemv delim=", " index_changes
 * | mvexpand index_changes
 * | rex field=index_changes "^(?<index_name>[^:]+):(?<subspace_key>[^:]+):(?<index_action>[^:]+):(?<old_state>[^:]+):(?<new_state>[^:]+)$"
 * | stats count by old_state new_state index_action
 * }</pre>
 */
@API(API.Status.INTERNAL)
class RecordStoreOperationContext {
    /** The store operations that create a context. */
    enum Operation {
        CHECK_VERSION,
        REBUILD_ALL_INDEXES,
        REBUILD_INDEX,
        CLEAR_AND_MARK_INDEX_WRITE_ONLY,
        MARK_INDEX_WRITE_ONLY,
        MARK_INDEX_DISABLED,
        MARK_INDEX_READABLE,
        MARK_INDEX_READABLE_OR_UNIQUE_PENDING,
        UNCHECKED_MARK_INDEX_READABLE,
        REMOVE_FORMER_INDEX,
        REMOVE_REPLACED_INDEXES,
        REPAIR_MISSING_HEADER
    }

    static final int MAX_LIST_ENTRIES = 100;
    private static final String STATIC_MESSAGE = "record store state changed";
    private static final String NOT_APPLICABLE = "-";

    @Nonnull
    private final Operation operation;
    @Nullable
    private final FDBRecordStore.RebuildIndexReason reason;
    @Nullable
    private final RecordMetaDataProto.DataStoreInfo headerSnapshot;
    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final FDBRecordContext recordContext;

    private final Map<String, IndexChange> indexChanges = new TreeMap<>();
    private final Set<String> removedFormerIndexes = new TreeSet<>();
    private boolean recordVersionsMigrated;
    @Nullable
    private Boolean recordsEmpty;
    private boolean failed;

    RecordStoreOperationContext(@Nonnull Operation operation,
                                  @Nullable FDBRecordStore.RebuildIndexReason reason,
                                  @Nullable RecordMetaDataProto.DataStoreInfo headerSnapshot,
                                  @Nonnull SubspaceProvider subspaceProvider,
                                  @Nonnull FDBRecordContext recordContext) {
        this.operation = operation;
        this.reason = reason;
        this.headerSnapshot = headerSnapshot;
        this.subspaceProvider = subspaceProvider;
        this.recordContext = recordContext;
    }

    synchronized void indexStateChanged(@Nonnull String indexName, @Nullable Index index,
                                        @Nonnull IndexState prior, @Nonnull IndexState next) {
        indexStateChange(indexName, index, prior, next);
    }

    synchronized void indexRebuilt(@Nonnull Index index, boolean noRecords) {
        indexAction(index, noRecords ? IndexAction.READABLE_NO_RECORDS : IndexAction.BUILT_INLINE);
    }

    synchronized void indexRebuildFailed(@Nonnull Index index) {
        indexAction(index, IndexAction.BUILD_FAILED);
    }

    synchronized void indexDisabledAfterFailure(@Nonnull Index index) {
        indexAction(index, IndexAction.DISABLED_AFTER_FAILURE);
    }

    synchronized void uniquenessDropped(@Nonnull Index index) {
        indexAction(index, IndexAction.UNIQUENESS_DROPPED);
    }

    synchronized void replacedIndexRetired(@Nonnull Index index) {
        indexAction(index, IndexAction.REPLACED);
    }

    synchronized void formerIndexRemoved(@Nullable String formerName, @Nonnull Object subspaceKey) {
        removedFormerIndexes.add((formerName == null ? NOT_APPLICABLE : sanitize(formerName)) + ':' + sanitize(String.valueOf(subspaceKey)));
    }

    synchronized void recordVersionsMigrated() {
        recordVersionsMigrated = true;
    }

    synchronized void scanned(boolean empty) {
        recordsEmpty = empty;
    }

    synchronized void operationFailed() {
        failed = true;
    }

    /**
     * Whether the operation began on a store that had no header.
     * @return {@code true} if the header snapshot is that of a store being created
     */
    boolean isNewStore() {
        return headerSnapshot != null && FDBRecordStore.isNewStoreHeader(headerSnapshot);
    }

    /**
     * Whether the index was added after the metadata version in the header snapshot.
     * @param index the index to check
     * @return {@code true} if the index did not exist in the store before this operation
     */
    boolean isNewIndex(@Nullable Index index) {
        return index != null && headerSnapshot != null
                && (isNewStore() || index.getAddedVersion() > headerSnapshot.getMetaDataversion());
    }

    /**
     * Whether the given header differs from the header snapshot.
     * @param info the header as modified by the operation
     * @return {@code true} if the header has changed and must be written
     */
    boolean headerChangedFrom(@Nonnull RecordMetaDataProto.DataStoreInfo.Builder info) {
        return headerSnapshot != null && !headerSnapshot.equals(info.build());
    }

    @Nullable
    synchronized KeyValueLogMessage infoMessage(@Nullable RecordMetaDataProto.DataStoreInfo currentHeader) {
        final KeyValueLogMessage message = KeyValueLogMessage.build(STATIC_MESSAGE, LogMessageKeys.STORE_OPERATION, operation.name());
        boolean any = false;
        if (headerSnapshot != null && currentHeader != null) {
            if (isNewStore()) {
                message.addKeyAndValue(LogMessageKeys.NEW_STORE, true);
                message.addKeyAndValue(LogMessageKeys.NEW_FORMAT_VERSION, currentHeader.getFormatVersion());
                message.addKeyAndValue(LogMessageKeys.NEW_META_DATA_VERSION, currentHeader.getMetaDataversion());
                any = true;
            } else {
                any |= addIfChanged(message, LogMessageKeys.OLD_FORMAT_VERSION, LogMessageKeys.NEW_FORMAT_VERSION,
                        headerSnapshot.getFormatVersion(), currentHeader.getFormatVersion());
                any |= addIfChanged(message, LogMessageKeys.OLD_META_DATA_VERSION, LogMessageKeys.NEW_META_DATA_VERSION,
                        headerSnapshot.getMetaDataversion(), currentHeader.getMetaDataversion());
                any |= addIfChanged(message, LogMessageKeys.OLD_USER_VERSION, LogMessageKeys.NEW_USER_VERSION,
                        headerSnapshot.getUserVersion(), currentHeader.getUserVersion());
                any |= addIfChanged(message, LogMessageKeys.OLD_CACHEABLE, LogMessageKeys.NEW_CACHEABLE,
                        headerSnapshot.getCacheable(), currentHeader.getCacheable());
                any |= addIfChanged(message, LogMessageKeys.OLD_OMIT_UNSPLIT_RECORD_SUFFIX, LogMessageKeys.NEW_OMIT_UNSPLIT_RECORD_SUFFIX,
                        headerSnapshot.getOmitUnsplitRecordSuffix(), currentHeader.getOmitUnsplitRecordSuffix());
                any |= addIfChanged(message, LogMessageKeys.OLD_RECORD_COUNT_KEY, LogMessageKeys.NEW_RECORD_COUNT_KEY,
                        recordCountKeyString(headerSnapshot), recordCountKeyString(currentHeader));
            }
        }
        final List<String> indexChanges = new ArrayList<>();
        for (IndexChange change : this.indexChanges.values()) {
            if (!isNewStore() || change.hasStateChange()) {
                indexChanges.add(change.render());
            }
        }
        any |= addList(message, indexChanges,
                LogMessageKeys.INDEX_CHANGE_COUNT, LogMessageKeys.INDEX_CHANGES, LogMessageKeys.INDEX_CHANGES_TRUNCATED);
        any |= addList(message, new ArrayList<>(removedFormerIndexes),
                LogMessageKeys.REMOVED_FORMER_INDEX_COUNT, LogMessageKeys.REMOVED_FORMER_INDEXES, LogMessageKeys.REMOVED_FORMER_INDEXES_TRUNCATED);
        if (recordVersionsMigrated) {
            message.addKeyAndValue(LogMessageKeys.RECORD_VERSIONS_MIGRATED, true);
            any = true;
        }
        if (recordsEmpty != null && (!recordsEmpty || !isNewStore())) {
            message.addKeyAndValue(LogMessageKeys.RECORDS_EMPTY, recordsEmpty);
            any = true;
        }
        return any ? finish(message) : null;
    }

    @Nullable
    synchronized KeyValueLogMessage debugMessage(@Nullable RecordMetaDataProto.DataStoreInfo currentHeader) {
        if (!isNewStore()) {
            return null;
        }
        final KeyValueLogMessage message = KeyValueLogMessage.build(STATIC_MESSAGE, LogMessageKeys.STORE_OPERATION, operation.name());
        boolean any = false;
        if (currentHeader != null) {
            if (currentHeader.hasUserVersion()) {
                message.addKeyAndValue(LogMessageKeys.NEW_USER_VERSION, currentHeader.getUserVersion());
                any = true;
            }
            if (currentHeader.hasCacheable()) {
                message.addKeyAndValue(LogMessageKeys.NEW_CACHEABLE, currentHeader.getCacheable());
                any = true;
            }
        }
        final List<String> entries = new ArrayList<>();
        for (IndexChange change : indexChanges.values()) {
            if (!change.hasStateChange()) {
                entries.add(change.render());
            }
        }
        any |= addList(message, entries,
                LogMessageKeys.INDEX_CHANGE_COUNT, LogMessageKeys.INDEX_CHANGES, LogMessageKeys.INDEX_CHANGES_TRUNCATED);
        if (Boolean.TRUE.equals(recordsEmpty)) {
            message.addKeyAndValue(LogMessageKeys.RECORDS_EMPTY, true);
            any = true;
        }
        return any ? finish(message) : null;
    }

    @Nonnull
    private IndexChange getIndexChange(@Nonnull String indexName, @Nullable Index index) {
        return indexChanges.computeIfAbsent(indexName,
                name -> new IndexChange(name, index == null ? null : String.valueOf(index.getSubspaceKey()), isNewIndex(index)));
    }

    private void indexStateChange(@Nonnull String indexName, @Nullable Index index,
                                  @Nonnull IndexState prior, @Nonnull IndexState next) {
        final IndexChange change = getIndexChange(indexName, index);
        if (change.priorState == null) {
            change.priorState = prior;
        }
        change.nextState = next;
    }

    private void indexAction(@Nonnull Index index, @Nonnull IndexAction action) {
        getIndexChange(index.getName(), index).action = action;
    }

    // Only called for a line that will be emitted, so the key-space path is rendered only when needed.
    @Nonnull
    private KeyValueLogMessage finish(@Nonnull KeyValueLogMessage message) {
        if (reason != null) {
            message.addKeyAndValue(LogMessageKeys.REASON, reason.name());
        }
        if (failed) {
            message.addKeyAndValue(LogMessageKeys.OPERATION_FAILED, true);
        }
        return message.addKeyAndValue(subspaceProvider.logKey(), subspaceProvider.toString(recordContext));
    }

    private static boolean addIfChanged(@Nonnull KeyValueLogMessage message,
                                        @Nonnull LogMessageKeys oldKey, @Nonnull LogMessageKeys newKey,
                                        @Nonnull Object oldValue, @Nonnull Object newValue) {
        if (oldValue.equals(newValue)) {
            return false;
        }
        message.addKeyAndValue(oldKey, oldValue);
        message.addKeyAndValue(newKey, newValue);
        return true;
    }

    private static boolean addList(@Nonnull KeyValueLogMessage message, @Nonnull List<String> entries,
                                   @Nonnull LogMessageKeys countKey, @Nonnull LogMessageKeys listKey,
                                   @Nonnull LogMessageKeys truncatedKey) {
        if (entries.isEmpty()) {
            return false;
        }
        entries.sort(null);
        message.addKeyAndValue(countKey, entries.size());
        message.addKeyAndValue(listKey, String.join(", ", entries.subList(0, Math.min(entries.size(), MAX_LIST_ENTRIES))));
        if (entries.size() > MAX_LIST_ENTRIES) {
            message.addKeyAndValue(truncatedKey, true);
        }
        return true;
    }

    // Removes the characters that separate list entries and the parts of an entry.
    @Nonnull
    private static String sanitize(@Nonnull String value) {
        return value.replace(":", "").replace(",", "");
    }

    @Nonnull
    private static String recordCountKeyString(@Nonnull RecordMetaDataProto.DataStoreInfo header) {
        return header.hasRecordCountKey() ? KeyExpression.fromProto(header.getRecordCountKey()).toString() : "none";
    }

    private enum IndexAction {
        /** The index was built in the operation's transaction. */
        BUILT_INLINE,
        /** The index covers only record types new to the store, so it was made readable without a build. */
        READABLE_NO_RECORDS,
        /** The inline build failed, and the index was left in its new state. */
        BUILD_FAILED,
        /** Building the index or setting its state failed, so it was disabled. */
        DISABLED_AFTER_FAILURE,
        /** The index is no longer unique, so its uniqueness violations were cleared. */
        UNIQUENESS_DROPPED,
        /** The index was disabled because the indexes that replace it are readable. */
        REPLACED;

        @Nonnull
        String logName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final class IndexChange {
        @Nonnull
        private final String indexName;
        @Nullable
        private final String subspaceKey;
        private final boolean newIndex;
        @Nullable
        private IndexState priorState;
        @Nullable
        private IndexState nextState;
        @Nullable
        private IndexAction action;

        private IndexChange(@Nonnull String indexName, @Nullable String subspaceKey, boolean newIndex) {
            this.indexName = indexName;
            this.subspaceKey = subspaceKey;
            this.newIndex = newIndex;
        }

        private boolean hasStateChange() {
            return nextState != null;
        }

        // Renders as name:subspace_key:action:old:new, where any part after the name may be absent.
        @Nonnull
        private String render() {
            final String prior;
            final String current;
            if (nextState != null) {
                prior = newIndex ? NOT_APPLICABLE : stateName(priorState);
                current = nextState.name();
            } else if (newIndex) {
                // A new index that was never written to has no state key, which means it is readable.
                prior = NOT_APPLICABLE;
                current = IndexState.READABLE.name();
            } else {
                prior = NOT_APPLICABLE;
                current = NOT_APPLICABLE;
            }
            return sanitize(indexName) + ':' + (subspaceKey == null ? NOT_APPLICABLE : sanitize(subspaceKey))
                    + ':' + (action == null ? NOT_APPLICABLE : action.logName()) + ':' + prior + ':' + current;
        }

        @Nonnull
        private static String stateName(@Nullable IndexState state) {
            return state == null ? NOT_APPLICABLE : state.name();
        }
    }
}
