/*
 * FDBRecordStoreClearIndexDataTest.java
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

import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Forward-looking tests verifying that the methods that clear per-index data cover every
 * per-index value of {@link FDBRecordStoreKeyspace}.
 *
 * <p>The lifecycle methods under test are
 * {@link FDBRecordStore#clearAndMarkIndexWriteOnly(Index)} (which routes through
 * {@code clearIndexData}), {@link FDBRecordStore#removeFormerIndex(FormerIndex)}, and
 * {@link FDBRecordStore#rebuildAllIndexes()}.</p>
 *
 * <p>Each test is parameterized over the relevant per-index keyspace and writes a sentinel
 * key under the per-index path before invoking the lifecycle method, then asserts that the
 * sentinel is gone. {@link #keyspaceIsCategorized} fails by design if a future enum value
 * isn't categorized in {@link #NON_PER_INDEX_KEYSPACES} or
 * {@link #CLEARED_BY_CLEAR_INDEX_DATA}, forcing the author to decide which lifecycle methods
 * need updating.</p>
 *
 * <p>This complements the focused regression test for {@code INDEX_SLIDING_WINDOW_SPACE} in
 * {@code SlidingWindowIndexTest} by ensuring future per-index keyspaces don't quietly slip
 * past the same lifecycle gaps.</p>
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreClearIndexDataTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "MySimpleRecord$str_value_indexed";

    /**
     * Keyspaces that are NOT keyed by {@code index.getSubspaceTupleKey()} and so are
     * intentionally outside the scope of the per-index lifecycle methods. New entries must be
     * justified by a comment.
     */
    private static final Set<FDBRecordStoreKeyspace> NON_PER_INDEX_KEYSPACES = ImmutableSet.of(
            FDBRecordStoreKeyspace.STORE_INFO,             // store header
            FDBRecordStoreKeyspace.RECORD,                 // record data
            FDBRecordStoreKeyspace.RECORD_COUNT,           // global record counts
            FDBRecordStoreKeyspace.RECORD_VERSION_SPACE,   // per-record versions
            FDBRecordStoreKeyspace.INDEX_STATE_SPACE       // keyed by index NAME, not subspace tuple key
    );

    /**
     * For each per-index keyspace, the {@link Tuple} suffix to write a sentinel under within
     * {@code (keyspace.key(), index.getSubspaceTupleKey(), suffix...)} for the
     * {@code clearAndMarkIndexWriteOnly} test.
     *
     * <p>Most keyspaces are full-range cleared, so any suffix works. INDEX_BUILD_SPACE is
     * special: {@code clearIndexData} only clears specific sub-keys (1L = scanned-records,
     * 2L = type-version, 3L–6L = scrub ranges, 7L = heartbeats), preserving the lock subspace
     * at sub-key 0L. The sentinel here is placed under sub-key 1L, which is cleared. The
     * lock-preservation contract is verified separately by
     * {@link #clearAndMarkPreservesIndexBuildLock()}.</p>
     */
    private static final Map<FDBRecordStoreKeyspace, Tuple> CLEARED_BY_CLEAR_INDEX_DATA = ImmutableMap.<FDBRecordStoreKeyspace, Tuple>builder()
            .put(FDBRecordStoreKeyspace.INDEX, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SLIDING_WINDOW_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_RANGE_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_BUILD_SPACE, Tuple.from(1L, "test-sentinel"))
            .build();

    /**
     * Per-index keyspaces cleared by {@link FDBRecordStore#removeFormerIndex(FormerIndex)} for
     * the former index's subspace tuple key. INDEX_BUILD_SPACE is intentionally absent —
     * {@code removeFormerIndex} does not currently clear that keyspace for the former index.
     * If that ever changes, add it here.
     */
    private static final Map<FDBRecordStoreKeyspace, Tuple> CLEARED_BY_REMOVE_FORMER_INDEX = ImmutableMap.<FDBRecordStoreKeyspace, Tuple>builder()
            .put(FDBRecordStoreKeyspace.INDEX, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SLIDING_WINDOW_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_RANGE_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE, Tuple.from("test-sentinel"))
            .build();

    /**
     * Per-index keyspaces cleared by {@link FDBRecordStore#rebuildAllIndexes()} via a
     * full-range clear. INDEX_BUILD_SPACE is intentionally absent — rebuild manages its own
     * build state and does not range-clear that keyspace. If that ever changes, add it here.
     */
    private static final Map<FDBRecordStoreKeyspace, Tuple> CLEARED_BY_REBUILD_ALL = ImmutableMap.<FDBRecordStoreKeyspace, Tuple>builder()
            .put(FDBRecordStoreKeyspace.INDEX, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_SLIDING_WINDOW_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_RANGE_SPACE, Tuple.from("test-sentinel"))
            .put(FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE, Tuple.from("test-sentinel"))
            .build();

    static Stream<Arguments> clearedByClearIndexData() {
        return CLEARED_BY_CLEAR_INDEX_DATA.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    static Stream<Arguments> clearedByRemoveFormerIndex() {
        return CLEARED_BY_REMOVE_FORMER_INDEX.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    static Stream<Arguments> clearedByRebuildAll() {
        return CLEARED_BY_REBUILD_ALL.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    /**
     * Forward-looking guard: if a new value is added to {@link FDBRecordStoreKeyspace} but is
     * not categorized in {@link #NON_PER_INDEX_KEYSPACES} or
     * {@link #CLEARED_BY_CLEAR_INDEX_DATA}, this test fails. The failure message points to
     * the tables to update.
     */
    @ParameterizedTest
    @EnumSource(FDBRecordStoreKeyspace.class)
    void keyspaceIsCategorized(FDBRecordStoreKeyspace keyspace) {
        final boolean nonPerIndex = NON_PER_INDEX_KEYSPACES.contains(keyspace);
        final boolean inClearIndexData = CLEARED_BY_CLEAR_INDEX_DATA.containsKey(keyspace);
        if (nonPerIndex) {
            assertFalse(inClearIndexData,
                    keyspace + " is in NON_PER_INDEX_KEYSPACES but also in CLEARED_BY_CLEAR_INDEX_DATA");
            assertFalse(CLEARED_BY_REMOVE_FORMER_INDEX.containsKey(keyspace),
                    keyspace + " is in NON_PER_INDEX_KEYSPACES but also in CLEARED_BY_REMOVE_FORMER_INDEX");
            assertFalse(CLEARED_BY_REBUILD_ALL.containsKey(keyspace),
                    keyspace + " is in NON_PER_INDEX_KEYSPACES but also in CLEARED_BY_REBUILD_ALL");
        } else {
            assertTrue(inClearIndexData,
                    keyspace + " is per-index but not categorized in CLEARED_BY_CLEAR_INDEX_DATA. "
                            + "Either add it (and verify FDBRecordStore.clearIndexData clears it), or list "
                            + "it in NON_PER_INDEX_KEYSPACES with a comment justifying why it isn't keyed by "
                            + "index subspace tuple key. Also consider whether removeFormerIndex and "
                            + "rebuildAllIndexes need similar updates.");
        }
    }

    @ParameterizedTest
    @MethodSource("clearedByClearIndexData")
    void clearAndMarkIndexWriteOnlyClearsKeyspace(FDBRecordStoreKeyspace keyspace, Tuple suffix) throws Exception {
        runClearLifecycleTest(keyspace, suffix, recordStore -> {
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            recordStore.clearAndMarkIndexWriteOnly(index).join();
        });
    }

    /**
     * Companion to {@link #clearAndMarkIndexWriteOnlyClearsKeyspace} that pins down the
     * lock-preservation contract for INDEX_BUILD_SPACE: a sentinel under the lock sub-key
     * (0L) must survive a clear, while sentinels under any other sub-key are removed (the
     * latter is verified by the parameterized test above).
     */
    @Test
    void clearAndMarkPreservesIndexBuildLock() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            context.ensureActive().set(lockSentinelKey(index), new byte[]{1});
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            assertNotNull(context.ensureActive().get(lockSentinelKey(index)).join(),
                    "INDEX_BUILD_SPACE lock subspace must be preserved by clearAndMarkIndexWriteOnly");
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("clearedByRemoveFormerIndex")
    void removeFormerIndexClearsKeyspace(FDBRecordStoreKeyspace keyspace, Tuple suffix) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            commit(context);
        }
        final Object subspaceKey;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            subspaceKey = index.getSubspaceKey();
            context.ensureActive().set(sentinelKey(index, keyspace, suffix), new byte[]{1});
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            assertNotNull(context.ensureActive().get(sentinelKey(index, keyspace, suffix)).join(),
                    "Sentinel under " + keyspace + " was not present before removeFormerIndex");
            // Synthetic FormerIndex pointing at the same subspace key as the live index. We
            // pick a different former-name to avoid colliding with the live index's
            // INDEX_STATE_SPACE entry.
            final FormerIndex formerIndex = new FormerIndex(subspaceKey, 1, 2, INDEX_NAME + "_synthetic_former");
            recordStore.removeFormerIndex(formerIndex);
            assertNull(context.ensureActive().get(sentinelKey(index, keyspace, suffix)).join(),
                    "Sentinel under " + keyspace + " (suffix " + suffix + ") survived removeFormerIndex; "
                            + "removeFormerIndex probably needs to be updated to clear " + keyspace);
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("clearedByRebuildAll")
    void rebuildAllIndexesClearsKeyspace(FDBRecordStoreKeyspace keyspace, Tuple suffix) throws Exception {
        runClearLifecycleTest(keyspace, suffix, recordStore -> recordStore.rebuildAllIndexes().join());
    }

    // ===== Helpers =====

    private void runClearLifecycleTest(FDBRecordStoreKeyspace keyspace, Tuple suffix,
                                       Consumer<FDBRecordStore> action) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            context.ensureActive().set(sentinelKey(index, keyspace, suffix), new byte[]{1});
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            assertNotNull(context.ensureActive().get(sentinelKey(index, keyspace, suffix)).join(),
                    "Sentinel under " + keyspace + " was not present before the clear");
            action.accept(recordStore);
            assertNull(context.ensureActive().get(sentinelKey(index, keyspace, suffix)).join(),
                    "Sentinel under " + keyspace + " (suffix " + suffix + ") survived the clear; "
                            + "lifecycle method probably needs to be updated to clear " + keyspace);
            commit(context);
        }
    }

    private byte[] sentinelKey(Index index, FDBRecordStoreKeyspace keyspace, Tuple suffix) {
        return recordStore.getSubspace()
                .subspace(Tuple.from(keyspace.key(), index.getSubspaceTupleKey()))
                .pack(suffix);
    }

    private byte[] lockSentinelKey(Index index) {
        // Lock subspace lives at sub-key 0L under INDEX_BUILD_SPACE — see IndexingSubspaces.
        return recordStore.getSubspace()
                .subspace(Tuple.from(FDBRecordStoreKeyspace.INDEX_BUILD_SPACE.key(), index.getSubspaceTupleKey()))
                .pack(Tuple.from(0L, "lock-sentinel"));
    }
}
