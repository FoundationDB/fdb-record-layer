/*
 * DeleteStoreTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.storestate;

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.DeleteStoreMode;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.ParameterizedTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheTestUtils.metaDataVersionStampCacheFactory;
import static com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheTestUtils.testContextSource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DeleteStoreTest extends FDBRecordStoreTestBase {

    /**
     * Validate that deleting a record store causes the record store to go back to the database as it's possible the
     * cached stuff is what was deleted.
     */
    @ParameterizedTest(name = "storeDeletionInSameContext (test context = {0}, deleteMode = {1})")
    @MethodSource("testContextAndDeleteStoreModeSource")
    public void storeDeletionInSameContext(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext,
                                           @Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());

            context.getTimer().reset();
            deleteStoreMode.deleteStore(context, recordStore.getSubspace());
            recordStore.asBuilder().create();
            assertCacheMiss(context.getTimer());

            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());
            path.deleteAllData(context);

            context.getTimer().reset();
            recordStore.asBuilder().create();
            assertCacheMiss(context.getTimer());
        }

        // Deleting all records should not disable the index, so the result should still be cacheable.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/399
        final String disabledIndex = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());
            recordStore.markIndexDisabled(disabledIndex).get();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            recordStore.deleteAllRecords();

            context.getTimer().reset();
            recordStore = recordStore.asBuilder().open();
            assertCacheHit(context.getTimer());
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            commit(context);
        }
    }

    /**
     * After a store is deleted, validate that future transactions need to reload it from cache.
     */
    @ParameterizedTest(name = "storeDeletionAcrossContexts (test context = {0}, deleteMode = {1})")
    @MethodSource("testContextAndDeleteStoreModeSource")
    public void storeDeletionAcrossContexts(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext,
                                            @Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        // Delete by calling deleteStore.
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());
            deleteStoreMode.deleteStore(context, recordStore.getSubspace());
            commit(context);
        }

        // After deleting it, when opening the same store again, it shouldn't be cached.
        try (FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer())) {
            FDBRecordStore store = storeBuilder.setContext(context).create();
            assertCacheMiss(context.getTimer());
            assertTrue(store.setStateCacheability(true));
            commit(context);
        }

        // Delete by calling path.deleteAllData
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertCacheHit(context.getTimer());
            path.deleteAllData(context);
            commit(context);
        }

        try (FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer())) {
            FDBRecordStore store = storeBuilder.setContext(context).create();
            store.setStateCacheabilityAsync(true).get();
            assertCacheMiss(context.getTimer());
            commit(context);
        }

        // Deleting all records should not disable the index state.
        final String disabledIndex = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(disabledIndex).get();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            recordStore.deleteAllRecords();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            commit(context);
        }
    }

    /**
     * Behavior of {@link FDBRecordStore#deleteStore}/{@link FDBRecordStore#deleteStoreAsync} on a non-cacheable store,
     * across both modes.
     *
     * <ul>
     *   <li>{@link DeleteStoreMode#ASYNC ASYNC} (new): reads the header, sees non-cacheable, and skips the meta-data
     *       version-stamp bump. No other client can hold a cached copy of a non-cacheable header, so bumping the
     *       cluster-wide bottleneck on the {@code \xff/metadataVersion} key would be pure waste. This is key to
     *       reducing cache invalidations and conflicts if a cluster is using the meta-data version-stamp, and is
     *       frequently deleting non-cacheable stores. For, example, if you have the relational catalog, and are
     *       frequently creating/deleting unrelated schemas, they would have conflicted with each other, because the
     *       catalog is a store with a cached header.</li>
     *   <li>{@link DeleteStoreMode#SYNC SYNC} (deprecated): bumps the meta-data version stamp unconditionally without
     *       reading the header — the pre-{@code deleteStoreAsync} behavior. Documented here so the deprecated method's
     *       cost is not silently forgotten.</li>
     * </ul>
     */
    @ParameterizedTest(name = "deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp [{0}]")
    @EnumSource(DeleteStoreMode.class)
    void deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp(@Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        ensureMetaDataVersionStampInitialized();
        assertMetaDataVersionStampBehaviorOnNonCacheableOrMissingStore(createStore(false).subspace(), deleteStoreMode);
    }

    /**
     * Behavior of {@link FDBRecordStore#deleteStore}/{@link FDBRecordStore#deleteStoreAsync} on an empty subspace with
     * no store header, mirroring the {@link #deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp}
     * split: {@link DeleteStoreMode#ASYNC ASYNC} sees the missing header and skips the bump;
     * {@link DeleteStoreMode#SYNC SYNC} bumps unconditionally.
     */
    @ParameterizedTest(name = "deleteMissingStoreDoesNotBumpMetaDataVersionStamp [{0}]")
    @EnumSource(DeleteStoreMode.class)
    void deleteMissingStoreDoesNotBumpMetaDataVersionStamp(@Nonnull DeleteStoreMode deleteStoreMode) {
        ensureMetaDataVersionStampInitialized();

        // Use the test's per-instance path, but never open a store there.
        final Subspace subspace;
        try (FDBRecordContext context = openContext()) {
            subspace = path.toSubspace(context);
        }
        assertMetaDataVersionStampBehaviorOnNonCacheableOrMissingStore(subspace, deleteStoreMode);
    }

    /**
     * Snapshot the meta-data version stamp, run a delete of the given subspace via {@code mode},
     * and assert the stamp behaved as the mode's contract requires: {@link DeleteStoreMode#SYNC}
     * bumps unconditionally, {@link DeleteStoreMode#ASYNC} leaves the stamp untouched when the
     * header is absent or marks the store non-cacheable.
     */
    private void assertMetaDataVersionStampBehaviorOnNonCacheableOrMissingStore(final Subspace subspace,
                                                                                @Nonnull DeleteStoreMode deleteStoreMode) {
        final byte[] beforeStamp = getMetaDataVersionStamp();
        assertNotNull(beforeStamp);

        deleteStore(subspace, deleteStoreMode);

        final byte[] afterStamp = getMetaDataVersionStamp();
        assertNotNull(afterStamp);
        if (deleteStoreMode == DeleteStoreMode.SYNC) {
            assertFalse(Arrays.equals(beforeStamp, afterStamp),
                    "deprecated sync deleteStore always bumps the meta-data version stamp, " +
                            "even when the store header is absent or non-cacheable");
        } else {
            assertArrayEquals(beforeStamp, afterStamp,
                    "async deleteStoreAsync must not bump the meta-data version stamp when the " +
                            "store header is absent or non-cacheable — nothing to invalidate");
        }
    }

    /**
     * Complement of {@link #deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp(DeleteStoreMode)}:
     * deleting a cacheable store MUST bump the stamp — otherwise sibling clients could keep serving
     * reads out of a stale cached header long after the store is gone. Both modes bump: SYNC does
     * so unconditionally, ASYNC does so because the header parses as cacheable.
     */
    @ParameterizedTest(name = "deleteCacheableStoreBumpsMetaDataVersionStamp [{0}]")
    @EnumSource(DeleteStoreMode.class)
    void deleteCacheableStoreBumpsMetaDataVersionStamp(@Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        ensureMetaDataVersionStampInitialized();

        final Subspace subspace = createStore(true).subspace();
        // Commit above already bumped the stamp (transition to cacheable). Snapshot after that.
        final byte[] beforeStamp = getMetaDataVersionStamp();
        assertNotNull(beforeStamp);

        deleteStore(subspace, deleteStoreMode);

        final byte[] afterStamp = getMetaDataVersionStamp();
        assertNotNull(afterStamp);
        assertFalse(Arrays.equals(beforeStamp, afterStamp),
                "deleting a cacheable store should have bumped the meta-data version stamp");
    }

    /**
     * Sanity check that {@link FDBRecordStore#deleteStore}/{@link FDBRecordStore#deleteStoreAsync}
     * actually clears the store's subspace: both the store header and every previously-saved
     * record disappear on commit.
     */
    @ParameterizedTest
    @EnumSource(DeleteStoreMode.class)
    void deleteStoreClearsSubspace(@Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        final StoreSetup setup = createStoreWithRecords(1L, 2L);
        assertFalse(isSubspaceEmpty(setup.subspace()),
                "store should have data on disk before deletion");

        deleteStore(setup.subspace(), deleteStoreMode);

        // After the delete, the subspace should be entirely empty — no header, no records,
        // no index entries. deleteStore clears subspace.range() unconditionally.
        assertTrue(isSubspaceEmpty(setup.subspace()),
                "store subspace should be empty after deletion");
    }

    /**
     * After a delete, opening the same store with the default
     * {@link FDBRecordStoreBase.StoreExistenceCheck#ERROR_IF_NOT_EXISTS} check (i.e. via
     * {@link FDBRecordStore.Builder#open() Builder.open()}) must throw
     * {@link RecordStoreDoesNotExistException} — the store header is gone, so as far as
     * the record layer is concerned the store no longer exists.
     */
    @ParameterizedTest
    @EnumSource(DeleteStoreMode.class)
    void openAfterDeleteThrows(@Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        final StoreSetup setup = createStoreWithRecords(1L);
        deleteStore(setup.subspace(), deleteStoreMode);

        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordStoreDoesNotExistException.class,
                    () -> setup.builder().copyBuilder().setContext(context).open(),
                    "opening a deleted store should throw RecordStoreDoesNotExistException");
        }
    }

    /**
     * After deleting a store, {@link FDBRecordStore.Builder#create()} must succeed on the
     * same path — a fresh, empty store — proving the delete really wiped every trace of the
     * old store's header rather than merely marking it inactive.
     */
    @ParameterizedTest
    @EnumSource(DeleteStoreMode.class)
    void deletedStoreCanBeRecreated(@Nonnull DeleteStoreMode deleteStoreMode) throws Exception {
        final StoreSetup setup = createStoreWithRecords(1L);
        deleteStore(setup.subspace(), deleteStoreMode);

        // Recreate the store; expect no prior records and be able to save fresh ones.
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recreated = setup.builder().copyBuilder().setContext(context).create();
            assertNotNull(recreated);
            recreated.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L).setStrValueIndexed("new").build());
            commit(context);
        }
    }


    @Nonnull
    public static Stream<Arguments> concurrentOperationPreventsDeleteStore() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(ConcurrentOperation.values()),
                Stream.of(DeleteStoreMode.values()),
                ParameterizedTestUtils.booleans("startCacheable")
        );
    }

    /**
     * The concurrent operation commits BEFORE the delete. What matters is whether the
     * delete's own commit machinery catches the concurrent modification and forces the
     * deleter to abort.
     *
     * <p>The key expectatian is that either:</p>
     * <ul>
     *   <li>The delete succeeds, and the range is empty, and if the store was <em>even</em> cacheable, the meta-data
     *   version-stamp was bumped.</li>
     *   <li>The delete conflicts.</li>
     * </ul>
     */
    @ParameterizedTest
    @MethodSource
    void concurrentOperationPreventsDeleteStore(@Nonnull ConcurrentOperation op,
                                                @Nonnull DeleteStoreMode deleteStoreMode,
                                                boolean startCacheable) throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));
        ensureMetaDataVersionStampInitialized();

        final StoreSetup setup = createStore(startCacheable);

        final boolean deleterCommitted;
        try (FDBRecordContext deleterContext = fdb.openContext(null, new FDBStoreTimer())) {
            // Pin the deleter's read version BEFORE the concurrent op commits.
            deleterContext.getReadVersion();

            // Separate transaction: apply the concurrent operation and commit it.
            try (FDBRecordContext opContext = fdb.openContext(null, new FDBStoreTimer())) {
                FDBRecordStore opStore = setup.builder().copyBuilder().setContext(opContext).open();
                op.apply(opStore);
                commit(opContext);
            }

            // Deleter deletes & commits.
            deleteStoreMode.deleteStore(deleterContext, setup.subspace());
            deleterCommitted = tryCommitOrDetectConflict(deleterContext);
        }

        final boolean expectDeleterConflicts =
                deleteStoreMode == DeleteStoreMode.ASYNC && op.writesStoreHeader();
        if (expectDeleterConflicts) {
            if (deleterCommitted) {
                fail("expected deleteStoreAsync to conflict with a concurrent " + op
                        + " that wrote STORE_INFO_KEY "
                        + ", but it committed. The read-conflict range added by the header "
                        + "read did not fire.");
            }
            // Op's committed state should still be on disk.
            try (FDBRecordContext peek = fdb.openContext(null, new FDBStoreTimer())) {
                assertFalse(peek.ensureActive().getRange(setup.subspace().range()).asList().join().isEmpty(),
                        "op's committed state should still be on disk after the deleter conflicted");
            }
        } else {
            assertTrue(deleterCommitted,
                    "expected the deleter to commit for op=" + op + " mode=" + deleteStoreMode
                            + " startCacheable=" + startCacheable);
            // Deleter's clear(subspace.range()) supersedes whatever the op wrote.
            try (FDBRecordContext peek = fdb.openContext(null, new FDBStoreTimer())) {
                assertTrue(peek.ensureActive().getRange(setup.subspace().range()).asList().join().isEmpty(),
                        "deleter committed, so the subspace should have been cleared");
            }
        }
    }

    @Nonnull
    public static Stream<Arguments> concurrentOperationConflictsCases() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(ConcurrentOperation.values()),
                Stream.of(DeleteStoreMode.values()),
                ParameterizedTestUtils.booleans("startCacheable"),
                // for a store that is not cacheable, pre-warming the cache should be a no-op
                ParameterizedTestUtils.booleans("cachePreWarmed")
        );
    }

    /**
     * The delete commits BEFORE the concurrent operation. In all combinations, the operation's own {@code open()} adds
     * {@code STORE_INFO_KEY} to its read set. That read-conflict range catches the prior
     * delete's write via {@code clear(subspace.range())}, so the operation always aborts
     * and the delete always sticks.
     */
    @ParameterizedTest
    @MethodSource("concurrentOperationConflictsCases")
    void concurrentOperationConflictsWithDeleteStore(@Nonnull ConcurrentOperation op,
                                                     @Nonnull DeleteStoreMode deleteStoreMode,
                                                     boolean startCacheable,
                                                     boolean cachePreWarmed) throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));
        ensureMetaDataVersionStampInitialized();

        final StoreSetup setup = createStore(startCacheable);

        if (cachePreWarmed) {
            // Open the store once in a separate transaction so the state cache has an entry
            // for this subspace before opContext.open() runs. For a cacheable store this
            // makes the subsequent open() take the handleCachedState path; for a
            // non-cacheable store the cache doesn't populate and this is effectively a no-op.
            try (FDBRecordContext warmup = fdb.openContext(null, new FDBStoreTimer())) {
                setup.builder().copyBuilder().setContext(warmup).open();
            }
        }

        final boolean opCommitted;
        try (FDBRecordContext opContext = fdb.openContext(null, new FDBStoreTimer())) {
            // Open the store first — this adds STORE_INFO_KEY to opContext's read set at
            // opContext's pinned read version, either via a SERIALIZABLE header load
            // (cache miss) or via handleCachedState (cache hit).
            FDBRecordStore opStore = setup.builder().copyBuilder().setContext(opContext).open();

            // Delete and commit in a nested transaction, writing STORE_INFO_KEY (via clear).
            try (FDBRecordContext deleterContext = fdb.openContext(null, new FDBStoreTimer())) {
                deleteStoreMode.deleteStore(deleterContext, setup.subspace());
                commit(deleterContext);
            }

            // Apply the concurrent operation on the already-open opStore and try to commit.
            // The read-conflict range on STORE_INFO_KEY from the initial open() overlaps
            // the deleter's committed write, so opContext must abort.
            op.apply(opStore);
            opCommitted = tryCommitOrDetectConflict(opContext);
        }

        assertFalse(opCommitted, "op should have conflicted with the preceding deleteStore");

        // Delete correctly landed. Subspace should be empty; a subsequent create() succeeds.
        try (FDBRecordContext contextUsingCache = fdb.openContext(null, new FDBStoreTimer())) {
            assertTrue(contextUsingCache.ensureActive()
                            .getRange(setup.subspace().range()).asList().join().isEmpty(),
                    "The store should have been deleted");
            recordStore = setup.builder().copyBuilder().setContext(contextUsingCache).create();
            assertNotCacheable();
        }
    }


    @Nonnull
    public static Stream<Arguments> testContextAndDeleteStoreModeSource() {
        return testContextSource().flatMap(testContext ->
                Stream.of(DeleteStoreMode.values()).map(mode -> Arguments.of(testContext, mode)));
    }


    @Nullable
    private byte[] getMetaDataVersionStamp() {
        try (FDBRecordContext context = fdb.openContext()) {
            return context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
        }
    }

    private void deleteStore(final Subspace subspace, @Nonnull DeleteStoreMode deleteStoreMode) {
        try (FDBRecordContext context = openContext()) {
            deleteStoreMode.deleteStore(context, subspace);
            commit(context);
        }
    }

    private void assertNotCacheable() {
        assertFalse(isStoreCachable(), "Store state should not be cacheable");
    }

    private boolean isStoreCachable() {
        return recordStore.getRecordStoreState().getStoreHeader().getCacheable();
    }

    /**
     * Bootstrap that guarantees the cluster-wide meta-data version stamp key exists, so callers
     * can compare before/after snapshots without special-casing null.
     */
    private void ensureMetaDataVersionStampInitialized() {
        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }
    }

    private static void assertCacheHit(final FDBStoreTimer timer) {
        FDBRecordStoreStateCacheTestUtils.assertCacheHit(timer, 1);
    }

    private static void assertCacheMiss(final FDBStoreTimer timer) {
        FDBRecordStoreStateCacheTestUtils.assertCacheMiss(timer, 1);
    }

    /**
     * Small tuple returned by {@link #createStore(boolean)} for tests that need both a
     * pre-configured {@link FDBRecordStore.Builder builder} (to rebind to fresh contexts) and
     * the {@link Subspace subspace} the store lives in. Both are captured while the setup
     * transaction is still open, so callers don't have to open another context just to
     * resolve the subspace.
     */
    private record StoreSetup(@Nonnull FDBRecordStore.Builder builder, @Nonnull Subspace subspace) {
    }

    /**
     * Create the standard simple record store, optionally flipped to cacheable, and return
     * a re-usable builder plus the store's subspace. Used by the {@code deleteStore} tests
     * to share the "open, configure cacheability, commit" boilerplate.
     */
    private StoreSetup createStore(boolean cacheable) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            if (cacheable) {
                assertTrue(recordStore.setStateCacheability(true),
                        "flipping to cacheable should have changed the header");
            } else {
                assertNotCacheable();
            }
            final FDBRecordStore.Builder builder = recordStore.asBuilder();
            final Subspace subspace = recordStore.getSubspace();
            commit(context);
            return new StoreSetup(builder, subspace);
        }
    }

    /**
     * Same as {@link #createStore(boolean) createStore(false)} but additionally saves one
     * {@link TestRecords1Proto.MySimpleRecord MySimpleRecord} per given {@code recNo} in a
     * follow-up transaction, so tests get a store with some concrete on-disk data to
     * exercise deletion against without having to hand-roll the save-then-commit boilerplate.
     */
    private StoreSetup createStoreWithRecords(long... recNos) throws Exception {
        final StoreSetup setup = createStore(false);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = setup.builder().copyBuilder().setContext(context).open();
            for (long recNo : recNos) {
                store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setStrValueIndexed("rec-" + recNo)
                        .build());
            }
            commit(context);
        }
        return setup;
    }

    private boolean isSubspaceEmpty(@Nonnull Subspace subspace) {
        try (FDBRecordContext context = fdb.openContext()) {
            return context.ensureActive().getRange(subspace.range()).asList().join().isEmpty();
        }
    }

    /**
     * Attempt to commit the given context. Returns {@code true} on success, {@code false} if
     * the commit failed with a conflict (any nested cause). Any other exception is re-raised
     * so an unexpected failure mode doesn't silently masquerade as "conflict".
     */
    private boolean tryCommitOrDetectConflict(@Nonnull FDBRecordContext context) {
        try {
            commit(context);
            return true;
        } catch (Exception ex) {
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause instanceof FDBExceptions.FDBStoreTransactionConflictException) {
                    return false;
                }
            }
            throw new AssertionError("unexpected exception from commit: " + ex, ex);
        }
    }

    /**
     * An operation applied on an open {@link FDBRecordStore} concurrently with a
     * {@link FDBRecordStore#deleteStore}/{@link FDBRecordStore#deleteStoreAsync}. The interesting
     * axis is whether the operation writes {@code STORE_INFO_KEY} — that is what determines
     * whether {@link DeleteStoreMode#ASYNC deleteStoreAsync}'s SERIALIZABLE header read
     * catches a concurrent commit and forces the deleter to conflict.
     */
    private enum ConcurrentOperation {
        /**
         * Flips the store's cacheability flag (toggles the current value). Writes
         * {@code STORE_INFO_KEY} via {@code updateStoreHeaderAsync} regardless of direction.
         */
        SET_CACHEABILITY(true) {
            @Override
            void apply(@Nonnull FDBRecordStore store) {
                final boolean currentlyCacheable = store.getRecordStoreState().getStoreHeader().getCacheable();
                assertTrue(store.setStateCacheability(!currentlyCacheable),
                        "flipping cacheability should have changed the header");
            }
        },
        /**
         * Sets a header user field. Writes {@code STORE_INFO_KEY} via
         * {@code updateStoreHeaderAsync}, but does not touch the cacheable flag — a
         * representative "other part of the header" mutation.
         */
        SET_HEADER_USER_FIELD(true) {
            @Override
            void apply(@Nonnull FDBRecordStore store) {
                store.setHeaderUserField("concurrent-op", new byte[]{1, 2, 3});
            }
        },
        /**
         * Saves a record. Writes only inside the records subspace — {@code STORE_INFO_KEY}
         * is untouched, so a concurrent {@code deleteStoreAsync} does NOT observe any
         * write in its read set.
         */
        SAVE_RECORD(false) {
            @Override
            void apply(@Nonnull FDBRecordStore store) {
                store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1L)
                        .setStrValueIndexed("racy")
                        .build());
            }
        };

        private final boolean writesStoreHeader;

        ConcurrentOperation(boolean writesStoreHeader) {
            this.writesStoreHeader = writesStoreHeader;
        }

        /**
         * Whether or not this operation writes to the the store header.
         * @return {@code true} iff the operation writes {@code STORE_INFO_KEY} on commit.
         *   The write-set of the concurrent operation is what {@code deleteStoreAsync}'s
         *   header read conflicts with; operations that never touch {@code STORE_INFO_KEY}
         *   cannot cause the deleter to abort.
         */
        boolean writesStoreHeader() {
            return writesStoreHeader;
        }

        abstract void apply(@Nonnull FDBRecordStore store);
    }

}
