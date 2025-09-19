/*
 * FDBReverseDirectoryCacheTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedDirectoryLayer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBReverseDirectoryCache}.
 */
@Tag(Tags.RequiresFDB)
public class FDBReverseDirectoryCacheTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private static final Logger logger = LoggerFactory.getLogger(FDBReverseDirectoryCacheTest.class);

    private FDBDatabase fdb;
    private FDBStoreTimer timer = new FDBStoreTimer();
    private Random random;
    private ScopedDirectoryLayer globalScope;

    @BeforeEach
    public void getFDB() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(100);
        long seed = System.currentTimeMillis();
        System.out.println("Seed " + seed);
        random = new Random(seed);
        factory.clear();
        fdb = dbExtension.getDatabase();
        globalScope = ScopedDirectoryLayer.global(fdb);
        fdb.clearReverseDirectoryCache();
    }

    private FDBRecordContext openContext() {
        return fdb.openContext(null, timer);
    }

    private void commit(FDBRecordContext context) {
        try {
            context.commit();
            if (logger.isInfoEnabled()) {
                KeyValueLogMessage msg = KeyValueLogMessage.build("committing transaction");
                msg.addKeysAndValues(timer.getKeysAndValues());
                logger.info(msg.toString());
            }
        } finally {
            timer.reset();
        }
    }

    @Test
    public void testReverseDirectoryCacheMiss() {
        FDBReverseDirectoryCache reverseDirectoryCache = fdb.getReverseDirectoryCache();
        assertFalse(reverseDirectoryCache.get(createRandomDirectoryScope().wrap(1L)).join().isPresent(),
                "reverse lookup miss should return empty optional");
    }


    @Test
    public void testResolveDoesPut() throws Exception {
        FDBReverseDirectoryCache reverseDirectoryCache = fdb.getReverseDirectoryCache();

        final Map<String, Long> forwardMapping = new HashMap<>();
        try (FDBRecordContext context = openContext()) {
            final Random random = new Random();
            for (int i = 0; i < 5; i++) {
                final String directoryKey = "dir_" + Math.abs(random.nextLong());
                forwardMapping.put(directoryKey, globalScope.resolve(context.getTimer(), directoryKey).get());
            }
        }

        reverseDirectoryCache.clearStats();
        BiMap<String, Long> mapping = ImmutableBiMap.copyOf(forwardMapping);
        for (Map.Entry<Long, String> reverseDirectoryEntry : mapping.inverse().entrySet()) {
            String reverseDirectoryValue = reverseDirectoryCache.get(globalScope.wrap(reverseDirectoryEntry.getKey())).join()
                    .orElseThrow(() -> new AssertionError("RD cache get should not be empty"));
            assertEquals(reverseDirectoryValue, reverseDirectoryEntry.getValue());
        }

        final String stats = " (persistent hit=" + reverseDirectoryCache.getPersistentCacheHitCount()
                + ", persistent miss=" + reverseDirectoryCache.getPersistentCacheMissCount() + ")";
        assertEquals(mapping.size(), reverseDirectoryCache.getPersistentCacheHitCount(), "persistent hit count " + stats);
        assertEquals(0L, reverseDirectoryCache.getPersistentCacheMissCount(), "persistent miss count " + stats);
    }

    @Test
    public void testPutSuccess() throws Exception {
        FDBReverseDirectoryCache reverseDirectoryCache = fdb.getReverseDirectoryCache();

        final Random random = new Random();
        final String id = "dir_" + Math.abs(random.nextLong());
        final Long value;
        try (FDBRecordContext context = openContext()) {
            value = globalScope.resolve(context.getTimer(), id).get();
            reverseDirectoryCache.put(context, globalScope.wrap(id)).get();
            commit(context);
        }

        reverseDirectoryCache.clearStats();
        fdb.clearForwardDirectoryCache();
        assertEquals(0, reverseDirectoryCache.getPersistentCacheHitCount());
        assertEquals(0, reverseDirectoryCache.getPersistentCacheMissCount());

        assertEquals(id, reverseDirectoryCache.get(globalScope.wrap(value)).join()
                .orElseThrow(() -> new AssertionError("should not be empty")));
        assertEquals(1, reverseDirectoryCache.getPersistentCacheHitCount());
        assertEquals(0, reverseDirectoryCache.getPersistentCacheMissCount());
    }

    @Test
    public void testPutFail() {
        FDBReverseDirectoryCache reverseDirectoryCache = fdb.getReverseDirectoryCache();

        ExecutionException err = assertThrows(ExecutionException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                reverseDirectoryCache.put(context, createRandomDirectoryScope().wrap("dir_does_not_exist")).get();
                context.commit();
            }
        }, "put should not add directories that do not exist");
        assertThat(err.getCause(), instanceOf(NoSuchElementException.class));
    }

    @Test
    public void testPutIfNotExists() throws Exception {
        final Random random = new Random();
        final String name = "dir_" + Math.abs(random.nextInt());
        final Long id;
        ScopedValue<String> scopedName = globalScope.wrap(name);

        FDBReverseDirectoryCache rdc = fdb.getReverseDirectoryCache();

        try (FDBRecordContext context = openContext()) {
            Transaction tr = context.ensureActive();
            id = Tuple.fromBytes(DirectoryLayer.getDefault().createOrOpen(tr, PathUtil.from(name)).get().pack()).getLong(0);
            commit(context);
        }

        // Initial store should be considered a hard miss
        try (FDBRecordContext context = openContext()) {
            rdc.putIfNotExists(context, scopedName, id).get();
            // A few more calls to make sure that we only put it in once
            rdc.putIfNotExists(context, scopedName, id).get();
            rdc.putIfNotExists(context, scopedName, id).get();
            rdc.putIfNotExists(context, scopedName, id).get();

            assertEquals(3L, rdc.getPersistentCacheHitCount());
            assertEquals(1L, rdc.getPersistentCacheMissCount());
            assertEquals(3L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT));
            assertEquals(1L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT));

            commit(context);
        }

        // Second attempt should miss the in-memory cache but hit the persistent cache
        rdc.clearStats();
        try (FDBRecordContext context = openContext()) {
            rdc.putIfNotExists(context, scopedName, id).get();

            assertEquals(1L, rdc.getPersistentCacheHitCount());
            assertEquals(0L, rdc.getPersistentCacheMissCount());
            assertEquals(1L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT));
            assertEquals(0L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT));

            commit(context);
        }
    }

    @Test
    public void testGetInReverseCacheSubspace() {
        final Random random = new Random();
        final String name = "dir_" + Math.abs(random.nextInt());
        final Long id;
        try (FDBRecordContext context = openContext()) {
            Transaction tr = context.ensureActive();
            // need to use the FDB DirectoryLayer to bypass LocatableResolver which populates the reverse directory cache automatically
            id = Tuple.fromBytes(DirectoryLayer.getDefault().createOrOpen(tr, Collections.singletonList(name)).join().getKey()).getLong(0);
            commit(context);
        }

        ScopedValue<Long> scopedId = globalScope.wrap(id);
        FDBStoreTimer timer = new FDBStoreTimer();
        assertThat("the lookup does not return a value",
                fdb.getReverseDirectoryCache().getInReverseDirectoryCacheSubspace(timer, scopedId).join(),
                is(Optional.empty()));
        assertEquals(fdb.getReverseDirectoryCache().getPersistentCacheMissCount(), 1);
        assertEquals(fdb.getReverseDirectoryCache().getPersistentCacheHitCount(), 0);
        assertThat("it does not scan the directory layer",
                timer.getCount(FDBStoreTimer.DetailEvents.RD_CACHE_DIRECTORY_SCAN), is(0));

        // assert that the last lookup did not populate the cache
        assertThat("the lookup still does not return a value",
                fdb.getReverseDirectoryCache().getInReverseDirectoryCacheSubspace(timer, scopedId).join(),
                is(Optional.empty()));
        assertEquals(fdb.getReverseDirectoryCache().getPersistentCacheMissCount(), 2);
        assertEquals(fdb.getReverseDirectoryCache().getPersistentCacheHitCount(), 0);
        assertThat("it does not scan the directory layer",
                timer.getCount(FDBStoreTimer.DetailEvents.RD_CACHE_DIRECTORY_SCAN), is(0));
    }

    @Test
    public void testPutIfNotExistsWrongValue() throws Exception {
        final Random random = new Random();
        final String name = "dir_" + Math.abs(random.nextInt());
        final Long id;
        ScopedValue<String> scopedName = globalScope.wrap(name);

        try (FDBRecordContext context = openContext()) {
            Transaction tr = context.ensureActive();
            // need to use the FDB DirectoryLayer to bypass LocatableResolver which populates the reverse directory cache automatically
            id = Tuple.fromBytes(DirectoryLayer.getDefault().createOrOpen(tr, PathUtil.from(name)).get().pack()).getLong(0);
            commit(context);
        }

        FDBReverseDirectoryCache rdc = fdb.getReverseDirectoryCache();
        // Store the correct value
        try (FDBRecordContext context = openContext()) {
            rdc.putIfNotExists(context, scopedName, id).get();

            // The put should be considered a hard miss because it had to write to the cache
            assertEquals(0L, rdc.getPersistentCacheHitCount());
            assertEquals(1L, rdc.getPersistentCacheMissCount());
            assertEquals(0L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT));
            assertEquals(1L, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT));

            commit(context);
        }

        // Try again with a different value
        ExecutionException err = assertThrows(ExecutionException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                rdc.putIfNotExists(context, globalScope.wrap(name + "_x"), id).get();
                commit(context);
            }
        }, "Should have thrown an exception due to wrong value");
        assertThat(err.getCause(), instanceOf(RecordCoreException.class));
    }

    @Test
    public void testPutIfNotExistsAvoidsConflict() throws Exception {

        final Random random = new Random();
        final String name = "dir_" + Math.abs(random.nextInt());
        final Long id;
        ScopedValue<String> scopedName = globalScope.wrap(name);

        FDBReverseDirectoryCache rdc = fdb.getReverseDirectoryCache();

        // Add entry in the directory layer in a separate transaction. This will fail if two transactions
        // try to create the same entry
        try (FDBRecordContext context = openContext()) {
            id = globalScope.resolve(context.getTimer(), name).get();
        }

        // Now, have two threads try to add the entry, this should succeed
        try (FDBRecordContext context1 = openContext()) {
            rdc.putIfNotExists(context1, scopedName, id).get();
            try (FDBRecordContext context2 = openContext()) {
                rdc.putIfNotExists(context2, scopedName, id).get();
                commit(context2);
            }
            commit(context1);
        }

        try (FDBRecordContext context1 = openContext()) {
            rdc.putIfNotExists(context1, scopedName, id).get();
            try (FDBRecordContext context2 = openContext()) {
                rdc.putIfNotExists(context2, scopedName, id).get();
                commit(context2);
            }
            commit(context1);
        }
    }

    @Test
    public void testGetAvoidsConflict() throws Exception {
        final FDBReverseDirectoryCache rdc = fdb.getReverseDirectoryCache();
        final int iterations = 20;
        final int parallelism = 2;
        final Executor executor = new ForkJoinPool(parallelism + 1);
        final Semaphore lock = new Semaphore(parallelism);

        for (int i = 0; i < iterations; i++) {
            final String name = "dir_" + Math.abs(new Random().nextInt());
            final Long id;
            try (FDBRecordContext context = openContext()) {
                id = globalScope.resolve(context.getTimer(), name).get();
            }

            final List<CompletableFuture<Void>> futures = IntStream.range(0, parallelism).mapToObj(k ->
                    CompletableFuture.runAsync(() -> {
                        try {
                            lock.acquire();
                            rdc.get(globalScope.wrap(id)).get();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    }, executor)).collect(Collectors.toList());
            lock.release(parallelism);

            AsyncUtil.whenAll(futures).get();
        }
    }

    @Test
    @Tag(Tags.WipesFDB)
    public void testMapPathKeysConflict() throws Exception {
        final int parallelism = 20;
        // use one FDB instance
        testParallelReverseDirectoryCache(parallelism, true, () -> fdb);
    }

    @Test
    @Tag(Tags.WipesFDB)
    public void testMapPathKeysConflictMultipleDatabaseObjects() throws Exception {
        // this is to simulate multiple VMs
        final int parallelism = 20;
        final String clusterFile = FDBTestEnvironment.randomClusterFile();
        testParallelReverseDirectoryCache(parallelism, true,
                () -> new FDBDatabase(dbExtension.getDatabaseFactory(), clusterFile));
    }

    @Test
    @Tag(Tags.WipesFDB)
    public void testCacheInitConflict() throws Exception {
        final int parallelism = 20;
        // use one FDB instance
        testParallelReverseDirectoryCache(parallelism, false, () -> fdb);
    }

    @Test
    @Tag(Tags.WipesFDB)
    public void testCacheInitConflictMultipleDatabaseObjects() throws Exception {
        // this is to simulate multiple VMs
        final int parallelism = 20;
        final String clusterFile = FDBTestEnvironment.randomClusterFile();
        testParallelReverseDirectoryCache(parallelism, false,
                () -> new FDBDatabase(dbExtension.getDatabaseFactory(), clusterFile));
    }

    private void testParallelReverseDirectoryCache(int parallelism, boolean preInitReverseDirectoryCache,
                                                   Supplier<FDBDatabase> getFdb) throws Exception {
        final String constantName = "fla_" + Math.abs(new Random().nextLong());
        runParallelCodeOnEmptyDB(parallelism,
                () -> {
                    if (preInitReverseDirectoryCache) {
                        try (final FDBRecordContext context = fdb.openContext(null, timer)) {
                            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE,
                                    globalScope.resolve(context.getTimer(), "something totally different"));
                        }
                    }
                },
                lock -> {
                    final String name = "chi_" + Math.abs(new Random().nextLong());
                    FDBDatabase fdb = getFdb.get();
                    try (final FDBRecordContext context = fdb.openContext(null, timer)) {
                        lock.acquire();
                        context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE,
                                CompletableFuture.allOf(
                                    globalScope.resolve(context.getTimer(), constantName),
                                    globalScope.resolve(context.getTimer(), name)
                                ));
                    } finally {
                        lock.release();
                    }
                });
    }

    private void runParallelCodeOnEmptyDB(int parallelism,
                                          TestHelpers.DangerousRunnable setup,
                                          TestHelpers.DangerousConsumer<Semaphore> parallelCode) throws Exception {
        // Wipe FDB!!!!!
        try (FDBRecordContext ctx = fdb.openContext()) {
            Transaction tr = ctx.ensureActive();
            tr.clear(new byte[]{(byte) 0x00}, new byte[]{(byte) 0xff});
            commit(ctx);
        }


        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();

        // Force the creation of a new FDB instance
        factory.clear();

        // we are running a bunch of code in a supplyAsync method, so the blocking detection will catch that and complain
        // we could rework the code to either not use CompletableFuture.supplyAsync, or remove the asyncToSync in the
        // parallelCode, but that's a bit more work.
        final Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier = factory.getBlockingInAsyncDetectionSupplier();
        try {
            factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.DISABLED);
            // Get a fresh new one
            fdb = factory.getDatabase(FDBTestEnvironment.randomClusterFile());

            final Executor executor = new ForkJoinPool(parallelism + 1);
            final Semaphore lock = new Semaphore(parallelism);

            setup.run();

            final List<CompletableFuture<Exception>> futures = IntStream.range(0, parallelism).mapToObj(k ->
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            parallelCode.accept(lock);
                            return null;
                        } catch (Exception e) {
                            return e;
                        }
                    }, executor)).collect(Collectors.toList());
            lock.release(parallelism);

            final List<Exception> exceptions = AsyncUtil.getAll(futures).get();
            exceptions.removeIf(Objects::isNull);
            exceptions.forEach(Throwable::printStackTrace);
            if (exceptions.size() > 0) {
                throw exceptions.get(0);
            }
        } finally {
            factory.setBlockingInAsyncDetection(blockingInAsyncDetectionSupplier);
        }
    }

    @Test
    public void testPutIfNotExistsNotVisibleUntilCommit() throws Exception {
        final String name = "dir_" + Math.abs(new Random().nextInt());
        final FDBReverseDirectoryCache rdc = fdb.getReverseDirectoryCache();
        final ScopedValue<String> scopedName = globalScope.wrap(name);

        // Create a new directory layer entry, put it in the cache in the same transaction
        try (FDBRecordContext context = openContext()) {
            Transaction transaction = context.ensureActive();
            DirectoryLayer directoryLayerToUse = new DirectoryLayer(context.join(globalScope.getNodeSubspace(context)), globalScope.getContentSubspace());
            final byte[] rawDirectoryEntry = directoryLayerToUse.createOrOpen(transaction, Collections.singletonList(name)).get().getKey();
            final Long id = Tuple.fromBytes(rawDirectoryEntry).getLong(0);
            rdc.putIfNotExists(context, scopedName, id).get();

            // This happens in its own transaction so should not yet see the uncommitted directory and reverse
            // directory entries that are being created.
            Optional<String> result = rdc.get(globalScope.wrap(id)).join();
            assertFalse(result.isPresent(), "Should not have gotten a result from RDC lookup");

            commit(context);
        }
    }

    @Tag(Tags.Slow)
    @Test
    public void testReverseDirectoryCacheLookup() throws Exception {
        FDBReverseDirectoryCache reverseDirectoryCache = fdb.getReverseDirectoryCache();

        // Force the reverse directory cache to use continuations as it works, to make sure
        // that piece is working ok
        reverseDirectoryCache.setMaxRowsPerTransaction(5);

        final String[] names = createRandomDirectoryKeys(fdb, 10);

        // Force the cache to be re-built with the new entries in place
        reverseDirectoryCache.rebuild(globalScope);

        // Add one more entry after the rebuild
        String afterRebuildName = createRandomDirectoryKeys(fdb, 1)[0];

        try (FDBRecordContext context = openContext()) {
            Map<Long, String> reverseMapping = new HashMap<>();
            for (int i = 0; i < names.length; i++) {
                Long id = globalScope.resolve(context.getTimer(), names[i]).join();
                reverseMapping.put(id, names[i]);
            }

            // clear stats which are set as a side-effect of resolve
            reverseDirectoryCache.clearStats();
            for (Map.Entry<Long, String> entry : reverseMapping.entrySet()) {
                Optional<String> name = reverseDirectoryCache.get(globalScope.wrap(entry.getKey())).join();
                assertTrue(name.isPresent());
                assertEquals(entry.getValue(), name.get());
            }
            assertEquals((long)names.length, reverseDirectoryCache.getPersistentCacheHitCount());
            assertEquals((long)names.length, context.getTimer().getCount(FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT));

            Long id = globalScope.resolve(context.getTimer(), afterRebuildName).get();
            reverseDirectoryCache.clearStats();

            Optional<String> name = reverseDirectoryCache.get(globalScope.wrap(id)).join();
            assertTrue(name.isPresent());
            assertEquals(afterRebuildName, name.get());
            assertEquals(0L, reverseDirectoryCache.getPersistentCacheMissCount(), "persistent cache miss count");
            assertEquals(1L, reverseDirectoryCache.getPersistentCacheHitCount(), "persistent cache hit count");
        }
    }


    @Test
    @Tag(Tags.WipesFDB)
    public void testUniqueCachePerDatabase() throws Exception {
        final Pair<String, Long>[] initialEntries = createRandomDirectoryEntries(fdb, 3);
        fdb.clearForwardDirectoryCache();
        FDBReverseDirectoryCache cache = fdb.getReverseDirectoryCache();
        cache.clearStats();

        // Populate the cache
        for (Pair<String, Long> pair : initialEntries) {
            assertEquals(Optional.of(pair.getLeft()), cache.get(globalScope.wrap(pair.getRight())).get());
        }
        assertEquals(initialEntries.length, cache.getPersistentCacheHitCount());

        // Ensure that the cache is populated
        for (Pair<String, Long> pair : initialEntries) {
            assertEquals(Optional.of(pair.getLeft()), cache.get(globalScope.wrap(pair.getRight())).get());
        }
        assertEquals(0, cache.getPersistentCacheMissCount());

        // Wipe FDB!!!!!
        try (FDBRecordContext ctx = fdb.openContext()) {
            Transaction tr = ctx.ensureActive();
            tr.clear( new byte[] { (byte) 0x00 }, new byte[] { (byte) 0xff } );
            commit(ctx);
        }

        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();

        // Force the creation of a new FDB instance
        factory.clear();

        // Get a fresh new one
        fdb = factory.getDatabase(fdb.getClusterFile());
        cache = fdb.getReverseDirectoryCache();

        // In the hopes to ensure that re-creating the entries that we previously created
        // will result in new id's, create some initial filler entries.
        final Pair<String, Long>[] fillerEntries = createRandomDirectoryEntries(fdb, 20);

        // Just make sure the filler entries are populated
        for (Pair<String, Long> pair : fillerEntries) {
            assertEquals(Optional.of(pair.getLeft()), cache.get(globalScope.wrap(pair.getRight())).get());
        }
        assertEquals(fillerEntries.length, cache.getPersistentCacheHitCount());

        // Re-create the initial entries again
        final Pair<String, Long>[] newEntries;
        try (FDBRecordContext context = fdb.openContext()) {
            List<String> keys = new ArrayList<>();
            List<CompletableFuture<Long>> futures = Arrays.stream(initialEntries)
                    .map(entry -> {
                        keys.add(entry.getLeft());
                        return entry.getLeft();
                    })
                    .map(key -> globalScope.resolve(context.getTimer(), key)).collect(Collectors.toList());
            List<Long> values = AsyncUtil.getAll(futures).get();

            newEntries = zipKeysAndValues(keys, values);
            commit(context);
        }

        boolean oldValuesMatchNewValues = true;
        for (int i = 0; i < newEntries.length; i++) {
            Pair<String, Long> initialEntry = initialEntries[i];
            Pair<String, Long> newEntry = newEntries[i];

            assertEquals(initialEntry.getLeft(), newEntry.getLeft());
            oldValuesMatchNewValues &= initialEntry.getRight().equals(newEntry.getRight());
            assertEquals(Optional.of(newEntry.getLeft()), cache.get(globalScope.wrap(newEntry.getRight())).get());
        }
        // While it's possible that some of the values are re-allocated to the same value, it is very unlikely that
        // they are *all* allocated the same values, so we assert that at least one of them differs
        assertFalse(oldValuesMatchNewValues, "At least one re-generated value should differ from original allocation");
    }

    private ScopedDirectoryLayer createRandomDirectoryScope() {
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        ResolvedKeySpacePath resolvedPath;
        try (FDBRecordContext context = fdb.openContext()) {
            resolvedPath = path.toResolvedPath(context);
        }
        return new ScopedDirectoryLayer(fdb, resolvedPath);
    }

    private String[] createRandomDirectoryKeys(FDBDatabase database, int nEntries) {
        return Arrays.stream(createRandomDirectoryEntries(database, nEntries))
                .map(Pair::getLeft)
                .collect(Collectors.toList()).toArray(new String[0]);
    }

    private Pair<String, Long>[] createRandomDirectoryEntries(FDBDatabase database, int nEntries) {
        try (FDBRecordContext context = database.openContext()) {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < nEntries; i++) {
                keys.add("dir_" + Math.abs(random.nextInt()));
            }
            List<Long> values = keys.stream()
                    .map(key -> globalScope.resolve(context.getTimer(), key).join())
                    .collect(Collectors.toList());
            return zipKeysAndValues(keys, values);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Pair<String, Long>[] zipKeysAndValues(List<String> keys, List<Long> values) {
        final Iterator<Long> valuesIter = values.iterator();
        return keys.stream()
                .map( key -> Pair.of(key, valuesIter.next()) )
                .collect(Collectors.toList())
                .toArray(new Pair[0]);
    }
}
