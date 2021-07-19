/*
 * ExtendedDirectoryLayerTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.MetadataHook;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class ExtendedDirectoryLayerTest extends LocatableResolverTest {
    private KeySpace keySpace = new KeySpace(
            new KeySpaceDirectory("path", STRING, "path")
                    .addSubdirectory(new KeySpaceDirectory("to", STRING, "to")
                            .addSubdirectory(new KeySpaceDirectory("dirLayer", STRING, "dirLayer"))
                    )
    );

    public ExtendedDirectoryLayerTest() {
        super(TestingResolverFactory.ResolverType.EXTENDED_DIRECTORY_LAYER);
    }

    @Test
    public void testWriteCompatibilityGlobal() {
        testReadCompatible(globalScope, ScopedDirectoryLayer.global(database));
    }

    @Test
    public void testReadCompatibilityGlobal() {
        testReadCompatible(ScopedDirectoryLayer.global(database), globalScope);
    }

    @Test
    public void testWriteCompatibilityScoped() {
        try (FDBRecordContext context = database.openContext()) {
            ResolvedKeySpacePath path = keySpace.path("path").add("to").add("dirLayer").toResolvedPath(context);
            LocatableResolver backwardsCompatible = resolverFactory.create(path);
            LocatableResolver scopedDirectoryLayer = new ScopedDirectoryLayer(database, path);

            testReadCompatible(backwardsCompatible, scopedDirectoryLayer);
        }
    }

    @Test
    public void testReadCompatibilityScoped() {
        try (FDBRecordContext context = database.openContext()) {
            ResolvedKeySpacePath path = keySpace.path("path").add("to").add("dirLayer").toResolvedPath(context);
            LocatableResolver backwardsCompatible = resolverFactory.create(path);
            LocatableResolver scopedDirectoryLayer = new ScopedDirectoryLayer(database, path);

            testReadCompatible(scopedDirectoryLayer, backwardsCompatible);
        }
    }

    private static void testReadCompatible(LocatableResolver writer, LocatableResolver reader) {
        Map<String, Long> allocations = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            String key = "key-" + i;
            Long value = writer.resolve(key).join();
            allocations.put(key, value);
        }

        for (Map.Entry<String, Long> entry : allocations.entrySet()) {
            Long value = reader.resolve(entry.getKey()).join();
            String reverseLookup = reader.reverseLookup(null, entry.getValue()).join();
            assertEquals(value, entry.getValue());
            assertEquals(reverseLookup, entry.getKey());
        }
    }

    @Test
    public void testScopedDirectorySeesUpdate() {
        LocatableResolver scopedDirectoryLayer = ScopedDirectoryLayer.global(database);
        LocatableResolver extendedDirectoryLayer = ExtendedDirectoryLayer.global(database);
        loopSeesUpdate(scopedDirectoryLayer, extendedDirectoryLayer);
    }

    @Test
    public void testExtendedSeesUpdate() {
        LocatableResolver scopedDirectoryLayer = ScopedDirectoryLayer.global(database);
        LocatableResolver extendedDirectoryLayer = ExtendedDirectoryLayer.global(database);
        loopSeesUpdate(extendedDirectoryLayer, scopedDirectoryLayer);
    }

    private void loopSeesUpdate(LocatableResolver loopingResolver, LocatableResolver otherResolver) {
        final String key = "a-key";
        final AtomicLong valueFromLoop = new AtomicLong(-1);
        CompletableFuture<Void> loop = AsyncUtil.whileTrue(() -> {
            FDBRecordContext context = database.openContext();
            return loopingResolver.mustResolve(context, key)
                    .handle((resolved, ex) -> {
                        context.close();
                        if (ex != null) {
                            return true;
                        }
                        valueFromLoop.set(resolved);
                        return false;
                    });
        });

        Long value = otherResolver.resolve(key).join();
        loop.join();
        assertThat("The loop eventually reads the value we allocated", value, is(valueFromLoop.get()));
    }


    @Test
    public void testCanAllocateSameKeysInParallelWithFDBDirectoryLayer() {
        final DirectoryLayer directoryLayer = DirectoryLayer.getDefault();

        testParallelAllocation(true, database,
                key -> database.runAsync(context ->
                        directoryLayer.createOrOpen(context.ensureActive(), Collections.singletonList(key))
                                .thenApply(subspace -> Tuple.fromBytes(subspace.pack()).getLong(0))),
                key -> globalScope.resolve(key),
                ExtendedDirectoryLayer.global(database), ScopedDirectoryLayer.global(database));
    }

    @Test
    public void testGlobalExtendedInParallelWithScopedDirectoryLayer() {
        LocatableResolver scopedDirectoryLayer = ScopedDirectoryLayer.global(database);
        LocatableResolver backwardsCompatibleDirectoryLayer = ExtendedDirectoryLayer.global(database);
        testParallelAllocation(true, database, backwardsCompatibleDirectoryLayer, scopedDirectoryLayer);
    }

    @Test
    public void testGlobalScopedDirectoryLayerInParallelWithExtended() {
        LocatableResolver scopedDirectoryLayer = ScopedDirectoryLayer.global(database);
        LocatableResolver backwardsCompatibleDirectoryLayer = ExtendedDirectoryLayer.global(database);
        testParallelAllocation(true, database, scopedDirectoryLayer, backwardsCompatibleDirectoryLayer);
    }

    @Test
    public void testExtendedInParallelWithScopedDirectoryLayer() {
        ResolvedKeySpacePath path;
        try (FDBRecordContext context = database.openContext()) {
            path = keySpace.path("path").add("to").add("dirLayer").toResolvedPath(context);
            ScopedDirectoryLayer scopedDirectoryLayer = new ScopedDirectoryLayer(database, path);
            ExtendedDirectoryLayer extendedDirectoryLayer = new ExtendedDirectoryLayer(database, path);
            testParallelAllocation(false, database, extendedDirectoryLayer, scopedDirectoryLayer);
        }
    }

    @Test
    public void testScopedDirectoryLayerInParallelWithExtended() {
        ResolvedKeySpacePath path;
        try (FDBRecordContext context = database.openContext()) {
            path = keySpace.path("path").add("to").add("dirLayer").toResolvedPath(context);
            ScopedDirectoryLayer scopedDirectoryLayer = new ScopedDirectoryLayer(database, path);
            ExtendedDirectoryLayer extendedDirectoryLayer = new ExtendedDirectoryLayer(database, path);
            testParallelAllocation(false, database, extendedDirectoryLayer, scopedDirectoryLayer);
        }
    }

    private static void testParallelAllocation(boolean checkDirectoryLayer,
                                               FDBDatabase database,
                                               LocatableResolver resolver1,
                                               LocatableResolver resolver2) {
        testParallelAllocation(checkDirectoryLayer,
                database,
                resolver1::resolve,
                resolver2::resolve,
                resolver1,
                resolver2);
    }

    private static void testParallelAllocation(boolean checkDirectoryLayer,
                                               FDBDatabase database,
                                               Function<String, CompletableFuture<Long>> resolveCall1,
                                               Function<String, CompletableFuture<Long>> resolveCall2,
                                               LocatableResolver resolver1,
                                               LocatableResolver resolver2) {
        Map<String, Long> mappings1 = new ConcurrentHashMap<>();
        Map<String, Long> mappings2 = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> operations = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            operations.add(resolveCall1.apply(key).thenAccept(value -> mappings1.put(key, value)));
            operations.add(resolveCall2.apply(key).thenAccept(value -> mappings2.put(key, value)));
        }
        CompletableFuture.allOf(operations.toArray(new CompletableFuture<?>[0])).join();

        for (Map.Entry<String, Long> entry : mappings1.entrySet()) {
            assertThat(String.format("the mappings for %s are identical", entry.getKey()),
                    mappings2.get(entry.getKey()), is(entry.getValue()));
            try (FDBRecordContext context = database.openContext()) {
                if (checkDirectoryLayer) {
                    // only applies when the scope is global
                    final DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
                    Long value = directoryLayer.open(context.ensureActive(), Collections.singletonList(entry.getKey()))
                            .thenApply(subspace -> Tuple.fromBytes(subspace.pack()).getLong(0)).join();
                    assertThat("the FDB directory layer sees the mapping", value, is(entry.getValue()));
                }
                assertThat(resolver1.getClass().getName() + " sees the mapping",
                        resolver1.mustResolve(context, entry.getKey()).join(), is(entry.getValue()));
                assertThat(resolver2.getClass().getName() + " sees the mapping",
                        resolver2.mustResolve(context, entry.getKey()).join(), is(entry.getValue()));

                checkMappingInReverseCache(resolver1, entry.getKey(), entry.getValue());
                checkMappingInReverseCache(resolver2, entry.getKey(), entry.getValue());
            }
        }
    }

    private static void checkMappingInReverseCache(LocatableResolver resolver, String key, Long value) {
        // clear caches and stats to force read of FDB
        resolver.getDatabase().clearCaches();
        resolver.getDatabase().getReverseDirectoryCache().clearStats();
        assertThat(resolver.getClass().getName() + " sees the reverse mapping",
                resolver.reverseLookup(null, value).join(), is(key));
        assertThat("we find the value in the reverse cache key space",
                resolver.getDatabase().getReverseDirectoryCache().getPersistentCacheHitCount(), is(1L));
        assertThat("we don't get a hard cache miss",
                resolver.getDatabase().getReverseDirectoryCache().getPersistentCacheMissCount(), is(0L));
    }

    @Test
    public void testDefaultCanAllocateIndependentKeysInParallel() {
        final ScopedDirectoryLayer directoryLayer = ScopedDirectoryLayer.global(database);

        Map<String, Long> mappingsFromOld = new ConcurrentHashMap<>();
        Map<String, Long> mappingsFromNew = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> operations = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String oldDirLayerKey = i + "-old-dl-key";
            String newDirLayerKey = i + "-new-dl-key";
            operations.add(directoryLayer.resolve(oldDirLayerKey).thenApply(value -> mappingsFromOld.put(oldDirLayerKey, value)));
            operations.add(globalScope.resolve(newDirLayerKey).thenApply(value -> mappingsFromNew.put(newDirLayerKey, value)));
        }
        CompletableFuture.allOf(operations.toArray(new CompletableFuture<?>[0])).join();

        // Implicitly checks that there are no duplicate keys or values in the two maps
        BiMap<String, Long> completeMapping = ImmutableBiMap.<String, Long>builder()
                .putAll(mappingsFromOld)
                .putAll(mappingsFromNew)
                .build();

        for (Map.Entry<String, Long> entry : completeMapping.entrySet()) {
            try (FDBRecordContext context = database.openContext()) {
                assertThat("the FDB directory layer sees the mapping",
                        directoryLayer.mustResolve(context, entry.getKey()).join(), is(entry.getValue()));
                assertThat("the ScopedDirectoryLayer sees the mapping",
                        globalScope.mustResolve(context, entry.getKey()).join(), is(entry.getValue()));
            }
        }
    }

    @Test
    public void testScopedDirectoryLayerResolvesWithoutMetadata() {
        MetadataHook hook = name -> Tuple.from("metadata-for-" + name).pack();
        ResolverCreateHooks createHooks = new ResolverCreateHooks(ResolverCreateHooks.DEFAULT_CHECK, hook);
        ResolverResult result = globalScope.resolveWithMetadata("some-key", createHooks).join();
        assertArrayEquals(Tuple.from("metadata-for-some-key").pack(), result.getMetadata(), "metadata was added");

        ResolverResult resultFromScoped = ScopedDirectoryLayer.global(database)
                .resolveWithMetadata("some-key", /* no hooks */ ResolverCreateHooks.getDefault())
                .join();
        assertEquals(resultFromScoped.getValue(), result.getValue());
        assertThat(resultFromScoped.getMetadata(), is(nullValue()));
        try (FDBRecordContext context = database.openContext()) {
            assertArrayEquals(globalScope.mustResolveWithMetadata(context, "some-key").join().getMetadata(),
                    Tuple.from("metadata-for-some-key").pack(),
                    "we can still read the metadata with " + globalScope.getClass().getName());
        }
    }

    @Test
    public void testSetMappingDoesNotScanDirectoryKeySpace() {
        FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = database.openContext()) {
            context.setTimer(timer);
            for (long i = 0; i < 10; i++) {
                globalScope.setMapping(context, "some-key-" + i, i).join();
            }
        }
        assertThat("there are no scans of the directory layer", timer.getCount(FDBStoreTimer.DetailEvents.RD_CACHE_DIRECTORY_SCAN), is(0));
    }

    // Unsupported operations

    @Test
    @Override
    public void testUpdateMetadata() {
        assertThrows(UnsupportedOperationException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.updateMetadata(context, "foo", null).join();
            }
        });
    }

}
