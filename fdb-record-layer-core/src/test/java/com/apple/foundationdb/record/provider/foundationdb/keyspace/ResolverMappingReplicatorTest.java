/*
 * ResolverMappingReplicatorTest.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.MetadataHook;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link ResolverMappingReplicator}.
 */
@Tag(Tags.RequiresFDB)
public abstract class ResolverMappingReplicatorTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    protected LocatableResolver primary;
    protected LocatableResolver replica;
    protected FDBDatabase database;
    protected boolean seedWithMetadata = false;
    private Random random = new Random();
    protected KeySpacePath basePath;

    @BeforeEach
    public void setupBase() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(100);
        database = dbExtension.getDatabase();
        database.clearCaches();
        basePath = pathManager.createPath(TestKeySpace.RESOLVER_MAPPING_REPLICATOR);
    }

    @Test
    public void testDirectoryLayerCopy() {
        Map<String, ResolverResult> dirLayerMappings;
        try (FDBRecordContext context = database.openContext()) {
            dirLayerMappings = seedDirectoryLayer(context, primary, 10);
        }
        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary);

        replicator.copyTo(replica);

        assertContainsMappings(replica, dirLayerMappings);
    }

    @Tag(Tags.Slow)
    @Test
    public void testCopyWithMultipleTransactions() {
        Map<String, ResolverResult> dirLayerMappings;
        try (FDBRecordContext context = database.openContext()) {
            dirLayerMappings = seedDirectoryLayer(context, primary, 100);
        }
        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary, 3);

        replicator.copyTo(replica);

        assertContainsMappings(replica, dirLayerMappings);
    }

    @Test
    public void testCopiedDirectoryStartsAllocationBeyondOriginal() {
        Map<String, ResolverResult> dirLayerMappings;
        try (FDBRecordContext context = database.openContext()) {
            dirLayerMappings = seedDirectoryLayer(context, primary, 20);
        }

        final Long maxMappedValue = dirLayerMappings.entrySet().stream()
                .max(ResolverMappingReplicatorTest::resolverResultComparator)
                .map(max -> max.getValue().getValue())
                .orElseThrow(IllegalStateException::new);

        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary);
        replicator.copyTo(replica);

        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 20; i++) {
                String key = "new-value-" + random.nextLong();
                Long mapped = replica.resolve(context.getTimer(), key).join();
                assertThat("new mapped values are all greater than max from original directory layer",
                        mapped, is(greaterThan(maxMappedValue)));
            }
        }
    }

    @Tag(Tags.Slow)
    @Test
    public void testPickupFromIncompleteCopy() {
        Map<String, ResolverResult> dirLayerMappings;
        try (FDBRecordContext context = database.openContext()) {
            // seed with some initial values
            dirLayerMappings = seedDirectoryLayer(context, primary, 34);
        }

        ResolverMappingReplicator incompleteReplicator = new ResolverMappingReplicator(primary, 10);
        incompleteReplicator.copyTo(replica);

        try (FDBRecordContext context = database.openContext()) {
            List<CompletableFuture<Boolean>> existsFutures = new ArrayList<>();
            for (Map.Entry<String, ResolverResult> entry : dirLayerMappings.entrySet()) {
                existsFutures.add(replica.read(context, entry.getKey()).thenApply(Optional::isPresent));
            }
            List<Boolean> results = AsyncUtil.getAll(existsFutures).join();
            long presentValues = results.stream().filter(a -> a).count();
            assertThat(presentValues, is(34L));
        }

        Map<String, ResolverResult> newMappings;
        try (FDBRecordContext context = database.openContext()) {
            // add some more entries to the primary
            newMappings = seedDirectoryLayer(context, primary, 66);
        }
        Map<String, ResolverResult> totalMappings = ImmutableMap.<String, ResolverResult>builder()
                .putAll(dirLayerMappings)
                .putAll(newMappings)
                .build();

        ResolverMappingReplicator completeReplicator = new ResolverMappingReplicator(primary, 10);
        completeReplicator.copyTo(replica);
        assertContainsMappings(replica, totalMappings);
    }

    @Test
    public void testCopyEmptyIsValidDirectoryLayer() {
        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary);

        replicator.copyTo(replica);


        Map<String, ResolverResult> dirLayerMappings = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 5; i++) {
                String key = "entry-" + i;
                ResolverResult value = replica.resolveWithMetadata(context.getTimer(), key, ResolverCreateHooks.getDefault()).join();
                dirLayerMappings.put(key, value);
            }
            context.commit();
        }

        assertContainsMappings(replica, dirLayerMappings);
    }

    @Tag(Tags.Slow)
    @Test
    public void testCopyWithConcurrentAccess() throws Exception {
        Map<String, ResolverResult> dirLayerMappings;
        try (FDBRecordContext context = database.openContext()) {
            dirLayerMappings = seedDirectoryLayer(context, primary, 50);
        }

        List<CompletableFuture<Void>> work = new ArrayList<>();
        // start copy
        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary, 10);
        work.add(replicator.copyToAsync(replica));
        Map<String, ResolverResult> concurrentlyAddedMappings = new ConcurrentHashMap<>();
        for (int i = 0; i < 50; i++) {
            final String key = randomLetters(25);
            work.add(CompletableFuture.supplyAsync(() -> database.openContext())
                    .thenCompose(context -> primary.resolveWithMetadata(context.getTimer(), key, ResolverCreateHooks.getDefault())
                            .thenCompose(resolved -> {
                                context.addAfterCommit(() -> concurrentlyAddedMappings.put(key, resolved));
                                return context.commitAsync();
                            })
                    )
            );
        }
        CompletableFuture.allOf(work.toArray(new CompletableFuture<?>[0])).join();

        assertContainsMappings(replica, dirLayerMappings);
        assertContainsMappings(primary, concurrentlyAddedMappings);
    }

    @Tag(Tags.Slow)
    @Test
    public void testDigestsMatch() {
        try (FDBRecordContext context = database.openContext()) {
            seedDirectoryLayer(context, primary, 50);
        }

        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary, 10);
        replicator.copyTo(replica);

        ResolverMappingDigest primaryDigest = new ResolverMappingDigest(primary);
        ResolverMappingDigest replicaDigest = new ResolverMappingDigest(replica);
        assertThat("digests match", primaryDigest.computeDigest().join(), is(replicaDigest.computeDigest().join()));
    }

    @Test
    public void testCopyInSameDatabase() {
        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary, 10);
        FDBDatabase differentDB = new FDBDatabase(dbExtension.getDatabaseFactory(), database.getClusterFile());

        KeySpacePath path = basePath.add("to").add("replica");
        try (FDBRecordContext context = differentDB.openContext()) {
            LocatableResolver resolverInDifferentDB = new ScopedInterningLayer(differentDB, path.toResolvedPath(context));
            replicator.copyTo(resolverInDifferentDB);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertThat(ex, hasMessageContaining("copy must be within same database"));
        }
    }

    protected void assertContainsMappings(LocatableResolver scope, Map<String, ResolverResult> mappings) {
        try (FDBRecordContext context = database.openContext()) {
            for (Map.Entry<String, ResolverResult> entry : mappings.entrySet()) {
                ResolverResult resolved = scope.resolveWithMetadata(context.getTimer(), entry.getKey(), ResolverCreateHooks.getDefault()).join();
                String keyFromReverseDirectoryCache = scope.reverseLookup((FDBStoreTimer)null, entry.getValue().getValue()).join();
                assertThat("mapping is in the directory layer", entry.getValue(), is(resolved));
                assertThat("reverse mapping is in the reverse directory cache", entry.getKey(), is(keyFromReverseDirectoryCache));
            }
        }
    }


    protected Map<String, ResolverResult> seedDirectoryLayer(FDBRecordContext context, LocatableResolver scope, int entries) {
        Map<String, ResolverResult> mappings = new HashMap<>();
        for (int i = 0; i < entries; i++) {
            String key = "entry-" + random.nextLong();
            ResolverResult result;
            if (seedWithMetadata) {
                // include metadata in ~1/2 the entries
                MetadataHook hook = random.nextBoolean() ? name -> Tuple.from("metadata-for-key-" + name, random.nextLong()).pack() : ignore -> null;
                result = scope.resolveWithMetadata(context.getTimer(), key, new ResolverCreateHooks(ResolverCreateHooks.DEFAULT_CHECK, hook)).join();
            } else {
                result = scope.resolveWithMetadata(context.getTimer(), key, ResolverCreateHooks.getDefault()).join();
            }
            mappings.put(key, result);
        }
        context.commit();
        return mappings;
    }

    private String randomLetters(final int length) {
        return random.ints((int)'a', (int)'z' + 1)
                .limit(length)
                .mapToObj(a -> String.valueOf((char)a))
                .reduce((a, b) -> a + b)
                .orElseThrow(IllegalStateException::new);
    }

    private static int resolverResultComparator(Map.Entry<String, ResolverResult> a, Map.Entry<String, ResolverResult> b) {
        if (a.getValue().getValue() < b.getValue().getValue()) {
            return -1;
        } else if (a.getValue().getValue() > b.getValue().getValue()) {
            return 1;
        }
        return 0;
    }

}
