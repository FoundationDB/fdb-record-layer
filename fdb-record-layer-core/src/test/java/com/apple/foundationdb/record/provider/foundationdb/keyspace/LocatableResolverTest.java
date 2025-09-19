/*
 * LocatableResolverTest.java
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
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactoryImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver.LocatableResolverLockedException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.MetadataHook;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.PreWriteCheck;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static com.apple.foundationdb.record.TestHelpers.consistently;
import static com.apple.foundationdb.record.TestHelpers.eventually;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.DEFAULT_CHECK;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.DEFAULT_HOOK;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link LocatableResolver}.
 */
@Tag(Tags.WipesFDB)
@Tag(Tags.RequiresFDB)
public abstract class LocatableResolverTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @RegisterExtension
    protected final TestingResolverFactory resolverFactory;

    protected Random random;
    protected LocatableResolver globalScope;
    protected FDBDatabase database;

    protected LocatableResolverTest(TestingResolverFactory.ResolverType resolverType) {
        resolverFactory = new TestingResolverFactory(dbExtension, resolverType);
    }

    @BeforeEach
    public void setup() {
        long seed = System.currentTimeMillis();
        System.out.println("Seed " + seed);
        random = new Random(seed);
        globalScope = resolverFactory.getGlobalScope();
        database = resolverFactory.getDatabase();
        // Loading the reverse directory cache can happen in a potentially conflicting transaction, so get it out of the way
        database.getReverseDirectoryCache().waitUntilReadyForTesting();
    }

    @Test
    void testLookupCaching() {
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("path", KeyType.STRING, "path"));

        ResolvedKeySpacePath path1;
        try (FDBRecordContext context = database.openContext()) {
            path1 = keySpace.resolveFromKey(context, Tuple.from("path"));
        }
        LocatableResolver resolver = resolverFactory.create(path1);

        Long value = resolver.resolve("foo").join();

        for (int i = 0; i < 5; i++) {
            Long fetched = resolver.resolve("foo").join();
            assertThat("we should always get the original value", fetched, is(value));
        }
        CacheStats stats = resolverFactory.getDirectoryCacheStats();
        assertThat("subsequent lookups should hit the cache", stats.hitCount(), is(5L));
    }

    @Test
    void testDirectoryIsolation() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("path", KeyType.STRING, "path")
                        .addSubdirectory(new KeySpaceDirectory("to", KeyType.STRING, "to")
                                .addSubdirectory(new KeySpaceDirectory("dirLayer1", KeyType.STRING, "dirLayer1"))
                                .addSubdirectory(new KeySpaceDirectory("dirLayer2", KeyType.STRING, "dirLayer2"))
                        )
        );

        try (FDBRecordContext context = database.openContext()) {
            ResolvedKeySpacePath path1 = keySpace.resolveFromKey(context, Tuple.from("path", "to", "dirLayer1"));
            ResolvedKeySpacePath path2 = keySpace.resolveFromKey(context, Tuple.from("path", "to", "dirLayer2"));
            LocatableResolver resolver = resolverFactory.create(path1);
            LocatableResolver sameResolver = resolverFactory.create(path1);
            LocatableResolver differentResolver = resolverFactory.create(path2);

            List<String> names = ImmutableList.of("a", "set", "of", "names", "to", "resolve");
            List<Long> resolved = new ArrayList<>();
            List<Long> same = new ArrayList<>();
            List<Long> different = new ArrayList<>();
            for (String name : names) {
                resolved.add(resolver.resolve(context.getTimer(), name).join());
                same.add(sameResolver.resolve(context.getTimer(), name).join());
                different.add(differentResolver.resolve(context.getTimer(), name).join());
            }
            assertThat("same resolvers produce identical results", resolved, contains(same.toArray()));
            assertThat("different resolvers are independent", resolved, not(contains(different.toArray())));
        }
    }

    @Test
    void testScopedCaching() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("path1", KeyType.STRING, "path1"),
                new KeySpaceDirectory("path2", KeyType.STRING, "path2")
        );

        try (FDBRecordContext context = database.openContext()) {
            final ResolvedKeySpacePath path1 = keySpace.resolveFromKey(context, Tuple.from("path1"));
            final LocatableResolver resolver1 = resolverFactory.create(path1);

            Cache<ScopedValue<String>, Long> cache = CacheBuilder.newBuilder().build();
            cache.put(resolver1.wrap("stuff"), 1L);

            assertThat("values can be read from the cache by scoped string",
                    cache.getIfPresent(resolver1.wrap("stuff")), is(1L));
            assertThat("cache misses when looking for unknown name in scope",
                    cache.getIfPresent(resolver1.wrap("missing")), equalTo(null));

            final ResolvedKeySpacePath path2 = keySpace.resolveFromKey(context, Tuple.from("path2"));
            final LocatableResolver resolver2 = resolverFactory.create(path2);
            assertThat("cache misses when string is not in this scope",
                    cache.getIfPresent(resolver2.wrap("stuff")), equalTo(null));

            final LocatableResolver newResolver = resolverFactory.create(path1);
            assertThat("scoping is determined by value of scope directory, not a reference to it",
                    cache.getIfPresent(newResolver.wrap("stuff")), is(1L));
        }
    }

    @Test
    void testDirectoryCache() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(10);

        FDBStoreTimer timer = new FDBStoreTimer();

        FDBDatabase fdb = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        fdb.close(); // Make sure cache is fresh.
        String key = "world";
        Long value;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            value = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), key));
        }
        int initialReads = timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ);
        assertThat(initialReads, is(greaterThanOrEqualTo(1)));

        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                assertThat("we continue to resolve the same value",
                        context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), key)),
                        is(value));
            }
        }
        assertEquals(timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), initialReads);
    }

    @Test
    void testDirectoryCacheWithUncommittedContext() {
        FDBDatabase fdb = database;
        fdb.clearCaches();

        // In the scoped directory layer test, this can conflict with initializing the reverse directory layer
        fdb.getReverseDirectoryCache().waitUntilReadyForTesting();

        final String key = "hello " + UUID.randomUUID();

        FDBStoreTimer timer = new FDBStoreTimer();
        long resolved;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.getReadVersion(); // Ensure initial get read version is instrumented
            resolved = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertAll(
                    () -> assertThat("directory resolution should not have been from cache", timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), equalTo(1)),
                    () -> assertThat("should only have opened at most 2 child transaction", timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT), lessThanOrEqualTo(3)),
                    () -> assertThat("should only have gotten one read version", timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION), equalTo(1)),
                    () -> assertThat("should have only committed the inner transaction", timer.getCount(FDBStoreTimer.Events.COMMIT), lessThanOrEqualTo(1))
            );
            // do not commit transaction (though child transaction updating the resolved key should have been committed)
        }

        // Should read cached value
        timer.reset();
        long resolved2;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.getReadVersion(); // Ensure initial get read version is instrumented
            resolved2 = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertAll(
                    () -> assertThat( "resolved value from cache does not match initial resolution", resolved2, equalTo(resolved)),
                    () -> assertEquals(0, timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), "should not have read from the directory layer"),
                    () -> assertEquals(1, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT), "should not have opened any additional contexts"),
                    () -> assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION), "should not need any additional read versions"),
                    () -> assertEquals(0, timer.getCount(FDBStoreTimer.Events.COMMIT))
            );
        }

        // Clear the caches and see that the value in the database matches
        database.clearCaches();
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            long resolved3 = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertAll(
                    () -> assertThat( "resolved value from database does not match initial resolution", resolved3, equalTo(resolved)),
                    () -> assertThat("directory resolution should not have been from cache", timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), equalTo(1)),
                    () -> assertThat("should only have opened at most 2 child transaction", timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT), lessThanOrEqualTo(3)),
                    () -> assertThat("should only have gotten one read version", timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION), equalTo(1)),
                    () -> assertThat("should only have committed the inner transaction", timer.getCount(FDBStoreTimer.Events.COMMIT), lessThanOrEqualTo(1))
            );
        }
    }

    @Test
    void testCachesWinnerOfConflict() {
        FDBDatabase fdb = database;
        fdb.clearCaches();

        // In the scoped directory layer test, this can conflict with initializing the reverse directory layer
        fdb.getReverseDirectoryCache().waitUntilReadyForTesting();

        final String key = "hello " + UUID.randomUUID();

        long resolved;
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context1 = fdb.openContext(null, timer); FDBRecordContext context2 = fdb.openContext(null, timer)) {
            // Ensure both started
            context1.getReadVersion();
            context2.getReadVersion();

            // Both contexts try to create the key
            CompletableFuture<Long> resolvedFuture1 = globalScope.resolve(context1, key);
            CompletableFuture<Long> resolvedFuture2 = globalScope.resolve(context2, key);

            long resolved1 = context1.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, resolvedFuture1);
            long resolved2 = context2.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, resolvedFuture2);

            assertAll(
                    () -> assertThat("two concurrent resolutions of the same key should match", resolved1, equalTo(resolved2)),
                    () -> assertThat("at least one transaction should read from database", timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), greaterThanOrEqualTo(1)),
                    () -> assertThat("should not open more transactions than the two parents and five children", timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT), lessThanOrEqualTo(7)),
                    () -> assertThat("should not have committed more than the five children", timer.getCount(FDBStoreTimer.Events.COMMIT), lessThanOrEqualTo(5))
            );
            resolved = resolved1;
        }

        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.getReadVersion();
            long resolvedAgain = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertAll(
                    () -> assertThat("resolved value in cache should match initial resolution", resolvedAgain, equalTo(resolved)),
                    () -> assertThat("should have resolved from cache", timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), equalTo(0))
            );
        }
    }

    /**
     * This is mainly to test a counter factual where the same transaction is used to actually resolve the value as
     * is used by the caller. In that case, one could accidentally pollute the cache with uncommitted data. To protect
     * against that, this test is designed to fail if someone changes the resolution logic so that uncommitted data
     * (even possibly uncommitted data re-read from the same transaction that wrote it) might be put in the cache.
     */
    @Test
    void testDoesNotCacheValueReadFromReadYourWritesCache() {
        FDBDatabase fdb = dbExtension.getDatabase(database.getClusterFile());
        fdb.clearCaches();

        final String key = "hello " + UUID.randomUUID();
        final FDBStoreTimer timer = new FDBStoreTimer();
        long resolved;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            // First time: nothing in cache or DB. Entry is created.
            resolved = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), "should have read from the database");

            // Second time: if same context used to create and read, then this would read from transaction's read your writes cache, not the database
            long resolvedAgain = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            assertEquals(resolved, resolvedAgain, "resolving the same key should not change the value even in the same transaction");

            // do not commit main transaction
        }

        // Read from cache. If present, this should not have changed its value
        timer.reset();
        boolean cached;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            long resolvedFromCache = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
            cached = timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ) == 0;
            if (cached) {
                assertEquals(resolved, resolvedFromCache, "resolved value should have changed when reading from cache");
            }
        }

        // Clear caches, and re-read from the database.
        if (cached) {
            fdb.clearCaches();
            timer.reset();
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                long resolvedFromDb = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context, key));
                assertEquals(resolved, resolvedFromDb, "resolved value from database should have matched initial resolution");
            }
        }
    }

    @Test
    void testResolveUseCacheCommits() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(10);

        FDBStoreTimer timer = new FDBStoreTimer();
        String key = "hello " + UUID.randomUUID();
        FDBDatabase fdb = factory.getDatabase(FDBTestEnvironment.randomClusterFile());

        assertEquals(0, timer.getCount(FDBStoreTimer.Events.COMMIT));
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), key));
        }
        // initial resolve may commit twice, once for the key and once to initialize the reverse directory cache
        assertThat(timer.getCount(FDBStoreTimer.Events.COMMIT), is(greaterThanOrEqualTo(1)));

        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), "a-new-key"));
        }
        assertEquals(1, timer.getCount(FDBStoreTimer.Events.COMMIT));


        timer.reset();
        assertEquals(0, timer.getCount(FDBStoreTimer.Events.COMMIT));
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), key));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Events.COMMIT));
    }

    @Test
    void testResolveCommitsWhenCacheEnabled() {
        Map<String, Long> mappings = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 10; i++) {
                String key = "string-" + i;
                Long value = globalScope.resolve(context, key).join();
                mappings.put(key, value);
            }
        }

        Long baseline = database.getDirectoryCacheStats().hitCount();
        Long reverseCacheBaseline = database.getReverseDirectoryInMemoryCache().stats().hitCount();
        database.close();
        database = dbExtension.getDatabaseFactory().getDatabase(database.getClusterFile());
        try (FDBRecordContext context = database.openContext()) {
            for (Map.Entry<String, Long> entry : mappings.entrySet()) {
                Long value = globalScope.resolve(context.getTimer(), entry.getKey()).join();
                String name = globalScope.reverseLookup(context, value).join();
                assertEquals(value, entry.getValue(), "mapping is persisted even though context in arg was not committed");
                assertEquals(name, entry.getKey(), "reverse mapping is persisted even though context in arg was not committed");
            }
            assertEquals(database.getDirectoryCacheStats().hitCount() - baseline, 0L, "values are persisted, not in cache");
            assertEquals(database.getReverseDirectoryInMemoryCache().stats().hitCount() - reverseCacheBaseline, 0L, "values are persisted, not in cache");
        }
    }

    /**
     * Test that if a value is resolved and, due to read version caching, it may read stale data and therefore miss
     * the most recent update, that upon an internal retry, it gets a fresh version (rather than getting a bunch of
     * conflicts).
     */
    @Test
    void testResolveWithWeakReadSemantics() {
        final boolean tracksReadVersions = database.isTrackLastSeenVersionOnRead();
        final boolean tracksCommitVersions = database.isTrackLastSeenVersionOnCommit();
        try {
            database.setTrackLastSeenVersionOnRead(true);
            database.setTrackLastSeenVersionOnCommit(false); // disable commit version tracking so that stale read version is cached

            final String key = "hello " + UUID.randomUUID();
            long resolvedValue;
            try (FDBRecordContext context = database.openContext()) {
                resolvedValue = globalScope.resolve(context, key).join();
            }

            // Clear the cache to ensure the database must be consulted
            database.clearCaches();

            // Using a stale read version should first read from the database, see that there is
            final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                    .setWeakReadSemantics(new FDBDatabase.WeakReadSemantics(0, Long.MAX_VALUE, true))
                    .build();
            try (FDBRecordContext context = database.openContext(config)) {
                long resolvedAgainValue = globalScope.resolve(context, key).join();
                assertEquals(resolvedValue, resolvedAgainValue, "resolved value changed between transactions");
            }
        } finally {
            database.setTrackLastSeenVersionOnRead(tracksReadVersions);
            database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
        }
    }

    @Test
    void testResolveWithNoMetadata() {
        Long value;
        ResolverResult noHookResult;
        value = globalScope.resolve("resolve-string").join();
        noHookResult = globalScope.resolveWithMetadata("resolve-string", ResolverCreateHooks.getDefault()).join();
        assertThat("the value is the same", noHookResult.getValue(), is(value));
        assertThat("entry was created without metadata", noHookResult.getMetadata(), is(nullValue()));
    }

    @Test
    void testReverseLookup() {
        Long value;
        try (FDBRecordContext context = database.openContext()) {
            value = globalScope.resolve(context.getTimer(), "something").join();
            context.commit();
        }

        String lookupString = globalScope.reverseLookup((FDBStoreTimer)null, value).join();
        assertThat("reverse lookup works in a new context", lookupString, is("something"));
    }

    @ParameterizedTest(name = "testManyReverseLookup[clearInMemoryReverseCache={0}]")
    @BooleanSource
    void testManyReverseLookup(boolean clearInMemoryReverseCache) {
        final Map<Long, String> allocatedValues = new HashMap<>();

        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 100; i++) {
                String name = "something_" + i;
                long value = globalScope.resolve(context, name).join();
                assertThat("same value should not be allocated twice", allocatedValues, not(hasKey(value)));
                allocatedValues.put(value, name);

                // Immediately do reverse lookup and verify it worked. This also places the value in the in memory reverse cache
                assertEquals(name, globalScope.reverseLookup(context, value).join());

                if (clearInMemoryReverseCache) {
                    // Optionally clear the cache. A cleared cache represents the case where two separate processes
                    // are trying to allocate (potentially conflicting) entries in the resolver. A full cache
                    // represents a single instance trying to make many allocations at once.
                    context.getDatabase().getReverseDirectoryInMemoryCache().invalidateAll();
                }
            }
        }

        for (Map.Entry<Long, String> allocatedEntry : allocatedValues.entrySet()) {
            long value = allocatedEntry.getKey();
            String name = allocatedEntry.getValue();
            assertEquals(name, globalScope.reverseLookup((FDBStoreTimer)null, value).join());
        }
    }

    @SuppressWarnings("squid:S5778") // allow multiple calls in throws lambda
    @Test
    void testReverseLookupNotFound() {
        CompletionException ex = assertThrows(CompletionException.class, () -> globalScope.reverseLookup((FDBStoreTimer)null, -1L).join());
        assertThat(ex.getCause(), is(instanceOf(NoSuchElementException.class)));
    }

    @Test
    void testReverseLookupCaching() {
        Long value;
        try (FDBRecordContext context = database.openContext()) {
            value = globalScope.resolve(context.getTimer(), "something").join();
            context.commit();
        }

        database.clearForwardDirectoryCache();
        long baseHitCount = database.getReverseDirectoryInMemoryCache().stats().hitCount();
        long baseMissCount = database.getReverseDirectoryInMemoryCache().stats().missCount();

        String name = globalScope.reverseLookup((FDBStoreTimer)null, value).join();
        assertThat("reverse lookup gives previous result", name, is("something"));
        long hitCount = database.getReverseDirectoryInMemoryCache().stats().hitCount();
        long missCount = database.getReverseDirectoryInMemoryCache().stats().missCount();
        assertEquals(0L, hitCount - baseHitCount);
        assertEquals(1L, missCount - baseMissCount);

        // repeated lookups use the in-memory cache
        for (int i = 0; i < 10; i++) {
            name = globalScope.reverseLookup((FDBStoreTimer)null, value).join();
            assertThat("reverse lookup gives the same result", name, is("something"));
        }
        hitCount = database.getReverseDirectoryInMemoryCache().stats().hitCount();
        missCount = database.getReverseDirectoryInMemoryCache().stats().missCount();
        assertEquals(10L, hitCount - baseHitCount);
        assertEquals(1L, missCount - baseMissCount);
    }

    /**
     * Test reverse lookup in the same transaction that created the mapping. In particular,
     * the following scenario:
     * <ol>
     *     <li>A transaction is created, then</li>
     *     <li>An entry is added to the resolver and committed, and then</li>
     *     <li>The transaction is used as the parent transaction during reverse lookup</li>
     * </ol>
     *
     * <p>
     * That the value written is still returned (instead of a {@link NoSuchElementException}).
     * This is tricky because the reverse lookup will borrow the read version from the first
     * transaction, which will necessarily be from before the entry was committed to the
     * database, so the lookup will need to be retried with a fresher read version.
     * </p>
     */
    @Test
    void testReverseLookupInSameTransaction() {
        try (FDBRecordContext context = database.openContext()) {
            final String key = "some_key";
            Long value = globalScope.resolve(context, key).join();

            Cache<?, ?> cache = database.getReverseDirectoryInMemoryCache();

            final long baseHitCount = cache.stats().hitCount();
            final long baseMissCount = cache.stats().missCount();

            // Resolve without the cache
            database.clearCaches();
            assertEquals(key, globalScope.reverseLookup(context, value).join());
            assertEquals(0L, cache.stats().hitCount() - baseHitCount);
            assertEquals(1L, cache.stats().missCount() - baseMissCount);

            // Now resolve with the cache
            assertEquals(key, globalScope.reverseLookup(context, value).join());
            assertEquals(1L, cache.stats().hitCount() - baseHitCount);
            assertEquals(1L, cache.stats().missCount() - baseMissCount);
        }
    }

    @Test
    void testCacheConsistency() {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        final List<Pair<String, ResolverResult>> cacheHits = new ArrayList<>();
        final List<Pair<Long, String>> reverseCacheHits = new ArrayList<>();
        CompletableFuture<Void> loopOperation = AsyncUtil.whileTrue(() ->
                MoreAsyncUtil.delayedFuture(1, TimeUnit.MILLISECONDS, database.getScheduledExecutor())
                        .thenRun(() -> gatherCacheHits(database.getDirectoryCache(/* always return current version */ 0), cacheHits))
                        .thenRun(() -> gatherCacheHits(database.getReverseDirectoryInMemoryCache(), reverseCacheHits))
                        .thenApply(ignored2 -> keepRunning.get())
        );

        List<CompletableFuture<Pair<String, ResolverResult>>> allocationOperations = IntStream.range(0, 50)
                .mapToObj(i -> "string-" + i)
                .map(name -> database.run(ctx -> globalScope.resolveWithMetadata(ctx.getTimer(), name, ResolverCreateHooks.getDefault()))
                        .thenApply(result -> Pair.of(name, result))
                )
                .collect(Collectors.toList());

        List<Pair<String, ResolverResult>> allocations = AsyncUtil.getAll(allocationOperations).thenApply(list -> {
            keepRunning.set(false);
            return list;
        }).join();
        loopOperation.join();

        List<Pair<Long, String>> reverseAllocations = allocations.stream()
                .map(e -> Pair.of(e.getValue().getValue(), e.getKey()))
                .collect(Collectors.toList());

        validateCacheHits(cacheHits, allocations,
                (context, name) -> globalScope.resolveWithMetadata(context.getTimer(), name, ResolverCreateHooks.getDefault()));
        validateCacheHits(reverseCacheHits, reverseAllocations,
                (context, value) -> globalScope.reverseLookup(context, value));
    }

    private <K, V> void validateCacheHits(List<Pair<K, V>> cacheHits,
                                          List<Pair<K, V>> allocations,
                                          BiFunction<FDBRecordContext, K, CompletableFuture<V>> persistedMappingSupplier) {
        for (Pair<K, V> pair : cacheHits) {
            V persistedMapping;
            try (FDBRecordContext context = database.openContext()) {
                persistedMapping = persistedMappingSupplier.apply(context, pair.getKey()).join();
            }
            assertThat("every cache hit corresponds to an allocation", allocations, hasItem(equalTo(pair)));
            assertEquals(persistedMapping, pair.getValue(), "all cache hits correspond to a persisted mapping");
        }
    }

    private <K, V> void gatherCacheHits(Cache<ScopedValue<K>, V> cache, List<Pair<K, V>> cacheHits) {
        cacheHits.addAll(
                cache.asMap().entrySet().stream().map(e -> Pair.of(e.getKey().getData(), e.getValue())).collect(Collectors.toList())
        );
    }

    @Test
    void testParallelSet() {
        String key = "some-random-key-" + random.nextLong();
        List<CompletableFuture<Long>> allocations = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            allocations.add(allocateInNewContext(key, globalScope));
        }
        Set<Long> allocationSet = new HashSet<>(AsyncUtil.getAll(allocations).join());

        assertThat("only one value is allocated", allocationSet, hasSize(1));
    }

    private CompletableFuture<Long> allocateInNewContext(String key, LocatableResolver resolver) {
        FDBRecordContext context = database.openContext();
        return resolver.resolve(context.getTimer(), key).whenComplete((ignore1, ignore2) -> context.close());
    }

    @Test
    void testWriteLockCaching() {
        FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = database.openContext(null, timer)) {
            globalScope.resolve(context.getTimer(), "something").join();
            int initialCount = timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ);
            assertThat("first read must check the lock in the database", initialCount, greaterThanOrEqualTo(1));

            timer.reset();
            int oldCount = timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ);
            for (int i = 0; i < 10; i++) {
                globalScope.resolve(context.getTimer(), "something-" + i).join();

                // depending on the nature of the write safety check we may need to read the key multiple times
                // so assert that we do at least one read on each resolve
                int currentCount = timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ);
                assertThat("subsequent writes must also check the key",
                        currentCount, is(greaterThan(oldCount)));
                oldCount = currentCount;
            }

            timer.reset();
            for (int i = 0; i < 10; i++) {
                globalScope.resolve(context.getTimer(), "something-" + i).join();
            }
            assertThat("reads do not need to check the key",
                    timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(0));
        }

        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.clear();
        FDBDatabase newDatabase = factory.getDatabase(database.getClusterFile());
        FDBStoreTimer timer2 = new FDBStoreTimer();
        try (FDBRecordContext context = newDatabase.openContext(null, timer2)) {
            globalScope.resolve(context.getTimer(), "something").join();
            assertThat("state is loaded from the new database",
                    timer2.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(1));
        }
    }

    @Test
    void testCachingPerDbPerResolver() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("resolver1", KeyType.STRING, "resolver1"),
                new KeySpaceDirectory("resolver2", KeyType.STRING, "resolver2"));

        LocatableResolver resolver1;
        LocatableResolver resolver2;

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = database.openContext(null, timer)) {
            resolver1 = resolverFactory.create(keySpace.path("resolver1").toResolvedPath(context));
            resolver2 = resolverFactory.create(keySpace.path("resolver2").toResolvedPath(context));

            for (int i = 0; i < 10; i++) {
                resolver1.getVersion(context.getTimer()).join();
            }
            assertThat("We only read the value once", timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(1));

            timer.reset();
            assertThat("count is reset", timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(0));

            resolver2.getVersion(context.getTimer()).join();
            assertThat("We have to read the value for the new resolver", timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(1));

            LocatableResolver newResolver1 = resolverFactory.create(keySpace.path("resolver1").toResolvedPath(context));
            timer.reset();
            assertThat("count is reset", timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(0));

            for (int i = 0; i < 10; i++) {
                newResolver1.getVersion(context.getTimer()).join();
            }
            assertThat("we still hit the cache", timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(0));
        }
    }

    @Test
    void testEnableDisableWriteLock() {
        database.setResolverStateRefreshTimeMillis(100);

        final Long value;
        try (FDBRecordContext context = database.openContext()) {
            // resolver starts in unlocked state
            value = globalScope.resolve(context.getTimer(), "some-string").join();
        }

        globalScope.enableWriteLock().join();

        assertLocked(database, globalScope);
        try (FDBRecordContext context = database.openContext()) {
            consistently("we should still be able to read the old value", () ->
                    globalScope.resolve(context.getTimer(), "some-string").join(), is(value), 100, 10);
        }

        globalScope.disableWriteLock().join();
        try (FDBRecordContext context = database.openContext()) {
            eventually("writes should succeed", () -> {
                try {
                    globalScope.resolve(context.getTimer(), "random-value-" + random.nextLong()).join();
                } catch (CompletionException exception) {
                    return exception.getCause();
                }
                return null;
            }, is(nullValue()), 120, 10);
            consistently("writes should continue to succeed", () -> {
                try {
                    globalScope.resolve(context.getTimer(), "random-value-" + random.nextLong()).join();
                } catch (CompletionException exception) {
                    return exception.getCause();
                }
                return null;
            }, is(nullValue()), 100, 10);
        }
    }

    @SuppressWarnings("squid:S5778") // allow multiple calls in throws lambda
    @Test
    void testExclusiveLock() {
        // version is cached for 30 seconds by default
        database.setResolverStateRefreshTimeMillis(100);

        globalScope.exclusiveLock().join();
        assertLocked(database, globalScope);

        CompletionException ex = assertThrows(CompletionException.class, () -> globalScope.exclusiveLock().join());
        assertThat("we get the correct cause", ex.getCause(), allOf(
                instanceOf(LocatableResolverLockedException.class),
                hasMessageContaining("resolver must be unlocked to get exclusive lock")
        ));
    }

    @Test
    void testExclusiveLockParallel() {
        // test that when parallel threads/instances attempt to get the exclusive lock only one will win.
        List<CompletableFuture<Void>> parallelGets = new ArrayList<>();
        AtomicInteger lockGetCount = new AtomicInteger();
        for (int i = 0; i < 20; i++) {
            parallelGets.add(globalScope.exclusiveLock().handle((ignore, ex) -> {
                if (ex == null) {
                    lockGetCount.incrementAndGet();
                    return null;
                } else if (ex instanceof LocatableResolverLockedException ||
                           (ex instanceof CompletionException && ex.getCause() instanceof LocatableResolverLockedException)) {
                    return null;
                }
                throw new AssertionError("unexpected error", ex);
            }));
        }
        CompletableFuture.allOf(parallelGets.toArray(new CompletableFuture<?>[0])).join();

        assertThat("only one exclusiveLock succeeds", lockGetCount.get(), is(1));
        assertLocked(database, globalScope);
    }

    private void assertLocked(@Nonnull final FDBDatabase database, @Nonnull final LocatableResolver resolver) {
        try (FDBRecordContext context = database.openContext()) {
            eventually("write lock is enabled", () -> {
                try {
                    resolver.resolve(context.getTimer(), "random-value-" + random.nextLong()).join();
                } catch (CompletionException exception) {
                    return exception.getCause();
                }
                return null;
            }, allOf(instanceOf(LocatableResolverLockedException.class),
                    hasMessageContaining("locatable resolver is not writable")), 120, 10);
            consistently("write lock remains enabled", () -> {
                try {
                    resolver.resolve(context.getTimer(), "random-value-" + random.nextLong()).join();
                } catch (CompletionException exception) {
                    return exception.getCause();
                }
                return null;
            }, allOf(instanceOf(LocatableResolverLockedException.class),
                    hasMessageContaining("locatable resolver is not writable")), 120, 10);
        }
    }

    @SuppressWarnings("squid:S2699") // assertions in eventually block not picked up by sonar
    @Test
    void testGetVersion() {
        // version is cached for 30 seconds by default
        database.setResolverStateRefreshTimeMillis(100);

        consistently("uninitialized version is 0", () -> {
            try (FDBRecordContext context = database.openContext()) {
                return globalScope.getVersion(context.getTimer()).join();
            }
        }, is(0), 200, 10);
        globalScope.incrementVersion().join();
        eventually("version changes to 1", () -> {
            try (FDBRecordContext context = database.openContext()) {
                return globalScope.getVersion(context.getTimer()).join();
            }
        }, is(1), 120, 10);
        globalScope.incrementVersion().join();
        eventually("version changes to 2", () -> {
            try (FDBRecordContext context = database.openContext()) {
                return globalScope.getVersion(context.getTimer()).join();
            }
        }, is(2), 120, 10);
    }

    @SuppressWarnings("squid:S2699") // assertions in consistently block not picked up by sonar
    @Test
    void testParallelDbAndScopeGetVersion() {
        // version is cached for 30 seconds by default
        database.setResolverStateRefreshTimeMillis(100);
        // sets the timeout for all the db instances we create
        final FDBDatabaseFactory parallelFactory = new FDBDatabaseFactoryImpl();
        parallelFactory.setStateRefreshTimeMillis(100);
        parallelFactory.setAPIVersion(dbExtension.getAPIVersion());
        String clusterFile = database.getClusterFile();
        Supplier<FDBDatabase> databaseSupplier = () -> new FDBDatabase(parallelFactory, clusterFile);
        consistently("uninitialized version is 0", () -> {
            try (FDBRecordContext context = database.openContext()) {
                return globalScope.getVersion(context.getTimer()).join();
            }
        }, is(0), 200, 10);

        List<Pair<FDBDatabase, LocatableResolver>> simulatedInstances = IntStream.range(0, 20)
                .mapToObj(i -> {
                    FDBDatabase db = databaseSupplier.get();
                    return Pair.of(db, resolverFactory.getGlobalScope(db));
                })
                .collect(Collectors.toList());

        Supplier<CompletableFuture<Set<Integer>>> supplier = () -> {
            List<CompletableFuture<Integer>> parallelOperations = simulatedInstances.stream()
                    .map(pair -> {
                        FDBDatabase db = pair.getKey();
                        LocatableResolver resolver = pair.getValue();
                        FDBRecordContext context = db.openContext();
                        return resolver.getVersion(context.getTimer()).whenComplete((ignore, e) -> context.close());
                    }).collect(Collectors.toList());
            return AsyncUtil.getAll(parallelOperations).thenApply(HashSet::new);
        };

        consistently("all instances report the version as 0", () -> supplier.get().join(),
                is(Collections.singleton(0)), 200, 10);
        globalScope.incrementVersion().join();
        eventually("all instances report the new version once the caches have refreshed", () -> supplier.get().join(),
                is(Collections.singleton(1)), 120, 10);
    }

    @Test
    void testVersionIncrementInvalidatesCache() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(10);
        final FDBStoreTimer timer = new FDBStoreTimer();
        FDBDatabase fdb = factory.getDatabase(database.getClusterFile());
        fdb.close(); // Make sure cache is fresh, and resets version
        fdb.setResolverStateRefreshTimeMillis(100);
        String key = "some-key";
        Long value;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            value = context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, globalScope.resolve(context.getTimer(), key));
        }
        assertThat(timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ), is(greaterThanOrEqualTo(1)));

        timer.reset();
        consistently("we hit the cached value", () -> {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                assertThat("the resolved value is still the same", globalScope.resolve(context.getTimer(), key).join(), is(value));
            }
            return timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ);
        }, is(0), 200, 10);
        globalScope.incrementVersion().join();
        timer.reset();
        eventually("we see the version change and invalidate the cache", () -> {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                assertThat("the resolved value is still the same", globalScope.resolve(context.getTimer(), key).join(), is(value));
            }
            return timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ);
        }, is(1), 120, 10);
        timer.reset();
        consistently("the value is cached while the version is not changed", () -> {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                assertThat("the resolved value is still the same", globalScope.resolve(context.getTimer(), key).join(), is(value));
            }
            return timer.getCount(FDBStoreTimer.Events.DIRECTORY_READ);
        }, is(0), 200, 10);
    }

    @Test
    void testUpdatingResolverStateDirectly() {
        final ResolverStateProto.State newState;
        try (FDBRecordContext context = database.openContext()) {
            ResolverStateProto.State state = globalScope.loadResolverState(context).join();
            assertNotNull(state);

            int version = globalScope.getVersion(null).join();
            assertEquals(state.getVersion(), version);

            newState = state.toBuilder()
                    .setVersion(state.getVersion() + 1)
                    .setLock(ResolverStateProto.WriteLock.RETIRED)
                    .build();
            globalScope.saveResolverState(context, newState).join();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            ResolverStateProto.State state = globalScope.loadResolverState(context).join();
            assertEquals(newState, state);

            int potentiallyCachedVersion = globalScope.getVersion(null).join();
            assertThat(potentiallyCachedVersion, either(equalTo(newState.getVersion())).or(equalTo(newState.getVersion() - 1)));
            database.clearCaches();
            assertEquals(newState.getVersion(), globalScope.getVersion(null).join());
        }
    }

    @Test
    void enforceResolverStateMonotonicity() {
        int initialVersion = globalScope.getVersion(null).join();
        try (FDBRecordContext context = database.openContext()) {
            globalScope.saveResolverState(context, ResolverStateProto.State.newBuilder().setVersion(initialVersion + 10).build()).join();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            ResolverStateProto.State newState = ResolverStateProto.State.newBuilder()
                    .setVersion(initialVersion + 5)
                    .build();
            CompletionException completionException = assertThrows(CompletionException.class, () -> globalScope.saveResolverState(context, newState).join());
            assertNotNull(completionException.getCause());
            assertThat(completionException.getCause(), instanceOf(RecordCoreArgumentException.class));
            RecordCoreArgumentException argumentException = (RecordCoreArgumentException) completionException.getCause();
            assertThat(argumentException, hasMessageContaining("resolver state version must monotonically increase"));
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            assertEquals(initialVersion + 10, globalScope.loadResolverState(context).join().getVersion());
            assertThat(globalScope.getVersion(null).join(), either(equalTo(initialVersion)).or(equalTo(initialVersion + 10)));
            database.clearCaches();
            assertEquals(initialVersion + 10, globalScope.getVersion(null).join());
        }
    }

    @SuppressWarnings("squid:S5778") // allow multiple calls in throws lambda
    @Test
    void testWriteSafetyCheck() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("path1", KeyType.STRING, "path1"),
                new KeySpaceDirectory("path2", KeyType.STRING, "path2")
        );

        final LocatableResolver path1Resolver;
        final LocatableResolver path2Resolver;
        try (FDBRecordContext context = database.openContext()) {
            ResolvedKeySpacePath path1 = keySpace.path("path1").toResolvedPath(context);
            ResolvedKeySpacePath path2 = keySpace.path("path2").toResolvedPath(context);
            path1Resolver = resolverFactory.create(path1);
            path2Resolver = resolverFactory.create(path2);
        }

        PreWriteCheck validCheck = (context, resolver) ->
                CompletableFuture.completedFuture(Objects.equals(path1Resolver, resolver));
        PreWriteCheck invalidCheck = (context, resolver) ->
                CompletableFuture.completedFuture(Objects.equals(path2Resolver, resolver));

        ResolverCreateHooks validHooks = new ResolverCreateHooks(validCheck, DEFAULT_HOOK);
        ResolverCreateHooks invalidHooks = new ResolverCreateHooks(invalidCheck, DEFAULT_HOOK);
        Long value = path1Resolver.resolve("some-key", validHooks).join();
        try (FDBRecordContext context = database.openContext()) {
            assertThat("it succeeds and writes the value", path1Resolver.mustResolve(context, "some-key").join(), is(value));
        }

        assertThat("when reading the same key it doesn't perform the check", path1Resolver.resolve("some-key", invalidHooks).join(), is(value));

        CompletionException ex = assertThrows(CompletionException.class, () -> path1Resolver.resolve("another-key", invalidHooks).join());
        assertThat("it has the correct cause", ex.getCause(), is(instanceOf(LocatableResolverLockedException.class)));
        assertThat(ex, hasMessageContaining("prewrite check failed"));
    }

    @Test
    void testResolveWithMetadata() {
        byte[] metadata = Tuple.from("some-metadata").pack();
        MetadataHook hook = ignore -> metadata;
        final ResolverResult result;
        final ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, hook);
        result = globalScope.resolveWithMetadata("a-key", hooks).join();
        assertArrayEquals(metadata, result.getMetadata());

        // check that the result with metadata is persisted to the database
        ResolverResult expected = new ResolverResult(result.getValue(), metadata);
        try (FDBRecordContext context = database.openContext()) {
            ResolverResult resultFromDB = globalScope.mustResolveWithMetadata(context, "a-key").join();
            assertEquals(expected.getValue(), resultFromDB.getValue());
            assertArrayEquals(expected.getMetadata(), resultFromDB.getMetadata());
        }

        assertEquals(expected, globalScope.resolveWithMetadata("a-key", hooks).join());

        byte[] newMetadata = Tuple.from("some-different-metadata").pack();
        MetadataHook newHook = ignore -> newMetadata;
        final ResolverCreateHooks newHooks = new ResolverCreateHooks(DEFAULT_CHECK, newHook);

        // make sure we don't just read the cached value
        database.clearCaches();
        assertArrayEquals(metadata, globalScope.resolveWithMetadata("a-key", newHooks).join().getMetadata(),
                "hook is only run on create, does not update metadata");
    }

    @Test
    void testUpdateMetadata() {
        database.setResolverStateRefreshTimeMillis(100);

        final byte[] oldMetadata = Tuple.from("old").pack();
        final byte[] newMetadata = Tuple.from("new").pack();
        final ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, ignore -> oldMetadata);

        ResolverResult initialResult;
        try (FDBRecordContext context = database.openContext()) {
            initialResult = globalScope.resolveWithMetadata(context.getTimer(), "some-key", hooks).join();
            assertArrayEquals(initialResult.getMetadata(), oldMetadata);
        }

        globalScope.updateMetadataAndVersion("some-key", newMetadata).join();
        ResolverResult expected = new ResolverResult(initialResult.getValue(), newMetadata);
        eventually("we see the new metadata", () ->
                        globalScope.resolveWithMetadata("some-key", hooks).join(),
                is(expected), 120, 10);
    }

    @Test
    void testSetMapping() {
        Long value;
        try (FDBRecordContext context = database.openContext()) {
            value = globalScope.resolve(context.getTimer(), "an-existing-mapping").join();
        }

        try (FDBRecordContext context = database.openContext()) {
            globalScope.setMapping(context, "a-new-mapping", 99L).join();
            globalScope.setMapping(context, "an-existing-mapping", value).join();

            // need to commit before we will be able to see the mapping with resolve
            // since resolve always uses a separate transaction
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            assertThat("we can see the new mapping", globalScope.resolve(context.getTimer(), "a-new-mapping").join(), is(99L));
            assertThat("we can see the new mapping", globalScope.resolve(context, "a-new-mapping").join(), is(99L));
            assertThat("we can see the new reverse mapping", globalScope.reverseLookup(context.getTimer(), 99L).join(), is("a-new-mapping"));
            assertThat("we can see the new reverse mapping", globalScope.reverseLookup(context, 99L).join(), is("a-new-mapping"));
            assertThat("we can see the existing mapping", globalScope.resolve(context.getTimer(), "an-existing-mapping").join(), is(value));
            assertThat("we can see the existing mapping", globalScope.resolve(context, "an-existing-mapping").join(), is(value));
            assertThat("we can see the existing reverse mapping", globalScope.reverseLookup(context.getTimer(), value).join(), is("an-existing-mapping"));
            assertThat("we can see the existing reverse mapping", globalScope.reverseLookup(context, value).join(), is("an-existing-mapping"));
        }
    }

    @Test
    void createAndReadInTransaction() {
        final String key = "some_key";
        ResolverResult result;
        try (FDBRecordContext context = database.openContext()) {
            assertNull(globalScope.readInTransaction(context, key).join());

            result = globalScope.createInTransaction(context, key, ResolverCreateHooks.getDefault()).join();
            assertEquals(result, globalScope.readInTransaction(context, key).join());
            assertEquals(key, globalScope.reverseLookupInTransaction(context, result.getValue()).join());
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            assertEquals(result, globalScope.resolveWithMetadata(context, key, ResolverCreateHooks.getDefault()).join());
            assertEquals(key, globalScope.reverseLookup(context, result.getValue()).join());
            database.clearCaches();
            assertEquals(result, globalScope.resolveWithMetadata(context, key, ResolverCreateHooks.getDefault()).join());
            assertEquals(key, globalScope.reverseLookup(context, result.getValue()).join());
        }
    }

    @Test
    void createInMultipleTransactions() {
        final String key = "a_key_to_create";
        final ResolverResult committedResult;
        final List<FDBRecordContext> contexts = new ArrayList<>();
        final Set<Long> otherValues = new HashSet<>();
        try {
            FDBRecordContext firstContext = database.openContext();
            firstContext.getReadVersion();
            contexts.add(firstContext);
            for (int i = 0; i < 10; i++) {
                FDBRecordContext nextContext = database.openContext();
                nextContext.getReadVersion();
                contexts.add(nextContext);
            }

            assertNull(globalScope.readInTransaction(firstContext, key).join());
            String expectedMetadata;
            ResolverCreateHooks firstCreateHooks;
            if (globalScope instanceof ScopedDirectoryLayer) {
                expectedMetadata = null;
                firstCreateHooks = ResolverCreateHooks.getDefault();
            } else {
                expectedMetadata = "first transaction";
                firstCreateHooks = new ResolverCreateHooks(DEFAULT_CHECK, ignore -> expectedMetadata.getBytes(StandardCharsets.UTF_8));
            }
            committedResult = globalScope.createInTransaction(firstContext, key, firstCreateHooks).join();
            assertEquals(expectedMetadata, committedResult.getMetadata() == null ? null : new String(committedResult.getMetadata(), StandardCharsets.UTF_8));
            assertEquals(key, globalScope.reverseLookupInTransaction(firstContext, committedResult.getValue()).join());

            for (FDBRecordContext otherContext : contexts.subList(1, contexts.size())) {
                assertNull(globalScope.readInTransaction(otherContext, key).join());
                ResolverResult otherResult = globalScope.createInTransaction(otherContext, key, ResolverCreateHooks.getDefault()).join();
                assertEquals(key, globalScope.reverseLookupInTransaction(otherContext, otherResult.getValue()).join());
                otherValues.add(otherResult.getValue());
            }

            firstContext.commit();
            for (FDBRecordContext otherContext : contexts.subList(1, contexts.size())) {
                assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, otherContext::commit);
            }
        } finally {
            contexts.forEach(FDBRecordContext::close);
        }

        try (FDBRecordContext context = database.openContext()) {
            assertEquals(committedResult, globalScope.resolveWithMetadata(context, key, ResolverCreateHooks.getDefault()).join());
            database.clearCaches();
            assertEquals(committedResult, globalScope.resolveWithMetadata(context, key, ResolverCreateHooks.getDefault()).join());
            otherValues.remove(committedResult.getValue());

            for (Long otherValue : otherValues) {
                CompletionException err = assertThrows(CompletionException.class, () -> globalScope.reverseLookup(context, otherValue).join());
                assertNotNull(err.getCause());
                assertThat(err.getCause(), instanceOf(NoSuchElementException.class));
            }
        }
    }

    @Test
    void onlyCacheCommittedResults() {
        final String key = "key_to_create_but_not_commit";
        final ResolverResult uncommittedResult;
        try (FDBRecordContext context = database.openContext()) {
            uncommittedResult = globalScope.createInTransaction(context, key, ResolverCreateHooks.getDefault()).join();
            assertNotNull(uncommittedResult);
            assertEquals(key, globalScope.reverseLookupInTransaction(context, uncommittedResult.getValue()).join());
            // do not commit
        }

        try (FDBRecordContext context = database.openContext()) {
            assertNull(globalScope.readInTransaction(context, key).join());
            ResolverResult intermediateResult = globalScope.createInTransaction(context, key, ResolverCreateHooks.getDefault()).join();
            assertEquals(intermediateResult, globalScope.readInTransaction(context, key).join());
            assertEquals(key, globalScope.reverseLookupInTransaction(context, intermediateResult.getValue()).join());
            CompletionException reversLookupErr = assertThrows(CompletionException.class, () -> globalScope.reverseLookup(context, uncommittedResult.getValue()).join());
            assertNotNull(reversLookupErr.getCause());
            assertThat(reversLookupErr.getCause(), instanceOf(NoSuchElementException.class));
            // do not commit
        }

        try (FDBRecordContext context1 = database.openContext(); FDBRecordContext context2 = database.openContext()) {
            context1.getReadVersion();
            context2.getReadVersion();

            byte[] conflictKey = ByteArrayUtil2.unprint("conflict_key");
            context1.ensureActive().addReadConflictKey(conflictKey);
            context2.ensureActive().addWriteConflictKey(conflictKey);

            assertNull(globalScope.readInTransaction(context1, key).join());
            ResolverResult intermediateResult = globalScope.createInTransaction(context1, key, ResolverCreateHooks.getDefault()).join();
            assertEquals(intermediateResult, globalScope.readInTransaction(context1, key).join());
            assertEquals(key, globalScope.reverseLookupInTransaction(context1, intermediateResult.getValue()).join());
            CompletionException reversLookupErr = assertThrows(CompletionException.class, () -> globalScope.reverseLookup(context1, uncommittedResult.getValue()).join());
            assertNotNull(reversLookupErr.getCause());
            assertThat(reversLookupErr.getCause(), instanceOf(NoSuchElementException.class));

            context2.commit();

            // Attempt to commit context1, but it fails due to a transaction conflict (on conflict_key)
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
        }

        final ResolverResult committedResult;
        try (FDBRecordContext context = database.openContext()) {
            assertNull(globalScope.readInTransaction(context, key).join());
            committedResult = globalScope.createInTransaction(context, key, ResolverCreateHooks.getDefault()).join();
            assertEquals(key, globalScope.reverseLookupInTransaction(context, committedResult.getValue()).join());

            context.commit();
        }

        assertEquals(committedResult, database.getDirectoryCache(globalScope.getVersion(null).join())
                .getIfPresent(globalScope.wrap(key)));
        assertEquals(key, database.getReverseDirectoryInMemoryCache().getIfPresent(globalScope.wrap(committedResult.getValue())));
        database.clearCaches();
        assertNull(database.getDirectoryCache(globalScope.getVersion(null).join())
                .getIfPresent(globalScope.wrap(key)));
        assertNull(database.getReverseDirectoryInMemoryCache().getIfPresent(globalScope.wrap(committedResult.getValue())));

        try (FDBRecordContext context = database.openContext()) {
            assertEquals(committedResult, globalScope.readInTransaction(context, key).join());
            assertEquals(key, globalScope.reverseLookupInTransaction(context, committedResult.getValue()).join());
            context.commit();
        }
        assertEquals(committedResult, database.getDirectoryCache(globalScope.getVersion(null).join())
                .getIfPresent(globalScope.wrap(key)));
        assertEquals(key, database.getReverseDirectoryInMemoryCache().getIfPresent(globalScope.wrap(committedResult.getValue())));
    }

    @SuppressWarnings("squid:S5778") // allow multiple calls in throws lambda
    @Test
    void testSetMappingWithConflicts() {
        Long value;
        try (FDBRecordContext context = database.openContext()) {
            value = globalScope.resolve(context.getTimer(), "an-existing-mapping").join();
        }

        CompletionException existingMappingException = assertThrows(CompletionException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.setMapping(context, "an-existing-mapping", value + 1).join();
            }
        });
        assertThat("cause is a record core exception", existingMappingException.getCause(), is(instanceOf(RecordCoreException.class)));
        assertThat("it has a helpful message", existingMappingException.getCause(),
                hasMessageContaining("mapping already exists with different value"));

        try (FDBRecordContext context = database.openContext()) {
            assertThat("will still only see the original mapping",
                    globalScope.mustResolve(context, "an-existing-mapping").join(), is(value));
            assertThat("will still only see the original reverse mapping",
                    globalScope.reverseLookup(context, value).join(), is("an-existing-mapping"));
            try {
                String key = globalScope.reverseLookup(context, value + 1).join();
                // there is a small chance that the mapping does exist, but it should be for some other key
                assertThat(key, is(not("an-existing-mapping")));
            } catch (CompletionException ex) {
                // we will get an exception if the reverse mapping does not exist, most of the time this will be the case
                assertThat("no such element on reverse lookup", ex.getCause(), is(instanceOf(NoSuchElementException.class)));
            }
        }

        CompletionException differentKeyException = assertThrows(CompletionException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.setMapping(context, "a-different-key", value).join();
            }
        });
        assertThat("cause is a record core exception", differentKeyException.getCause(), is(instanceOf(RecordCoreException.class)));
        assertThat("it has a helpful message", differentKeyException.getCause(),
                hasMessageContaining("reverse mapping already exists with different key"));

        assertThrows(CompletionException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.mustResolve(context, "a-different-key").join();
            }
        }, "nothing is added for a-different-key");
    }

    @Test
    void testSetMappingWithUpdatedValue() {
        final String key = "key_with_meta_data";
        final String metaData1 = "meta_data_1";
        final String metaData2 = "meta_data_2";
        final ResolverResult initialResult;
        try (FDBRecordContext context = database.openContext()) {
            initialResult = globalScope.createInTransaction(context, key, new ResolverCreateHooks(List.of(), ignore -> {
                if (globalScope instanceof ScopedDirectoryLayer) {
                    return null;
                } else {
                    return metaData1.getBytes(StandardCharsets.UTF_8);
                }
            })).join();

            if (globalScope instanceof ScopedDirectoryLayer) {
                assertNull(initialResult.getMetadata());
            } else {
                assertNotNull(initialResult.getMetadata());
                assertEquals(metaData1, new String(initialResult.getMetadata(), StandardCharsets.UTF_8));
            }
            context.commit();
        }

        assertEquals(initialResult, globalScope.resolveWithMetadata((FDBStoreTimer) null, key, ResolverCreateHooks.getDefault()).join());

        // Update the meta-data on this key
        final ResolverResult newResult = new ResolverResult(initialResult.getValue(), metaData2.getBytes(StandardCharsets.UTF_8));
        try (FDBRecordContext context = database.openContext()) {
            if (globalScope instanceof ScopedDirectoryLayer) {
                UnsupportedOperationException err = assertThrows(UnsupportedOperationException.class, () -> globalScope.setMapping(context, key, newResult).join());
                assertThat(err, hasMessageContaining("cannot manually add mappings"));
            } else {
                CompletionException err = assertThrows(CompletionException.class, () -> globalScope.setMapping(context, key, newResult).join());
                assertNotNull(err.getCause());
                assertThat(err.getCause(), hasMessageContaining("mapping already exists with different value"));
            }
        }
    }

    @Test
    void testSetWindow() {
        Map<String, Long> oldMappings = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 20; i++) {
                String key = "old-resolved-" + i;
                Long value = globalScope.resolve(context.getTimer(), key).join();
                oldMappings.put(key, value);
            }
        }

        globalScope.setWindow(10000L).join();

        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < 20; i++) {
                Long value = globalScope.resolve(context.getTimer(), "new-resolved-" + i).join();
                assertThat("resolved value is larger than the set window", value, greaterThanOrEqualTo(10000L));
            }

            for (Map.Entry<String, Long> entry : oldMappings.entrySet()) {
                Long value = globalScope.resolve(context.getTimer(), entry.getKey()).join();
                assertThat("we can still read the old mappings", value, is(entry.getValue()));
            }
        }
    }

    // Protected methods
    @Test
    void testReadCreateExists() {
        ResolverResult value;
        try (FDBRecordContext context = database.openContext()) {
            value = globalScope.create(context, "a-string").join();

            assertThat("we see the value exists", globalScope.read(context, "a-string").join().isPresent(), is(true));
            assertThat("we see other values don't exist", globalScope.read(context, "something-else").join().isPresent(), is(false));
            assertThat("we can read the value", globalScope.read(context, "a-string").join(), is(Optional.of(value)));
            assertThat("we get nothing for other values", globalScope.read(context, "something-else").join(), is(Optional.empty()));
        }
    }

    @Test
    void testValidateMissingReverseEntries() {
        final List<ResolverKeyValue> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String key = "key_" + i;
            entries.add(new ResolverKeyValue(key, globalScope.resolveWithMetadata("key_" + i,
                    ResolverCreateHooks.getDefault()).join()));
        }

        final Set<ResolverValidator.ValidatedEntry> missingEntries = new HashSet<>();
        final Set<ResolverValidator.ValidatedEntry> invalidEntries = new HashSet<>();

        missingEntries.add(resolverFactory.deleteReverseEntry(globalScope, entries.get(0)));
        missingEntries.add(resolverFactory.deleteReverseEntry(globalScope, entries.get(3)));
        missingEntries.add(resolverFactory.deleteReverseEntry(globalScope, entries.get(7)));

        // map reverse lookup or key_8 to key_1
        invalidEntries.add(resolverFactory.putReverseEntry(globalScope, entries.get(8), "key_1"));
        // map reverse lookup or key_9 to key_2
        invalidEntries.add(resolverFactory.putReverseEntry(globalScope, entries.get(9), "key_2"));

        final Set<ResolverValidator.ValidatedEntry> allBadEntries = new HashSet<>();
        allBadEntries.addAll(missingEntries);
        allBadEntries.addAll(invalidEntries);

        validate(globalScope, allBadEntries);

        // Repair missing entries
        try (FDBRecordContext context = globalScope.getDatabase().openContext()) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE,
                    ResolverValidator.validate(globalScope, context, null, 5, true, ScanProperties.FORWARD_SCAN)
                            .forEach(validated -> { }).thenCompose(vignore -> context.commitAsync()));
        }

        validate(globalScope, Collections.emptySet());
    }

    private void validate(LocatableResolver locatableResolver, Set<ResolverValidator.ValidatedEntry> expectedBadEntries) {
        final Set<ResolverValidator.ValidatedEntry> foundBadEntries = new HashSet<>();
        ResolverValidator.validate(
                null,
                locatableResolver,
                ExecuteProperties.newBuilder()
                        .setFailOnScanLimitReached(false)
                        .setScannedRecordsLimit(2),   // Make sure continuations are used
                3,
                true,
                foundBadEntries::add);

        assertEquals(foundBadEntries, expectedBadEntries);
    }
}
