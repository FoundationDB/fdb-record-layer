/*
 * GlobalResolverFactoryTest.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBReverseDirectoryCache;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver.LocatableResolverLockedException;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.apple.foundationdb.record.TestHelpers.consistently;
import static com.apple.foundationdb.record.TestHelpers.eventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(Tags.WipesFDB)
@Tag(Tags.RequiresFDB)
class GlobalResolverFactoryTest {
    private FDBDatabase database;
    private Random random;

    @BeforeEach
    public void setup() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setDirectoryCacheSize(100);
        database = factory.getDatabase();
        database.clearCaches();
        wipeFDB();
        database.setResolverStateRefreshTimeMillis(100);
        random = new Random();
    }

    @AfterEach
    public void teardown() {
        // tests that modify migration state of global directory layer need to wipe FDB to prevent pollution of other suites
        wipeFDB();
    }

    private void wipeFDB() {
        database.run((context -> {
            context.ensureActive().clear(new Range(new byte[] {(byte)0x00}, new byte[] {(byte)0xFF}));
            return null;
        }));
    }

    @Test
    public void testDefaultGlobal() {
        assertThat("the default resolver is the global FDB resolver",
                GlobalResolverFactory.globalResolver(database).join(), is(ScopedDirectoryLayer.global(database)));
    }

    @Test
    public void testRetireSwitchesToNewGlobalDirectoryLayer() {
        LocatableResolver scope = ScopedDirectoryLayer.global(database);

        consistently("we get the default global directory layer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(ScopedDirectoryLayer.global(database)), 200, 10);

        scope.retireLayer().join();

        eventually("we switch to the new directory layer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(ScopedInterningLayer.global(database)), 250, 10);

        consistently("we still get the new directory layer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(ScopedInterningLayer.global(database)), 200, 10);
    }

    @Test
    public void testGlobalFactoryCachesResponses() {
        final FDBStoreTimer timer = new FDBStoreTimer();

        assertThat("we get the default global resolver",
                GlobalResolverFactory.globalResolver(timer, database).join(), is(ScopedDirectoryLayer.global(database)));
        assertThat("we checked the state of the resolver",
                timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ), is(1));

        consistently("we get the cached value", () -> {
            timer.reset();
            assertThat("we get still get the default global resolver",
                    GlobalResolverFactory.globalResolver(timer, database).join(), is(ScopedDirectoryLayer.global(database)));
            return timer.getCount(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ);
        }, is(0), 80, 10);
    }

    @Test
    public void testSimulateMigration() {
        // This test is not strictly for the GlobalResolverFactory, but instead shows how various pieces can be glued
        // together to migrate the global directory layer from the ScopedDirectoryLayer to ScopedInterningLayer implementation.
        final LocatableResolver legacyGlobal = ScopedDirectoryLayer.global(database);
        final LocatableResolver newGlobal = ScopedInterningLayer.global(database);

        consistently("the global resolver is the ScopedDirectoryLayer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(legacyGlobal), 250, 10);

        // Migrate to the ScopedInterningLayer
        Map<String, Long> afterMigrationMappings;
        afterMigrationMappings = testMigration(legacyGlobal, newGlobal, seed(20));

        eventually("the global resolver switches to the StringInterningLayer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(newGlobal), 500, 50);


        // seed some more mappings
        Map<String, Long> preBackMigration = ImmutableMap.<String, Long>builder()
                .putAll(afterMigrationMappings)
                .putAll(seed(20))
                .build();
        // migrate back using the extended directory layer
        LocatableResolver extendedGlobal = ExtendedDirectoryLayer.global(database);
        Map<String, Long> totalMapping = testMigration(newGlobal, extendedGlobal, preBackMigration);

        // We have to use ExtendedDirectoryLayer for the migration but GlobalResolverFactory will switch back
        // to using ScopedDirectoryLayer. This is OK since the implementations are compatible.
        eventually("the global resolver switches back to the ScopedDirectoryLayer",
                () -> GlobalResolverFactory.globalResolver(database).join(),
                is(legacyGlobal), 500, 50);

        // test that all the mappings are seen by the legacy resolver (ScopedDirectoryLayer)
        assertInGlobalReverseDirectoryCache(legacyGlobal, totalMapping);
        assertContains(legacyGlobal, totalMapping);
    }

    private Map<String, Long> testMigration(LocatableResolver primary, LocatableResolver replica, Map<String, Long> seeded) {
        AtomicBoolean keepGoing = new AtomicBoolean(true);
        Map<String, Long> preMigration = new HashMap<>(seeded);
        Map<String, Long> postMigration = new HashMap<>();
        CompletableFuture<Void> parallelWrites = parallelWrites(preMigration, postMigration, keepGoing);

        migrate(primary, replica);
        keepGoing.set(false);
        parallelWrites.join();

        assertContainsNone(primary, postMigration);
        assertContains(replica, ImmutableMap.<String, Long>builder()
                .putAll(postMigration)
                .putAll(preMigration).build());

        return ImmutableMap.<String, Long>builder()
                .putAll(preMigration)
                .putAll(postMigration)
                .build();
    }

    private Map<String, Long> seed(int nEntries) {
        Map<String, Long> mappings = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            for (int i = 0; i < nEntries; i++) {
                String key = String.format("%d-seeded-mapping", Math.abs(random.nextLong()));
                Long value = GlobalResolverFactory.globalResolver(database)
                        .thenCompose(resolver -> resolver.resolve(context.getTimer(), key)).join();
                mappings.put(key, value);
            }
        }
        return mappings;
    }

    private CompletableFuture<Void> parallelWrites(Map<String, Long> preMigration,
                                                   Map<String, Long> postMigration,
                                                   AtomicBoolean keepGoing) {
        AtomicBoolean migrationStarted = new AtomicBoolean();
        return AsyncUtil.whileTrue(() ->
                MoreAsyncUtil.delayedFuture(1, TimeUnit.MILLISECONDS)
                        .thenComposeAsync(ignore -> {
                            String key = String.format("%d-parallel", Math.abs(random.nextLong()));
                            FDBRecordContext context = database.openContext();
                            return GlobalResolverFactory.globalResolver(database)
                                    .thenComposeAsync(resolver -> resolver.resolve(context.getTimer(), key))
                                    .handle((resolved, ex) -> {
                                        context.close();
                                        if (ex == null) {
                                            return migrationStarted.get() ? postMigration.put(key, resolved) : preMigration.put(key, resolved);
                                        } else if (ex.getCause() instanceof LocatableResolverLockedException) {
                                            migrationStarted.set(true);
                                            return null;
                                        }
                                        throw new AssertionError("unexpected error");
                                    });
                        })
                        .thenApply(ignore -> keepGoing.get())
        );
    }

    private void migrate(LocatableResolver primary, LocatableResolver replica) {
        long waitTime = 2 * database.getResolverStateCacheRefreshTime();
        primary.enableWriteLock().join();
        replica.enableWriteLock().join();

        try {
            // wait for 2x the lifetime of the lock state cache
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            fail("interrupted");
        }

        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary);
        replicator.copyTo(replica);
        byte[] oldDigest = new ResolverMappingDigest(primary).computeDigest().join();
        byte[] newDigest = new ResolverMappingDigest(replica).computeDigest().join();
        assertArrayEquals(oldDigest, newDigest);

        primary.retireLayer().join();

        try {
            // wait for 2x the *combined* lifetimes of the lock state cache and global resolver cache
            Thread.sleep(2 * waitTime);
        } catch (InterruptedException e) {
            fail("interrupted");
        }

        replica.disableWriteLock().join();

        try {
            // wait for 2x the lifetime of the lock state cache
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            fail("interrupted");
        }
    }

    private void assertContains(@Nonnull final LocatableResolver resolver, @Nonnull Map<String, Long> mapping) {
        for (Map.Entry<String, Long> entry : mapping.entrySet()) {
            try (FDBRecordContext context = database.openContext()) {
                Long readValue = resolver.mustResolve(context, entry.getKey()).join();
                assertThat("mapping is present in the resolver", readValue, is(entry.getValue()));
                String reverseLookupKey = resolver.reverseLookup(null, entry.getValue()).join();
                assertThat("reverse mapping is present in the resolver", reverseLookupKey, is(entry.getKey()));
            }
        }
    }

    private void assertInGlobalReverseDirectoryCache(@Nonnull final LocatableResolver resolver, @Nonnull Map<String, Long> mapping) {
        FDBReverseDirectoryCache reverseDirectoryCache = database.getReverseDirectoryCache();
        reverseDirectoryCache.clearStats();
        for (Map.Entry<String, Long> entry : mapping.entrySet()) {
            String key = reverseDirectoryCache.get(resolver.wrap(entry.getValue())).join()
                    .orElseThrow(() -> new AssertionError("value not found"));
            assertThat(key, is(entry.getKey()));
        }
        assertEquals(reverseDirectoryCache.getPersistentCacheMissCount(), 0, "there are no hard cache misses");
        assertEquals(reverseDirectoryCache.getPersistentCacheHitCount(), mapping.size(), "we get 1 cache hit per mapping lookup");
    }

    private void assertContainsNone(@Nonnull final LocatableResolver resolver, @Nonnull Map<String, Long> mapping) {
        for (Map.Entry<String, Long> entry : mapping.entrySet()) {
            try (FDBRecordContext context = database.openContext()) {
                resolver.mustResolve(context, entry.getKey()).join();
                fail("mustResolve should throw exception");
            } catch (CompletionException ex) {
                assertThat(ex.getCause(), is(instanceOf(NoSuchElementException.class)));
                assertThat(ex.getCause().getMessage(), containsString(entry.getKey()));
            }

            try {
                resolver.reverseLookup(null, entry.getValue()).join();
                fail("reverseLookup should fail");
            } catch (CompletionException ex) {
                assertThat(ex.getCause(), is(instanceOf(NoSuchElementException.class)));
                assertThat(ex.getCause().getMessage(), containsString("reverse lookup of"));
            }
        }
    }
}
