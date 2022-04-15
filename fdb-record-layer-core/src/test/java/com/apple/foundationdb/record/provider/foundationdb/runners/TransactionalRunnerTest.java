/*
 * TransactionalRunnerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.runners;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class TransactionalRunnerTest extends FDBTestBase {

    private FDBDatabase database;

    @BeforeEach
    public void getDatabase() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Test
    void commits() {
        final Random random = new Random();
        final byte[] key = randomBytes(100, random);
        key[0] = 0x10; // to make sure it doesn't end up in unwritable space
        final byte[] value = randomBytes(200, random);
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final String result = runner.runAsync(false, context -> {
                context.ensureActive().set(key, value);
                return CompletableFuture.completedFuture("boo");
            }).join();
            assertEquals("boo", result);

            assertArrayEquals(value, runner.runAsync(false, context -> context.ensureActive().get(key)).join());
        }
    }

    @Test
    void aborts() {
        final Random random = new Random();
        final byte[] key = randomBytes(100, random);
        key[0] = 0x10; // to make sure it doesn't end up in unwritable space
        final byte[] value = randomBytes(200, random);
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final Exception cause = new Exception("ABORT");
            final CompletionException exception = assertThrows(CompletionException.class,
                    () -> runner.runAsync(false, context -> {
                        context.ensureActive().set(key, value);
                        CompletableFuture<String> future = new CompletableFuture<>();
                        future.completeExceptionally(cause);
                        return future;
                    }).join());
            assertEquals(cause, exception.getCause());

            assertArrayEquals(null, runner.runAsync(false, context -> context.ensureActive().get(key)).join());
        }
    }

    @Test
    void conflicts() {
        final Conflicter conflicter = new Conflicter();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            assertConflicts(runner.runAsync(false,
                    context -> conflicter.readOtherKeySetKey(context)
                            .thenCompose(vignore ->
                                    runner.runAsync(false, conflicter::readKeySetOtherKey))));
            conflicter.expectValues(runner, null, conflicter.otherValue);
        }
    }

    @Test
    void doesNotConflictBeforeUsingTransaction() {
        // this is to assert that we can do stuff before the transaction starts.
        // We do this in the test by creating another transaction that would conflict, but in real use cases, this is
        // more useful to do expensive operations that might cause issues with the 5 second transaction limit
        final Conflicter conflicter = new Conflicter();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            assertEquals("set key", runner.runAsync(false,
                    context -> runner.runAsync(false, conflicter::readKeySetOtherKey)
                            .thenCompose(vignore -> conflicter.readOtherKeySetKey(context))).join());

            conflicter.expectValues(runner, conflicter.value, conflicter.otherValue);
        }
    }

    /**
     * Validate that if the user sets {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics}
     * on the runner to something non-trivial, then the cached version is only used the first time.
     * Subsequent attempts, especially attempts that are caused by conflicts, do not want to use the stale read version,
     * as it may see the same old data multiple times (and therefore conflict each time).
     */
    @Test
    public void runWithWeakReadSemantics() {
        final boolean tracksReadVersions = database.isTrackLastSeenVersionOnRead();
        final boolean tracksCommitVersions = database.isTrackLastSeenVersionOnCommit();
        try {
            // Enable version tracking so that the database will use the latest version seen if we have weak read semantics
            database.setTrackLastSeenVersionOnRead(true);
            database.setTrackLastSeenVersionOnCommit(false); // disable commit tracking so that the stale read version is definitely the version remembered
            final byte[] key = Tuple.from(UUID.randomUUID()).pack(); // not actually modified, so value doesn't matter

            // Commit something and cache just the read version
            long firstReadVersion;
            final Function<FDBRecordContext, CompletableFuture<Long>> getReadVersionWithWriteConflict = context -> {
                context.ensureActive().addWriteConflictKey(key);
                return context.getReadVersionAsync();
            };
            try (TransactionalRunner runner = defaultTransactionalRunner()) {
                firstReadVersion = runner.runAsync(false, getReadVersionWithWriteConflict).join();
            }

            FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(
                    firstReadVersion, Long.MAX_VALUE, true);
            final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder()
                    .setWeakReadSemantics(weakReadSemantics);
            try (TransactionalRunner runner = new TransactionalRunner(database, contextConfigBuilder)) {
                assertEquals(firstReadVersion, runner.runAsync(false, getReadVersionWithWriteConflict).join());
                final Long newReadVersion = runner.runAsync(true, getReadVersionWithWriteConflict).join();
                assertNotEquals(firstReadVersion, newReadVersion);
                assertEquals(newReadVersion, runner.runAsync(false, getReadVersionWithWriteConflict).join());

                assertConflicts(runner.runAsync(false, context -> {
                    context.ensureActive().addReadConflictKey(key); // will cause conflict
                    context.ensureActive().addWriteConflictKey(key);
                    return CompletableFuture.completedFuture("ignored");
                }));

                assertEquals("boxes", runner.runAsync(true, context -> {
                    context.ensureActive().addReadConflictKey(key); // no conflict because we cleared the weak read semantics
                    context.ensureActive().addWriteConflictKey(key);
                    return CompletableFuture.completedFuture("boxes");
                }).join());

                assertConflicts(runner.runAsync(false, context -> {
                    context.ensureActive().addReadConflictKey(key); // conflicts because we are reusing the weak read semantics
                    context.ensureActive().addWriteConflictKey(key);
                    return CompletableFuture.completedFuture("ignored");
                }));
            }
        } finally {
            database.setTrackLastSeenVersionOnRead(tracksReadVersions);
            database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
        }
    }

    @Test
    void closesContexts() {
        List<FDBRecordContext> contexts = new ArrayList<>();
        AtomicInteger completed = new AtomicInteger();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            for (int i = 0; i < 20; i++) {
                runner.runAsync(false, context -> {
                    contexts.add(context);
                    return MoreAsyncUtil.delayedFuture(60, TimeUnit.SECONDS)
                            // delayed future isn't guaranteed, so we need to make sure at least some delayed past the close
                            .thenApply(vignore -> completed.incrementAndGet());
                }); // DO NOT join
            }
            for (final FDBRecordContext context : contexts) {
                assertFalse(context.isClosed());
            }
        }
        assertThat(completed.get(), Matchers.lessThan(20));
        for (final FDBRecordContext context : contexts) {
            assertTrue(context.isClosed());
        }

    }

    /**
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner} has features that require mutating
     * the {@link FDBRecordContextConfig.Builder} on an existing runner. It's not clear whether this is a good idea or
     * not, but it's there, so this test makes sure that is an option here, leaving changing the api for another time.
     * It's possible that this is a symptom of a time before we had these builders, rather than intentional design, and
     * thus we should remove these setters.
     */
    @Test
    void mutateContextConfigMdc() {
        final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder();
        try (TransactionalRunner runner = new TransactionalRunner(database, contextConfigBuilder)) {
            assertNull(runner.runAsync(false,
                            context -> CompletableFuture.completedFuture(MDC.get("foobar")))
                    .join());

            final Map<String, String> mdc = Map.of("foobar", "boxes");
            contextConfigBuilder.setMdcContext(mdc);

            assertEquals(mdc, runner.runAsync(false,
                            context -> CompletableFuture.completedFuture(context.getMdcContext()))
                    .join());

        }
    }

    /**
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner} has features that require mutating
     * the {@link FDBRecordContextConfig.Builder} on an existing runner. It's not clear whether this is a good idea or
     * not, but it's there, so this test makes sure that is an option here, leaving changing the api for another time.
     * It's possible that this is a symptom of a time before we had these builders, rather than intentional design, and
     * thus we should remove these setters.
     */
    @Test
    void mutateContextConfigWeakReadSemantics() {
        final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder();
        try (TransactionalRunner runner = new TransactionalRunner(database, contextConfigBuilder)) {
            final byte[] key = Tuple.from(UUID.randomUUID()).pack(); // not actually modified, so value doesn't matter

            // Commit something and cache just the read version
            long firstReadVersion;
            final Function<FDBRecordContext, CompletableFuture<Long>> getReadVersionWithWriteConflict = context -> {
                context.ensureActive().addWriteConflictKey(key);
                return context.getReadVersionAsync();
            };

            final boolean tracksReadVersions = database.isTrackLastSeenVersionOnRead();
            final boolean tracksCommitVersions = database.isTrackLastSeenVersionOnCommit();
            try {
                // Enable version tracking so that the database will use the latest version seen if we have weak read semantics
                database.setTrackLastSeenVersionOnRead(true);
                database.setTrackLastSeenVersionOnCommit(false); // disable commit tracking so that the stale read version is definitely the version remembered
                final Long readVersion = runner.runAsync(false,
                        FDBRecordContext::getReadVersionAsync).join();
                final FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(
                        readVersion, Long.MAX_VALUE, true);
                contextConfigBuilder.setWeakReadSemantics(weakReadSemantics);
                assertEquals(readVersion, runner.runAsync(false, getReadVersionWithWriteConflict)
                        .join());
                assertEquals(weakReadSemantics, contextConfigBuilder.getWeakReadSemantics());
                assertNotEquals(readVersion, runner.runAsync(true, getReadVersionWithWriteConflict)
                        .join());
                // clearing the weak read semantics should not clear it on the config bulider passed in
                assertEquals(weakReadSemantics, contextConfigBuilder.getWeakReadSemantics());
            } finally {
                database.setTrackLastSeenVersionOnRead(tracksReadVersions);
                database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
            }
        }
    }

    @Nonnull
    private TransactionalRunner defaultTransactionalRunner() {
        return new TransactionalRunner(database, FDBRecordContextConfig.newBuilder().build());
    }

    private <T> void assertConflicts(CompletableFuture<T> future) {
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception.getCause(), Matchers.instanceOf(FDBException.class));
        assertEquals(FDBError.NOT_COMMITTED.code(), ((FDBException) exception.getCause()).getCode());
    }

    @Nonnull
    private static byte[] randomBytes(final int count, final Random random) {
        final byte[] key = new byte[count];
        random.nextBytes(key);
        return key;
    }

    private static class Conflicter {
        private final byte[] key;
        private final byte[] otherKey;
        private final byte[] value;
        private final byte[] otherValue;

        public Conflicter() {
            final Random random = new Random();
            key = randomBytes(100, random);
            key[0] = 0x10; // to make sure it doesn't end up in unwritable space
            otherKey = randomBytes(100, random);
            otherKey[0] = 0x11; // to make sure it doesn't end up in unwritable space, and is different from key
            value = randomBytes(200, random);
            value[0] = 0x10;
            otherValue = randomBytes(200, random);
            otherValue[0] = 0x11; // definitely different
        }

        public CompletableFuture<String> readKeySetOtherKey(FDBRecordContext context) {
            return context.ensureActive().get(key)
                    .thenCompose(vignore2 -> {
                        context.ensureActive().set(otherKey, otherValue);
                        return CompletableFuture.completedFuture("set otherKey");
                    });
        }

        public CompletableFuture<String> readOtherKeySetKey(FDBRecordContext context) {
            return context.ensureActive().get(otherKey)
                    .thenCompose(vignore -> {
                        context.ensureActive().set(key, value);
                        return CompletableFuture.completedFuture("set key");
                    });
        }

        public void expectValues(TransactionalRunner runner,
                                 @Nullable byte[] expectedForKey, @Nullable byte[] expectedForOtherKey) {
            assertArrayEquals(expectedForKey, runner.runAsync(false,
                    context -> context.ensureActive().get(key)).join());
            assertArrayEquals(expectedForOtherKey, runner.runAsync(false,
                    context -> context.ensureActive().get(otherKey)).join());
        }
    }
}
