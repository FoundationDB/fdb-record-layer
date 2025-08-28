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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.TriFunction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
class TransactionalRunnerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase database;
    private FDBStoreTimer timer;
    private byte[] key;
    private byte[] value;

    @BeforeEach
    public void setUp() {
        database = dbExtension.getDatabase();
        final Random random = new Random();
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        key = database.run(path::toSubspace).pack(Tuple.from("key"));
        value = randomBytes(200, random);
        timer = new FDBStoreTimer();
    }

    @AfterEach
    public void tearDown() {
        assertEquals(timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT), timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT),
                "an equal number of contexts should have been opened and closed");
        timer = null;
    }

    @Test
    void commits() {
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final String result = runner.runAsync(false, context -> {
                context.ensureActive().set(key, value);
                return CompletableFuture.completedFuture("boo");
            }).join();
            assertEquals("boo", result);

            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT));
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT));

            assertValue(runner, key, value);
        }
    }

    @Test
    void commitsSynchronous() {
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final String result = runner.run(false, context -> {
                context.ensureActive().set(key, value);
                return "boo";
            });
            assertEquals("boo", result);

            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT));
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT));

            assertValue(runner, key, value);
        }
    }

    /**
     * Validate the behavior when the future returned by {@link TransactionalRunner#runAsync(boolean, Function)}
     * completes exceptionally. The work done by the transaction should not be committed, and the underlying
     * exception should be forwarded up wrapped as a {@link CompletionException}.
     */
    @Test
    void abortsAsyncInChainedFuture() {
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

            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT));
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT));

            assertValue(runner, key, null);
        }
    }

    /**
     * Check the behavior of an exception that is thrown directly in the callback of
     * the {@link TransactionalRunner#runAsync(boolean, Function)} call. Here, the exception
     * is forwarded directly rather than wrapped in a {@link CompletionException}. We may
     * want to consider modifying this so that the semantics are the same as if the
     * exception occurs in the callback (that is, always return a future). In that case,
     * the assertions here should more closely mirror {@link #abortsAsyncInChainedFuture()}.
     *
     * @see #abortsAsyncInChainedFuture()
     */
    @Test
    void abortsAsyncDuringRunnable() {
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final RuntimeException cause = new RuntimeException("ABORT");
            final RuntimeException exception = assertThrows(RuntimeException.class, () ->
                    runner.runAsync(false, context -> {
                        context.ensureActive().set(key, value);
                        throw cause;
                    }).join());

            assertEquals(cause, exception);

            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT));
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT));

            assertValue(runner, key, null);
        }
    }

    /**
     * Validate the behavior when an exception is thrown during {@link TransactionalRunner#run(boolean, Function)}.
     * The exception should be forwarded, and the transaction should not be committed.
     */
    @Test
    void abortsSynchronous() {
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final RuntimeException cause = new RuntimeException("ABORT");
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> runner.run(false, context -> {
                        context.ensureActive().set(key, value);
                        throw cause;
                    }));
            assertEquals(cause, exception);

            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT));
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.CLOSE_CONTEXT));

            assertValue(runner, key, null);
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
    void conflictsSynchronous() {
        final Conflicter conflicter = new Conflicter();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            assertConflicts(() -> runner.run(false,
                    context -> {
                        conflicter.readOtherKeySetKey(context).join();
                        return runner.run(false, context1 -> conflicter.readKeySetOtherKey(context1).join());
                    }));
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

    @Test
    void doesNotConflictBeforeUsingTransactionSynchronous() {
        // just like doesNotConflictBeforeUsingTransaction but using synchronous Apis
        final Conflicter conflicter = new Conflicter();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            assertEquals("set key", runner.run(false,
                    context -> {
                        runner.run(false, context1 -> conflicter.readKeySetOtherKey(context1).join());
                        return conflicter.readOtherKeySetKey(context).join();
                    }));

            conflicter.expectValues(runner, conflicter.value, conflicter.otherValue);
        }
    }


    @Test
    void runWithWeakReadSemanticsSynchronous() {
        runWithWeakReadSemantics(
                (runner, clearWeakReadSemantics) ->
                        runner.run(clearWeakReadSemantics, context -> {
                            context.ensureActive().addWriteConflictKey(key);
                            return context.getReadVersion();
                        }),
                (runner, clearWeakReadSemantics) ->
                        runner.run(clearWeakReadSemantics, context -> {
                            // will not conflict if we clear weak read semantics
                            context.ensureActive().addReadConflictKey(key);
                            context.ensureActive().addWriteConflictKey(key);
                            return "ignored";
                        }),
                FDBExceptions.FDBStoreTransactionConflictException.class,
                this::assertConflictException);
    }

    @Test
    void runWithWeakReadSemantics() {
        runWithWeakReadSemantics(
                (runner, clearWeakReadSemantics) -> runner.runAsync(clearWeakReadSemantics, context -> {
                    context.ensureActive().addWriteConflictKey(key);
                    return context.getReadVersionAsync();
                }).join(),
                (runner, clearWeakReadSemantics) -> runner.runAsync(clearWeakReadSemantics, context -> {
                    // will not conflict if we clear weak read semantics
                    context.ensureActive().addReadConflictKey(key);
                    context.ensureActive().addWriteConflictKey(key);
                    return CompletableFuture.completedFuture("ignored");
                }).join(),
                CompletionException.class,
                this::assertConflictException);
    }

    /**
     * Validate that if the user sets {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics}
     * on the runner to something non-trivial, then the cached version is only used the first time.
     * Subsequent attempts, especially attempts that are caused by conflicts, do not want to use the stale read version,
     * as it may see the same old data multiple times (and therefore conflict each time).
     */
    private <T extends Exception> void runWithWeakReadSemantics(
            BiFunction<TransactionalRunner, Boolean, Long> getReadVersionWithWriteConflict,
            BiFunction<TransactionalRunner, Boolean, String> conflicting,
            Class<T> exceptionClass,
            Consumer<T> assertConflicts) {
        final boolean tracksReadVersions = database.isTrackLastSeenVersionOnRead();
        final boolean tracksCommitVersions = database.isTrackLastSeenVersionOnCommit();
        try {
            // Enable version tracking so that the database will use the latest version seen if we have weak read semantics
            database.setTrackLastSeenVersionOnRead(true);
            database.setTrackLastSeenVersionOnCommit(false); // disable commit tracking so that the stale read version is definitely the version remembered
            final byte[] key = Tuple.from(UUID.randomUUID()).pack(); // not actually modified, so value doesn't matter

            // Commit something and cache just the read version
            long firstReadVersion;
            try (TransactionalRunner runner = defaultTransactionalRunner()) {
                firstReadVersion = getReadVersionWithWriteConflict.apply(runner, false);
            }

            FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(
                    firstReadVersion, Long.MAX_VALUE, true);
            final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder()
                    .setWeakReadSemantics(weakReadSemantics);
            try (TransactionalRunner runner = new TransactionalRunner(database, contextConfigBuilder)) {
                assertEquals(firstReadVersion, getReadVersionWithWriteConflict.apply(runner, false));
                final Long newReadVersion = getReadVersionWithWriteConflict.apply(runner, true);
                assertNotEquals(firstReadVersion, newReadVersion);
                assertEquals(newReadVersion, getReadVersionWithWriteConflict.apply(runner, false));

                assertConflicts.accept(assertThrows(exceptionClass, () -> conflicting.apply(runner, false)));

                assertEquals("ignored", conflicting.apply(runner, true));

                assertConflicts.accept(assertThrows(exceptionClass, () -> conflicting.apply(runner, false)));
            }
        } finally {
            database.setTrackLastSeenVersionOnRead(tracksReadVersions);
            database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
        }
    }

    @Test
    void closesContextsSynchronous() {
        //needs to be synchronized, since CompletableFuture.runAsync() will push items into the futures() list in another thread
        final List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());
        try {
            final ForkJoinPool forkJoinPool = new ForkJoinPool(10);
            closesContext((runner, contextFuture, completed) ->
                    // You shouldn't be doing this, but maybe I haven't thought of something similar, but reasonable, where
                    // the executable for `run` does not complete, but the runner is closed
                    CompletableFuture.runAsync(() ->
                            runner.run(false, context -> {
                                final CompletableFuture<Void> future = new CompletableFuture<>();
                                futures.add(future);
                                contextFuture.complete(context);
                                future.join(); // never joins
                                return completed.incrementAndGet();
                            }),
                            forkJoinPool)
            );
        } finally {
            // cleanup the futures, so that the executor used by runAsync doesn't have a bunch of garbage sitting around
            // Note: if you remove this, and change the test to @RepeatedTest(100), after 28 repetitions, it fails
            // consistently.
            futures.forEach(future -> future.complete(null));
        }
    }

    @Test
    void closesContexts() {
        closesContext((runner, contextFuture, completed) ->
                runner.runAsync(false, context -> {
                    contextFuture.complete(context);
                    // the first future will never complete
                    return new CompletableFuture<Void>()
                            .thenApply(vignore -> completed.incrementAndGet());
                }) // DO NOT join
        );
    }

    private <T> void closesContext(TriFunction<TransactionalRunner, CompletableFuture<FDBRecordContext>, AtomicInteger, T> run) {
        List<FDBRecordContext> contexts;
        AtomicInteger completed = new AtomicInteger();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final List<CompletableFuture<FDBRecordContext>> contextFutures = IntStream.range(0, 10).mapToObj(i -> {
                CompletableFuture<FDBRecordContext> contextFuture = new CompletableFuture<>();
                run.apply(runner, contextFuture, completed);
                return contextFuture;
            }).collect(Collectors.toList());
            // make sure that the contexts have been created
            contexts = contextFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
            assertEquals(0, completed.get());
            for (final FDBRecordContext context : contexts) {
                assertFalse(context.isClosed());
            }
        }
        assertEquals(0, completed.get());
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
        assertNull(MDC.get("foobar"));
    }

    @Test
    void synchronousDoesNotChangeMdc() {
        final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder();
        contextConfigBuilder.setMdcContext(Map.of("foobar", "fishes"));
        try (TransactionalRunner runner = new TransactionalRunner(database, contextConfigBuilder)) {
            assertNull(runner.run(false, context -> MDC.get("foobar")));

            contextConfigBuilder.setMdcContext(Map.of("foobar", "boxes"));

            assertNull(runner.run(false, context -> MDC.get("foobar")));
        }
        assertNull(MDC.get("foobar"));
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
            final Function<FDBRecordContext, CompletableFuture<? extends Long>> getReadVersionWithWriteConflict = context -> {
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
                // clearing the weak read semantics should not clear it on the config builder passed in
                assertEquals(weakReadSemantics, contextConfigBuilder.getWeakReadSemantics());
            } finally {
                database.setTrackLastSeenVersionOnRead(tracksReadVersions);
                database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
            }
        }
    }

    @Test
    void runSynchronously() {
        Set<Thread> threads = Collections.synchronizedSet(new HashSet<>());
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final String result = runner.run(false, context -> {
                threads.add(Thread.currentThread());
                context.ensureActive().set(key, value);
                return "boo";
            });
            assertEquals("boo", result);

            assertValue(runner, key, value);
            assertEquals(Set.of(Thread.currentThread()), threads);
        }
    }

    @ParameterizedTest
    @BooleanSource("successful")
    void closesAfterCompletion(boolean success) {
        AtomicReference<FDBRecordContext> contextRef = new AtomicReference<>();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final Exception cause = new Exception("ABORT");
            final CompletableFuture<String> runResult = runner.runAsync(false, context -> {
                context.ensureActive().set(key, value);
                contextRef.set(context);
                CompletableFuture<String> future = new CompletableFuture<>();
                if (success) {
                    future.complete("boo");
                } else {
                    future.completeExceptionally(cause);
                }
                return future;
            });
            final CompletionException exception;
            if (success) {
                assertEquals("boo", runResult.join());
                exception = null;
            } else {
                exception = assertThrows(CompletionException.class, runResult::join);
            }
            assertTrue(contextRef.get().isClosed());

            if (success) {
                assertValue(runner, key, value);
            } else {
                assertEquals(cause, exception.getCause());
                assertValue(runner, key, null);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource("successful")
    void closesAfterCompletionSynchronous(boolean success) throws Exception {
        AtomicReference<FDBRecordContext> contextRef = new AtomicReference<>();
        try (TransactionalRunner runner = defaultTransactionalRunner()) {
            final RuntimeException cause = new RuntimeException("ABORT");
            final Callable<String> callable = () -> runner.run(false, context -> {
                context.ensureActive().set(key, value);
                contextRef.set(context);
                if (success) {
                    return "boo";
                } else {
                    throw cause;
                }
            });
            final RuntimeException exception;
            if (success) {
                assertEquals("boo", callable.call());
                exception = null;
            } else {
                exception = assertThrows(RuntimeException.class, callable::call);
            }
            assertTrue(contextRef.get().isClosed());

            if (success) {
                assertValue(runner, key, value);
            } else {
                assertEquals(cause, exception);
                assertValue(runner, key, null);
            }
        }
    }

    private static void assertValue(final TransactionalRunner runner, final byte[] key, final byte[] value) {
        assertArrayEquals(value, runner.runAsync(false, context -> context.ensureActive().get(key)).join());
    }

    @Nonnull
    private TransactionalRunner defaultTransactionalRunner() {
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTimer(timer)
                .build();
        return new TransactionalRunner(database, config);
    }

    private <T> void assertConflicts(CompletableFuture<T> future) {
        assertConflictException(assertThrows(CompletionException.class, future::join));
    }

    private void assertConflicts(Executable callable) {
        assertConflictException(assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, callable));
    }

    private void assertConflictException(final FDBExceptions.FDBStoreTransactionConflictException exception) {
        assertEquals(FDBError.NOT_COMMITTED.code(), ((FDBException)exception.getCause()).getCode());
    }

    private void assertConflictException(final CompletionException exception) {
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
            assertValue(runner, key, expectedForKey);
            assertValue(runner, otherKey, expectedForOtherKey);
        }
    }
}
