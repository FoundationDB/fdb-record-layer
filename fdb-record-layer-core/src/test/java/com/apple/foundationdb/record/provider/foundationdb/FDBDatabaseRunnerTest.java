/*
 * FDBDatabaseRunnerTest.java
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseTest.testStoreAndRetrieveSimpleRecord;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link FDBDatabaseRunner} and {@link FDBDatabaseRunnerImpl}.
 */
@Tag(Tags.RequiresFDB)
public class FDBDatabaseRunnerTest {
    @RegisterExtension
    static final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    FDBDatabase database;
    KeySpacePath path;

    @BeforeEach
    void setUp() {
        database = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    private static FDBException nonRetriableError() {
        return new FDBException(FDBError.IO_ERROR.name(), FDBError.IO_ERROR.code());
    }

    private static FDBException retriableError() {
        return new FDBException(FDBError.NOT_COMMITTED.name(), FDBError.NOT_COMMITTED.code());
    }

    @Test
    public void runNonFDBException() {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.run(context -> {
                throw new IllegalStateException("Cannot run.");
            });
            fail("Did not error on first non-retriable exception");
        } catch (IllegalStateException e) {
            assertEquals("Cannot run.", e.getMessage());
        }
    }

    @Test
    public void runNonRetriableException() {
        final FDBException error = nonRetriableError();
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.run(context -> {
                throw new RecordCoreException("Encountered an I/O error", error);
            });
            fail("Did not error on second non-retriable exception");
        } catch (RecordCoreException e) {
            assertEquals("Encountered an I/O error", e.getMessage());
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof FDBException);
            assertEquals(error.getMessage(), e.getCause().getMessage());
            assertEquals(error.getCode(), ((FDBException)e.getCause()).getCode());
        }

        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.run(context -> {
                throw new RecordCoreException("Internal error");
            });
            fail("Did not catch third non-retriable exception");
        } catch (RecordCoreException e) {
            assertEquals("Internal error", e.getMessage());
            assertNull(e.getCause());
        }
    }

    @Test
    public void runRetryToSuccess() {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            AtomicInteger count = new AtomicInteger(0);
            String value = runner.run(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new RecordCoreRetriableTransactionException("Have to try again!", retriableError());
                } else {
                    return "Success!";
                }
            });
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.run(context -> {
                if (count.getAndIncrement() == 0) {
                    throw retriableError();
                } else {
                    return "Success!";
                }
            });
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.run(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new RecordCoreRetriableTransactionException("Something non-standard");
                } else {
                    return "Success!";
                }
            });
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            value = runner.run(context -> "Success!");
            assertEquals("Success!", value);
        }
    }

    @Test
    public void runDatabaseOperations() {
        // Tests to make sure the database operations are run and committed.

        try (FDBDatabaseRunner runner = database.newRunner()) {
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            testStoreAndRetrieveSimpleRecord(database, metaData, path);

            runner.run(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(path)
                        .build();
                store.deleteRecord(Tuple.from(1066L));
                return null;
            });

            FDBStoredRecord<Message> retrieved2 = runner.run(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(path)
                        .build();
                return store.loadRecord(Tuple.from(1066L));
            });
            assertNull(retrieved2);
        }
    }

    @Test
    public void runRetryNoSuccess() {
        // The rest of the tests retry all of the way, so set guards to make sure they don't take forever.
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.setMaxAttempts(5);
            runner.setMaxDelayMillis(100);
            runner.setInitialDelayMillis(5);

            AtomicInteger iteration = new AtomicInteger(0);
            final FDBException error = retriableError();
            try {
                runner.run(context -> {
                    assertTrue(iteration.get() < runner.getMaxAttempts());
                    iteration.incrementAndGet();
                    throw new RecordCoreRetriableTransactionException("Have to try again!", error);
                });
                fail("Did not catch retriable error that hit maximum retry limit");
            } catch (RecordCoreException e) {
                assertEquals("Have to try again!", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals(error.getMessage(), e.getCause().getMessage());
                assertEquals(error.getCode(), ((FDBException)e.getCause()).getCode());
            }
            assertEquals(runner.getMaxAttempts(), iteration.get());
        }
    }

    @Test
    public void runAsyncNonFDBException() {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                throw new IllegalStateException("Cannot run.");
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertTrue(e instanceof IllegalStateException);
                assertEquals("Cannot run.", e.getMessage());
                return null;
            }).join();
        }
    }

    @Test
    public void runAsyncNonRetriableException() {
        final FDBException error = nonRetriableError();
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                throw new RecordCoreException("Encountered an I/O error", error);
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertTrue(e instanceof RecordCoreException);
                assertEquals("Encountered an I/O error", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals(error.getMessage(), e.getCause().getMessage());
                assertEquals(error.getCode(), ((FDBException)e.getCause()).getCode());
                return null;
            }).join();
        }

        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                throw new RecordCoreException("Internal error");
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertTrue(e instanceof RecordCoreException);
                assertEquals("Internal error", e.getMessage());
                assertNull(e.getCause());
                return null;
            });
        }
    }

    @Test
    public void runAsyncRetryToSuccess() {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            AtomicInteger count = new AtomicInteger(0);
            String value = runner.runAsync(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new RecordCoreRetriableTransactionException("Have to try again!", retriableError());
                } else {
                    return CompletableFuture.completedFuture("Success!");
                }
            }).join();
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.runAsync(context -> {
                if (count.getAndIncrement() == 0) {
                    throw retriableError();
                } else {
                    return CompletableFuture.completedFuture("Success!");
                }
            }).join();
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.runAsync(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new RecordCoreRetriableTransactionException("Something non-standard");
                } else {
                    return CompletableFuture.completedFuture("Success!");
                }
            }).join();
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            value = runner.runAsync(context -> CompletableFuture.completedFuture("Success!")).join();
            assertEquals("Success!", value);
        }
    }

    @Test
    public void runAsyncDatabaseOperations() {
        // Tests to make sure the database operations are run and committed.
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        testStoreAndRetrieveSimpleRecord(database, metaData, path);

        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(path)
                        .build();
                return store.deleteRecordAsync(Tuple.from(1066L));
            }).join();

            FDBStoredRecord<Message> retrieved2 = runner.runAsync(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(path)
                        .build();
                return store.loadRecordAsync(Tuple.from(1066L));
            }).join();
            assertNull(retrieved2);
        }
    }

    @Test
    public void runAsyncRetryNoSuccess() {
        // The rest of the tests retry all of the way, so set guards to make sure they don't take forever.
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.setMaxAttempts(5);
            runner.setMaxDelayMillis(100);
            runner.setInitialDelayMillis(5);

            AtomicInteger iteration = new AtomicInteger(0);
            final FDBException error = retriableError();
            runner.runAsync(context -> {
                assertTrue(iteration.get() < runner.getMaxAttempts());
                iteration.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Have to try again!", error);
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertEquals("Have to try again!", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals(error.getMessage(), e.getCause().getMessage());
                assertEquals(error.getCode(), ((FDBException)e.getCause()).getCode());
                return null;
            }).join();
            assertEquals(runner.getMaxAttempts(), iteration.get());
        }
    }

    /**
     * Validate that if the user sets {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics}
     * on the runner to something non-trivial, then the cached version is only used the first time in the retry loop.
     * Subsequent attempts, especially attempts that are caused by conflicts, do not want to use the stale read version,
     * as it may see the same old data multiple times (and therefore conflict each time).
     */
    @Test
    public void runWithWeakReadSemantics() {
        final boolean tracksReadVersions = database.isTrackLastSeenVersionOnRead();
        final boolean tracksCommitVersions = database.isTrackLastSeenVersionOnCommit();
        try {
            database.setTrackLastSeenVersionOnRead(true);
            database.setTrackLastSeenVersionOnCommit(false); // disable commit tracking so that the stale read version is definitely the version remembered

            final byte[] key = Tuple.from(UUID.randomUUID()).pack(); // not actually modified, so value doesn't matter

            // Commit something and cache just the read version
            long firstReadVersion;
            try (FDBDatabaseRunner runner = database.newRunner()) {
                firstReadVersion = runner.run(context -> {
                    context.ensureActive().addWriteConflictKey(key);
                    return context.getReadVersion();
                });
            }

            // Begin a runner that then uses that cached read version but also conflicts with that transaction
            try (FDBDatabaseRunner runner = database.newRunner()) {
                FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(firstReadVersion, Long.MAX_VALUE, true);
                runner.setWeakReadSemantics(weakReadSemantics);
                runner.setMaxAttempts(3); // just so that if it loops more than twice, the test terminates faster

                final AtomicInteger attempts = new AtomicInteger(0);
                runner.run(context -> {
                    int attempt = attempts.getAndIncrement();
                    if (attempt == 0) {
                        assertEquals(firstReadVersion, context.getReadVersion(), "read version should have used cached version");
                    } else {
                        assertThat("read version should be updated on retry", context.getReadVersion(), greaterThan(firstReadVersion));
                    }
                    context.ensureActive().addReadConflictKey(key); // will cause conflict the first attempt
                    context.ensureActive().addWriteConflictKey(key);

                    return null;
                });
                assertEquals(2, attempts.get());
            }

        } finally {
            database.setTrackLastSeenVersionOnRead(tracksReadVersions);
            database.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
        }
    }

    @Test
    public void stopOnTimeout() {
        AtomicReference<FDBRecordContext> contextRef = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger();
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.setTransactionTimeoutMillis(100L);
            runner.setMaxAttempts(2);
            CompletableFuture<Void> future = runner.runAsync(context -> {
                attempts.incrementAndGet();
                assertEquals(100L, context.getTimeoutMillis());
                contextRef.set(context);

                // Keep continuously getting read versions until it times out.
                // Needs to have the actual FDB transaction in the loop, or it won't stop.
                // Note that FDBRecordContext::getReadVersionAsync caches in Java-land, so is insufficient.
                return AsyncUtil.whileTrue(() -> context.ensureActive().getReadVersion().thenApply(ignore -> true) , context.getExecutor());
            });
            CompletionException err = assertThrows(CompletionException.class, future::join);
            assertNotNull(err.getCause());
            assertThat(err.getCause(), instanceOf(FDBExceptions.FDBStoreTransactionTimeoutException.class));

            assertNotNull(contextRef.get());
            assertTrue(contextRef.get().isClosed(), "transaction should have been closed by runner");
            assertEquals(1, attempts.get());
        }
    }

    @Test
    public void close() throws Exception {
        AtomicInteger iteration = new AtomicInteger(0);
        CompletableFuture<Void> future;
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.setMaxAttempts(Integer.MAX_VALUE);
            runner.setInitialDelayMillis(100);
            runner.setMaxDelayMillis(100);

            future = runner.runAsync(context -> {
                iteration.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Have to try again!", retriableError());
            });
        }
        int currentIteration = iteration.get();
        assertThat("Should have run at least once", currentIteration, greaterThan(0));
        try {
            future.join();
            fail("Should have stopped exceptionally");
        } catch (Exception ex) {
            if (!(ex instanceof FDBDatabaseRunner.RunnerClosed ||
                    (ex instanceof CompletionException && ex.getCause() instanceof FDBDatabaseRunner.RunnerClosed))) {
                throw ex;
            }
        }
        Thread.sleep(150);
        assertEquals(currentIteration, iteration.get(), "Should have stopped running");
    }

    @Test
    void testRestoreMdc() {
        Executor oldExecutor = FDBDatabaseFactory.instance().getExecutor();
        try {
            ThreadContext.clearAll();
            ThreadContext.put("outer", "Echidna");
            final Map<String, String> outer = ThreadContext.getContext();
            final ImmutableMap<String, String> restored = ImmutableMap.of("restored", "Platypus");

            FDBDatabaseFactory.instance().setExecutor(new ContextRestoringExecutor(
                    new ForkJoinPool(2), ImmutableMap.of("executor", "Water Bear")));
            AtomicInteger attempts = new AtomicInteger(0);
            final FDBDatabaseRunner runner = database.newRunner(FDBRecordContextConfig.newBuilder().setMdcContext(restored));
            List<Map<String, String>> threadContexts = new Vector<>();
            Consumer<String> saveThreadContext =
                    name -> threadContexts.add(threadContextPlus(name, attempts.get(), ThreadContext.getContext()));
            final String runnerRunAsyncName = "runner runAsync";
            final String supplyAsyncName = "supplyAsync";
            final String handleName = "handle";

            // Delay starting the future until all callbacks have been set up so that the handle lambda
            // runs in the context-restoring executor.
            CompletableFuture<Void> signal = new CompletableFuture<>();
            CompletableFuture<?> task = runner.runAsync(recordContext -> {
                saveThreadContext.accept(runnerRunAsyncName);
                return signal.thenCompose(vignore -> CompletableFuture.supplyAsync(() -> {
                    saveThreadContext.accept(supplyAsyncName);
                    if (attempts.getAndIncrement() == 0) {
                        throw new RecordCoreRetriableTransactionException("Retriable and lessener",
                                retriableError());
                    } else {
                        return null;
                    }
                }, recordContext.getExecutor()));
            }).handle((result, exception) -> {
                saveThreadContext.accept(handleName);
                return exception;

            });
            signal.complete(null);
            assertNull(task.join());
            List<Map<String, String>> expected = ImmutableList.of(
                    // first attempt:
                    // it is known behavior that the first will be run in the current context
                    threadContextPlus(runnerRunAsyncName, 0, outer),
                    threadContextPlus(supplyAsyncName, 0, restored),
                    // second attempt
                    // the code that creates the future, should now have the correct MDC
                    threadContextPlus(runnerRunAsyncName, 1, restored),
                    threadContextPlus(supplyAsyncName, 1, restored),
                    // handle
                    // this should also have the correct MDC
                    threadContextPlus(handleName, 2, restored));
            assertEquals(expected, threadContexts);
            assertEquals(outer, ThreadContext.getContext());
        } finally {
            FDBDatabaseFactory.instance().setExecutor(oldExecutor);
        }

    }

    private Map<String, String> threadContextPlus(String name, final int attempt, final Map<String, String> threadContext) {
        return ImmutableMap.<String, String>builder()
                .put("loc", name)
                .put("attempt", Integer.toString(attempt))
                .putAll(threadContext).build();
    }
}
