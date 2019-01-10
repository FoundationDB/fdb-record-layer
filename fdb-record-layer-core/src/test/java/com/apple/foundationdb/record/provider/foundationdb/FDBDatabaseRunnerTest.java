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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseTest.testStoreAndRetrieveSimpleRecord;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link FDBDatabaseRunner}.
 */
@Tag(Tags.RequiresFDB)
public class FDBDatabaseRunnerTest extends FDBTestBase {

    private static final Object[] PATH_OBJECTS = {"record-test", "unit"};

    private FDBDatabase database;

    @BeforeEach
    public void getDatabase() {
        database = FDBDatabaseFactory.instance().getDatabase();
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
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.run(context -> {
                throw new RecordCoreException("Encountered an I/O error", new FDBException("io_error", 1510));
            });
            fail("Did not error on second non-retriable exception");
        } catch (RecordCoreException e) {
            assertEquals("Encountered an I/O error", e.getMessage());
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof FDBException);
            assertEquals("io_error", e.getCause().getMessage());
            assertEquals(1510, ((FDBException)e.getCause()).getCode());
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
                    throw new RecordCoreRetriableTransactionException("Have to try again!", new FDBException("not_committed", 1020));
                } else {
                    return "Success!";
                }
            });
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.run(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new FDBException("not_committed", 1020);
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
            testStoreAndRetrieveSimpleRecord(database, metaData);

            runner.run(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH_OBJECTS))
                        .build();
                store.deleteRecord(Tuple.from(1066L));
                return null;
            });

            FDBStoredRecord<Message> retrieved2 = runner.run(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH_OBJECTS))
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
            try {
                runner.run(context -> {
                    assertTrue(iteration.get() < runner.getMaxAttempts());
                    iteration.incrementAndGet();
                    throw new RecordCoreRetriableTransactionException("Have to try again!", new FDBException("not_committed", 1020));
                });
                fail("Did not catch retriable error that hit maximum retry limit");
            } catch (RecordCoreException e) {
                assertEquals("Have to try again!", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals("not_committed", e.getCause().getMessage());
                assertEquals(1020, ((FDBException)e.getCause()).getCode());
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
        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                throw new RecordCoreException("Encountered an I/O error", new FDBException("io_error", 1510));
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertTrue(e instanceof RecordCoreException);
                assertEquals("Encountered an I/O error", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals("io_error", e.getCause().getMessage());
                assertEquals(1510, ((FDBException)e.getCause()).getCode());
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
                    throw new RecordCoreRetriableTransactionException("Have to try again!", new FDBException("not_committed", 1020));
                } else {
                    return CompletableFuture.completedFuture("Success!");
                }
            }).join();
            assertEquals("Success!", value);
            assertEquals(2, count.get(), "Should only take one try");

            count.set(0);
            value = runner.runAsync(context -> {
                if (count.getAndIncrement() == 0) {
                    throw new FDBException("not_committed", 1020);
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
        testStoreAndRetrieveSimpleRecord(database, metaData);

        try (FDBDatabaseRunner runner = database.newRunner()) {
            runner.runAsync(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH_OBJECTS))
                        .build();
                return store.deleteRecordAsync(Tuple.from(1066L));
            }).join();

            FDBStoredRecord<Message> retrieved2 = runner.runAsync(context -> {
                FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                        .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH_OBJECTS))
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
            runner.runAsync(context -> {
                assertTrue(iteration.get() < runner.getMaxAttempts());
                iteration.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Have to try again!", new FDBException("not_committed", 1020));
            }).handle((ignore, e) -> {
                assertNotNull(e);
                assertEquals("Have to try again!", e.getMessage());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                assertEquals("not_committed", e.getCause().getMessage());
                assertEquals(1020, ((FDBException)e.getCause()).getCode());
                return null;
            }).join();
            assertEquals(runner.getMaxAttempts(), iteration.get());
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
                throw new RecordCoreRetriableTransactionException("Have to try again!", new FDBException("not_committed", 1020));
            });
        }
        int currentIteration = iteration.get();
        assertThat("Should have run at least once", currentIteration, Matchers.greaterThan(0));
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
}
