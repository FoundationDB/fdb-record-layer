/*
 * LoadUnderBatchGrvTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb;

import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.system.SystemKeyspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.SuperSlow;
import com.apple.test.Tags;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test that FDB behaves well when saturated under batch priority.
 */
@Tag(Tags.RequiresFDB)
public class LoadUnderBatchGrvTest {

    private static final Logger logger = LogManager.getLogger(LoadUnderBatchGrvTest.class);
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rsSubspaceExtension = new TestSubspaceExtension(dbExtension);

    enum BatchActivity {
        RegularKey,
        RegularKeyAtSnapshot,
        RegularRange,
        RegularRangeAtSnapshot,
        MetadataVersionKey,
        MetadataVersionKeyAtSnapshot,
    }

    @Test
    @SuperSlow
    void loadTest() {
        AtomicBoolean stopped = new AtomicBoolean();
        final Database database = dbExtension.getDatabase();
        final Subspace subspace = rsSubspaceExtension.getSubspace();
        final byte[] value = new byte[1000];
        ThreadLocalRandom.current().nextBytes(value);
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        final int cachedKeyCount = 1000;
        final Vector<UUID> keys = new Vector<>(cachedKeyCount);
        for (int i = 0; i < cachedKeyCount; i++) {
            keys.add(UUID.randomUUID());
        }
        final ForkJoinPool writePool = new ForkJoinPool(100);
        final ForkJoinPool readPool = new ForkJoinPool(BatchActivity.values().length * 5);
        CompletableFuture<Void> start = new CompletableFuture<>();

        // execute a variety of read operations at batch priority level in background threads, until they fail
        final List<CompletableFuture<Map.Entry<BatchActivity, Exception>>> resultFutures =
                executeReadsAtBatchPriority(stopped, database, subspace, keys, readPool);

        // generate a bunch of load at default priority
        final ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() ->
                CompletableFuture.runAsync(
                        () -> generateLoad(stopped, database, subspace, value, keys, start, writePool),
                        writePool),
                0, 2000, TimeUnit.MILLISECONDS);

        // assert that all the reads were successful, until they failed with "Batch GRV request rate limit exceeded"
        final Map<BatchActivity, Exception> results = AsyncUtil.getAll(resultFutures).join().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertAll(Arrays.stream(BatchActivity.values()).map(batchActivity -> (Executable)() -> {
            final Exception exception = results.get(batchActivity);
            final Supplier<String> message = () -> batchActivity.name() + " - " + exception.getClass() + ": " + exception.getMessage();
            assertNotNull(exception, message);
            assertEquals(ExecutionException.class, exception.getClass(), message);
            assertNotNull(exception.getCause(), message);
            assertEquals(FDBException.class, exception.getCause().getClass(), message);
            assertEquals("Batch GRV request rate limit exceeded", exception.getCause().getMessage(), message);
        }).collect(Collectors.toList()));
        // stop all remaining background work
        stopped.set(true);
        scheduledFuture.cancel(true);
    }

    private static @Nonnull List<CompletableFuture<Map.Entry<BatchActivity, Exception>>> executeReadsAtBatchPriority(
            final AtomicBoolean stopped, final Database database, final Subspace subspace, final Vector<UUID> keys,
            final ForkJoinPool readPool) {
        final List<BatchActivity> batchActivities = new ArrayList<>();
        Collections.addAll(batchActivities, BatchActivity.values());
        Collections.shuffle(batchActivities);
        List<CompletableFuture<Map.Entry<BatchActivity, Exception>>> resultFutures = new ArrayList<>();
        for (final BatchActivity batchActivity : batchActivities) {
            resultFutures.add(CompletableFuture.supplyAsync(() -> {
                while (!stopped.get()) {
                    try (Transaction transaction = database.createTransaction()) {
                        transaction.options().setPriorityBatch();
                        doGet(batchActivity, transaction, subspace, keys)
                                .get(40, TimeUnit.SECONDS);
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        stopped.set(true);
                        return Map.entry(batchActivity, e);
                    } catch (ExecutionException e) {
                        logger.info(batchActivity + " failed", e);
                        return Map.entry(batchActivity, e);
                    } catch (TimeoutException e) {
                        logger.info(batchActivity + " Timed out", e);
                        return Map.entry(batchActivity, e);
                    }
                }
                return Map.entry(batchActivity, new RuntimeException("Nothing thrown"));
            }, readPool));
        }
        return resultFutures;
    }

    private static CompletableFuture<Void> generateLoad(final AtomicBoolean stopped, final Database database, final Subspace subspace,
                                                        final byte[] value, final Vector<UUID> keys,
                                                        final CompletableFuture<Void> start, final Executor writePool) {
        final UUID worker = UUID.randomUUID();
        logger.debug("Starting " + worker);
        return AsyncUtil.whileTrue(() -> {
            final HashSet<UUID> localKeys = new HashSet<>();
            if (database == null) {
                throw new NullPointerException("Database is null");
            }
            return database.runAsync(transaction -> {
                ArrayList<CompletableFuture<byte[]>> futures = new ArrayList<>();
                try {
                    localKeys.clear(); // if the transaction failed and was retried by runAsync
                    for (int i = 0; i < 15; i++) {
                        final UUID key = UUID.randomUUID();
                        localKeys.add(key);
                        transaction.set(subspace.pack(Tuple.from(key)), value);
                    }
                    for (int i = 0; i < 10; i++) {
                        futures.add(transaction.get(subspace.pack(Tuple.from(randomFrom(keys)))));
                    }
                } catch (RuntimeException e) {
                    logger.info("Failed to generate", e);
                }
                return AsyncUtil.whenAll(futures);
            }).thenApply(vignore -> {
                try {
                    localKeys.stream().limit(3)
                            .forEach(key -> setRandom(key, keys));
                    start.complete(null);
                    //logger.debug("Committed " + worker);
                } catch (RuntimeException e) {
                    logger.info("Failed to generate", e);
                }
                return !stopped.get();
            }).exceptionally(e -> {
                logger.info("Failed to commit", e);
                return false;
            });
        }, writePool);
    }

    private static CompletableFuture<byte[]> doGet(final BatchActivity batchActivity, final Transaction transaction,
                                                   final Subspace subspace, final Vector<UUID> keys) {
        switch (batchActivity) {
            case RegularKey:
                return transaction.get(subspace.pack(Tuple.from(randomFrom(keys))));
            case RegularKeyAtSnapshot:
                return transaction.snapshot().get(subspace.pack(Tuple.from(randomFrom(keys))));
            case RegularRange:
                return firstValueOfRandomRange(transaction, subspace, keys);
            case RegularRangeAtSnapshot:
                return firstValueOfRandomRange(transaction.snapshot(), subspace, keys);
            case MetadataVersionKey:
                return transaction.get(SystemKeyspace.METADATA_VERSION_KEY);
            case MetadataVersionKeyAtSnapshot:
                return transaction.snapshot().get(SystemKeyspace.METADATA_VERSION_KEY);
            default:
                throw new RuntimeException("Unexpected batch activity " + batchActivity);
        }
    }

    private static CompletableFuture<byte[]> firstValueOfRandomRange(final ReadTransaction transaction, final Subspace subspace, final Vector<UUID> keys) {
        Tuple key1 = Tuple.from(randomFrom(keys));
        Tuple key2 = Tuple.from(randomFrom(keys));
        AsyncIterator<KeyValue> iterator;
        if (key1.compareTo(key2) > 0) {
            iterator = transaction.getRange(subspace.pack(key2), subspace.pack(key1)).iterator();
        } else {
            iterator = transaction.getRange(subspace.pack(key1), subspace.pack(key2)).iterator();
        }
        return iterator.onHasNext().thenApply(hasNext -> hasNext ? iterator.next().getValue() : null);
    }

    private static UUID randomFrom(final Vector<UUID> keys) {
        return keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
    }

    private static UUID setRandom(final UUID key, final Vector<UUID> keys) {
        return keys.set(ThreadLocalRandom.current().nextInt(keys.size()), key);
    }
}
