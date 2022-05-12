/*
 * TransactionalLimitedRunnerTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.provider.foundationdb.runners.ExponentialDelay;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(Tags.RequiresFDB)
class TransactionalLimitedRunnerTest extends FDBTestBase {

    private FDBDatabase fdb;
    private Tuple prefix;
    // the size of the value is small enough to be inserted, but large enough that it quickly exceeds max transaction
    // size (~100 key/value pairs)
    private final byte[] largeValue = new byte[100_000];

    @BeforeEach
    public void runBefore() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        prefix = Tuple.from(UUID.randomUUID());
    }

    @Test
    void basicTest() {
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(
                fdb, FDBRecordContextConfig.newBuilder().build(), 500, mockDelay())
                .setIncreaseLimitAfter(4)
                .setDecreaseLimitAfter(2)) {
            runner.runAsync(runState ->
                    getStartingKey(runState.getContext())
                            .thenApply(starting -> {
                                long actualLimit = writeMoreData(runState, starting, 1_000);
                                return starting + actualLimit < 1_000;
                            }),
                    List.of("loggingKey", "aConstantValue")).join();
        }
        final List<Long> resultingKeys = new AutoContinuingCursor<>(fdb.newRunner(),
                (context, continuation) ->
                        KeyValueCursor.Builder.withSubspace(new Subspace(prefix))
                                .setContext(context)
                                .setScanProperties(ScanProperties.FORWARD_SCAN)
                                .setContinuation(continuation).build()
                                .map(keyValue -> ((long)Tuple.fromBytes(keyValue.getKey()).get(1))))
                .asList().join();
        assertEquals(LongStream.range(0, 1000).boxed().collect(Collectors.toList()),
                resultingKeys);
    }

    @Test
    void weakReadSemantics() {
        // TODO extract this into something reusable across test fixtures
        final boolean tracksReadVersions = fdb.isTrackLastSeenVersionOnRead();
        final boolean tracksCommitVersions = fdb.isTrackLastSeenVersionOnCommit();
        try {
            // Enable version tracking so that the database will use the latest version seen if we have weak read semantics
            fdb.setTrackLastSeenVersionOnRead(true);
            fdb.setTrackLastSeenVersionOnCommit(false); // disable commit tracking so that the stale read version is definitely the version remembered
            final byte[] conflictingKey = prefix.add(0).pack();
            final Long firstReadVersion = fdb.run(context -> {
                context.ensureActive().addWriteConflictKey(conflictingKey);
                return context.getReadVersion();
            });
            FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(
                    firstReadVersion, Long.MAX_VALUE, true);
            final FDBRecordContextConfig contextConfig = FDBRecordContextConfig.newBuilder()
                    .setWeakReadSemantics(weakReadSemantics)
                    .build();

            AtomicLong newValue = new AtomicLong(1);
            List<Long> readVersions = new ArrayList<>();
            try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(fdb, contextConfig, 20, mockDelay())
                    .setDecreaseLimitAfter(1)) {
                runner.runAsync(runState -> {
                    // will not conflict if we clear weak read semantics
                    runState.getContext().ensureActive().addReadConflictKey(conflictingKey);
                    runState.getContext().ensureActive().addWriteConflictKey(conflictingKey);
                    long v = newValue.getAndIncrement();
                    for (int i = 0; i < runState.getLimit(); i++) {
                        runState.getContext().ensureActive().set(prefix.add(i).pack(), Tuple.from(v).pack());
                    }
                    return runState.getContext().getReadVersionAsync()
                            .thenApply(readVersion -> {
                                readVersions.add(readVersion);
                                return false;
                            });
                }, List.of()).join();
            }
            assertThat(readVersions, Matchers.hasSize(Matchers.greaterThan(1)));
            assertEquals(firstReadVersion, readVersions.get(0));
            final List<Long> resultingValues = fdb.run(context ->
                            KeyValueCursor.Builder.withSubspace(new Subspace(prefix))
                                    .setContext(context)
                                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                                    .build()
                                    .map(keyValue -> ((long)Tuple.fromBytes(keyValue.getValue()).get(0))))
                    .asList().join();
            assertThat(resultingValues, Matchers.hasSize(Matchers.lessThan(20)));
            assertThat(resultingValues, Matchers.everyItem(Matchers.greaterThan(1L)));
        } finally {
            fdb.setTrackLastSeenVersionOnRead(tracksReadVersions);
            fdb.setTrackLastSeenVersionOnCommit(tracksCommitVersions);
        }
    }

    @Test
    void postCommitHookUsage() {
        List<Pair<Long, Long>> attempted = new ArrayList<>();
        List<Pair<Long, Long>> committed = new ArrayList<>();
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(
                fdb, FDBRecordContextConfig.newBuilder().build(), 500, mockDelay())
                .setIncreaseLimitAfter(4)
                .setDecreaseLimitAfter(2)) {
            runner.runAsync(runState ->
                    writeMoreData(runState.getContext(), runState.getLimit(), 1_000)
                            .thenApply(pair -> {
                                attempted.add(pair);
                                runState.getContext().addPostCommit(() -> {
                                    committed.add(pair);
                                    return AsyncUtil.DONE;
                                });
                                return pair.getKey() + pair.getValue() < 1_000;
                            }),
                    List.of("loggingKey", "aConstantValue")).join();
        }
        assertThat(committed, Matchers.hasSize(Matchers.greaterThan(0)));
        assertEquals(committed.stream().sorted().collect(Collectors.toList()),
                committed.stream().sorted().distinct().collect(Collectors.toList()));
        assertThat(attempted,
                Matchers.containsInRelativeOrder(committed.stream().map(Matchers::equalTo).collect(Collectors.toList())));
        assertThat(attempted, Matchers.hasSize(Matchers.greaterThan(committed.size())));
    }

    @Test
    void eachAttemptUsesNewTransaction() {
        // Here we want to make sure that we're using new commits between attempts,
        // in addition to the failed ones that try to write too much, we have a post-commit that will write other data
        // causing the next run to skip some data, as that got written out of band
        List<Pair<Long, Long>> mainTransaction = new ArrayList<>();
        List<Pair<Long, Long>> otherTransaction = new ArrayList<>();
        final int endValue = 1_000;
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(
                fdb, FDBRecordContextConfig.newBuilder().build(), 500, mockDelay())
                .setIncreaseLimitAfter(10)
                .setDecreaseLimitAfter(1);
                FDBDatabaseRunner fdbDatabaseRunner = fdb.newRunner()) {
            runner.runAsync(runState ->
                    writeMoreData(runState.getContext(), runState.getLimit(), endValue)
                            .thenApply(pair -> {
                                runState.getContext().addPostCommit(() -> {
                                    mainTransaction.add(pair);
                                    return fdbDatabaseRunner.runAsync(context ->
                                            writeMoreData(context, 10, endValue).thenApply(otherPair -> {
                                                otherTransaction.add(otherPair);
                                                return null;
                                            }));
                                });
                                return pair.getKey() + pair.getValue() < endValue;
                            }),
                    List.of("loggingKey", "aConstantValue")).join();
        }
        assertThat(otherTransaction, Matchers.hasSize(Matchers.allOf(
                Matchers.greaterThanOrEqualTo(mainTransaction.size() - 1),
                Matchers.lessThanOrEqualTo(mainTransaction.size() + 1))));
        assertFullSequence(mainTransaction, otherTransaction, endValue);
    }

    /**
     * Prep doing something before touching the transaction.
     * <p>
     *     Primarily the need for this is to support time-consuming activities without risking the time-limit.
     *     In order to avoid making these tests take many seconds, instead, before touching the context provided by the
     *     runner, we modify the state, and make sure the transaction picks that up.
     * </p>
     */
    @Test
    void prepBeforeTransaction() {
        List<Pair<Long, Long>> mainTransaction = new ArrayList<>();
        List<Pair<Long, Long>> otherTransaction = new ArrayList<>();
        final int endValue = 1_000;
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(
                fdb, FDBRecordContextConfig.newBuilder().build(), 200, mockDelay())
                .setIncreaseLimitAfter(10)
                .setDecreaseLimitAfter(1);
                FDBDatabaseRunner fdbDatabaseRunner = fdb.newRunner()) {
            runner.runAsync(runState ->
                            fdbDatabaseRunner
                                    .runAsync(context ->
                                            writeMoreData(context, 10, endValue).thenApply(pair -> {
                                                otherTransaction.add(pair);
                                                return null;
                                            }))
                            .thenCompose(vignored ->
                                    writeMoreData(runState.getContext(), runState.getLimit(), endValue)
                                            .thenApply(pair -> {
                                                runState.getContext().addPostCommit(() -> {
                                                    mainTransaction.add(pair);
                                                    return AsyncUtil.DONE;
                                                });
                                                return pair.getKey() + pair.getValue() < endValue;
                                            })),
                    List.of("loggingKey", "aConstantValue")).join();
        }
        assertFullSequence(otherTransaction, mainTransaction, endValue);
    }

    /**
     * Asserts that two lists of pairs alternate, and cover the entire span between 0 and {@code endValue}.
     * <p>
     *     For example:
     *     <code>
     *         first = [(0,10), (17, 8), (33, 7)]
     *         second = [(10, 7), (25, 8)]
     *         endValue = 40
     *     </code>
     * </p>
     * @param firstList first list of pairs of (start, count)
     * @param secondList second list of pairs of (start, count)
     * @param endValue max value covered by both lists
     */
    private void assertFullSequence(@Nonnull final List<Pair<Long, Long>> firstList,
                                    @Nonnull final List<Pair<Long, Long>> secondList,
                                    final int endValue) {
        System.out.println(firstList);
        System.out.println(secondList);
        System.out.println(endValue);

        assertThat(firstList, Matchers.hasSize(Matchers.greaterThan(0)));
        assertThat(secondList, Matchers.hasSize(Matchers.greaterThan(0)));
        assertEquals(0, firstList.get(0).getKey());
        final List<Pair<Long, Long>> all = Stream.concat(firstList.stream(), secondList.stream())
                .sorted(Comparator.comparing(Pair::getKey))
                .collect(Collectors.toList());
        for (int i = 1; i < all.size(); i++) {
            final Pair<Long, Long> previous = all.get(i - 1);
            assertEquals(previous.getKey() + previous.getValue(), all.get(i).getKey());
        }
        final Pair<Long, Long> last = all.get(all.size() - 1);
        assertEquals(endValue, last.getKey() + last.getValue());
    }

    private ExponentialDelay mockDelay() {
        return new ExponentialDelay(3, 10) {
            @Override
            public CompletableFuture<Void> delay() {
                return AsyncUtil.DONE;
            }
        };
    }

    private CompletableFuture<Long> getStartingKey(final FDBRecordContext context) {
        return context.ensureActive().getRange(prefix.range(), 1, true)
                .asList().thenApply(keyValues -> {
                    long starting = 0;
                    if (!keyValues.isEmpty()) {
                        starting = ((long)Tuple.fromBytes(keyValues.get(0).getKey()).get(1)) + 1;
                    }
                    return starting;
                });
    }

    private CompletableFuture<Pair<Long, Long>> writeMoreData(final FDBRecordContext context, final int limit, final int end) {
        return getStartingKey(context).thenApply(starting -> {
            long actualLimit = writeMoreData(starting, limit, end, context);
            return Pair.of(starting, actualLimit);
        });
    }

    private long writeMoreData(final TransactionalLimitedRunner.RunState runState,
                               final long starting, final int end) {
        return writeMoreData(starting, runState.getLimit(), end, runState.getContext());
    }

    private long writeMoreData(final long starting, final int limit, final int end, final FDBRecordContext context) {
        long actualLimit = Math.min(limit, end - starting);
        for (int i = 0; i < actualLimit; i++) {
            context.ensureActive().set(prefix.add(starting + i).pack(), largeValue);
        }
        return actualLimit;
    }

}
