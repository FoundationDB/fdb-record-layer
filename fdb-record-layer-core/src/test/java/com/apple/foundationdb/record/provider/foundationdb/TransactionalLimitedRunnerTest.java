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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(Tags.RequiresFDB)
class TransactionalLimitedRunnerTest extends FDBTestBase {

    private FDBDatabase fdb;
    private Tuple prefix;

    @BeforeEach
    public void runBefore() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        prefix = Tuple.from(UUID.randomUUID());
    }

    @Test
    void basicTest() {
        // the size of the value is small enough to be inserted, but large enough that it quickly exceeds max transaction
        // size (~100 key/value pairs)
        byte[] value = new byte[100_000];
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(
                fdb, FDBRecordContextConfig.newBuilder(), 500, mockDelay())
                .setIncreaseLimitAfter(4)
                .setDecreaseLimitAfter(2)) {
            runner.runAsync(runState ->
                    runState.getContext().ensureActive().getRange(prefix.range(), 1, true)
                            .asList().thenApply(keyValues -> {
                                long starting = 0;
                                if (!keyValues.isEmpty()) {
                                    starting = ((long)Tuple.fromBytes(keyValues.get(0).getKey()).get(1)) + 1;
                                }
                                long l = Math.min(runState.getLimit(), 1_000 - starting);
                                for (int i = 0; i < l; i++) {
                                    runState.getContext().ensureActive().set(prefix.add(starting + i).pack(), value);
                                }
                                System.out.println(starting + " " + runState.getLimit());
                                return starting + l < 1_000;
                            }), List.of("loggingKey", "aConstantValue")).join();
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
            final FDBRecordContextConfig.Builder contextConfigBuilder = FDBRecordContextConfig.newBuilder()
                    .setWeakReadSemantics(weakReadSemantics);

            AtomicLong newValue = new AtomicLong(1);
            List<Long> readVersions = new ArrayList<>();
            try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(fdb, contextConfigBuilder, 20, mockDelay())
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

    private ExponentialDelay mockDelay() {
        return new ExponentialDelay(3, 10) {
            @Override
            public CompletableFuture<Void> delay() {
                return AsyncUtil.DONE;
            }
        };
    }

    @Test
    void postCommitHookUsage() {
        // TODO add a test where the runnable uses a postCommit hook to do something only for success
    }

    @Test
    void eachAttemptUsesNewTransaction() {
        // TODO make sure it's creating a fresh transaction for every attempt
    }

    @Test
    void prepBeforeTransaction() {
        // TODO test of doing something before the transaction starts
        // The primary reason for having a prep method before the transaction starts is to load some sort of buffer
        // without affecting risking hitting the 5 second timeout.
        // To avoid writing a test that depends on that, which would be both slow, and brittle, this test opens another
        // transaction in prep, and does something that would conflict if the other transaction had already been opened
        // TODO I think the `prep` method itself is unnecessary to achieve this because openContext doesn't do anything,
        //      but the prep would have to be done before the first action, or anything that results in getReadVersion,
        //      which is what starts the clock
    }
}
