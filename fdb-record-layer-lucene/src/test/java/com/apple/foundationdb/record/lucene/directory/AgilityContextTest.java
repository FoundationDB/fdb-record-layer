/*
 * AgilityContextTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for AgilityContext.
 */
@Tag(Tags.RequiresFDB)
class AgilityContextTest extends FDBRecordStoreTestBase {
    int loopCount = 20;
    int threadCount = 5; // if exceeds a certain size, may cause an execution pool deadlock
    final String prefix = ByteString.copyFrom(RandomUtils.nextBytes(100)).toString();

    private AgilityContext getAgilityContextAgileProp(FDBRecordContext callerContext) {
        final long timeQuotaMillis =
                Objects.requireNonNullElse(callerContext.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA),
                        4000);
        final long sizeQuotaBytes =
                Objects.requireNonNullElse(callerContext.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA),
                        900_000);
        return AgilityContext.agile(callerContext, timeQuotaMillis, sizeQuotaBytes);
    }

    private AgilityContext getAgilityContext(FDBRecordContext callerContext, boolean useAgileContext) {
        return useAgileContext ?
               getAgilityContextAgileProp(callerContext) : AgilityContext.nonAgile(callerContext);
    }

    void testAgilityContextConcurrentSingleObject(final AgilityContext agilityContext, boolean doFlush) throws ExecutionException, InterruptedException {
        agilityContextTestSingleThread(1, 0, agilityContext, doFlush);
        agilityContextTestSingleThread(1, 1, agilityContext, doFlush);
        agilityContextTestSingleThread(1, 1, agilityContext, doFlush);

        for (int loop = 0; loop < loopCount; loop++) {
            final int loopFinal = loop;
            IntStream.rangeClosed(0, threadCount).parallel().forEach(i -> {
                try {
                    agilityContextTestSingleThread(loopFinal,  i, agilityContext, doFlush);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void agilityContextTestSingleThread(int loop, int i, AgilityContext agilityContext, boolean explicityFlush) throws ExecutionException, InterruptedException {
        byte[] key = Tuple.from(500, loop, i).pack();
        byte[] val = Tuple.from(loop, i).pack();

        agilityContext.set(key, val);
        if (explicityFlush && i % 3 == 0) { // keep some un-flushed values
            byte[] bytes = agilityContext.get(key).join(); // maybe flushed, probably not
            Tuple retTuple = Tuple.fromBytes(bytes);
            assertEquals(retTuple.getLong(0), loop);
            assertEquals(retTuple.getLong(1), i);
            agilityContext.flush();
            bytes = agilityContext.get(key).join();
            retTuple = Tuple.fromBytes(bytes);
            assertEquals(retTuple.getLong(0), loop);
            assertEquals(retTuple.getLong(1), i);
        }
    }

    private void assertLoopThreadsValues() {
        try (FDBRecordContext context = fdb.openContext()) {
            for (int loop = 0; loop < loopCount; loop++) {
                final AgilityContext agilityContext = getAgilityContext(context, false);
                for (int i = 0; i < threadCount; i++) {
                    byte[] key = Tuple.from(500, loop, i).pack();
                    final byte[] bytes = agilityContext.get(key).join();
                    final Tuple retTuple = Tuple.fromBytes(bytes);
                    assertEquals(retTuple.getLong(0), loop);
                    assertEquals(retTuple.getLong(1), i);
                }
            }
            context.commit();
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testAgilityContextConcurrent(boolean useAgile)  throws ExecutionException, InterruptedException {
        try (FDBRecordContext context = fdb.openContext()) {
            final AgilityContext agilityContext = getAgilityContext(context, useAgile);
            testAgilityContextConcurrentSingleObject(agilityContext, true);
            context.commit();
        }
        assertLoopThreadsValues();
    }

    @ParameterizedTest
    @BooleanSource
    void testAgilityContextConcurrentNonExplicitCommits(boolean useAgile) throws ExecutionException, InterruptedException {
        for (int sizeQuota : new int[] {1, 2, 7, 21, 100, 10000}) {
            final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                    .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeQuota);

            try (FDBRecordContext context = openContext(insertProps)) {
                final AgilityContext agilityContext = getAgilityContext(context, useAgile);
                testAgilityContextConcurrentSingleObject(agilityContext, false);
                context.commit();
            }
        }
        assertLoopThreadsValues();
    }

    @Test
    void testAgilityContextConcurrentNonExplicitCommitsExplicitParams() throws ExecutionException, InterruptedException {
        for (int sizeQuota : new int[] {1, 2, 7, 21, 100, 10000}) {
            try (FDBRecordContext context = openContext()) {
                final AgilityContext agilityContext = AgilityContext.agile(context, 10000, sizeQuota);
                testAgilityContextConcurrentSingleObject(agilityContext, false);
                context.commit();
            }
        }
        assertLoopThreadsValues();
    }

    private enum Method {
        Set,
        Apply,
        Accept
    }

    private enum LimitType {
        Size(1, 100_000, LuceneEvents.Counts.LUCENE_AGILE_COMMITS_SIZE_QUOTA),
        Time(100_000, 1, LuceneEvents.Counts.LUCENE_AGILE_COMMITS_TIME_QUOTA);

        private final int sizeLimit;
        private final int timeLimit;
        private final StoreTimer.Event timerEvent;

        LimitType(final int sizeLimit, final int timeLimit, final StoreTimer.Event timerEvent) {
            this.sizeLimit = sizeLimit;
            this.timeLimit = timeLimit;
            this.timerEvent = timerEvent;
        }
    }

    static Stream<Arguments> agilityContextLimits() {
        return Stream.of(true, false).flatMap(useProp ->
                Arrays.stream(LimitType.values()).flatMap(limitType ->
                        Arrays.stream(Method.values()).filter(method ->
                                        // AgilityContext is only aware of bytes written when set is called
                                        limitType == LimitType.Time || method == Method.Set)
                                .map(method ->
                                        Arguments.of(useProp, method, limitType))));
    }

    @ParameterizedTest(name = "useProp:{0},{1} by {2}")
    @MethodSource("agilityContextLimits")
    void testAgilityContextOneLongWrite(boolean useProp, Method method, LimitType limitType) {
        int sizeLimit = limitType.sizeLimit;
        int timeLimit = limitType.timeLimit;
        final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeLimit)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, timeLimit);

        String RobertFrost = // (Written in 1916. In the public domain since 2020.)
                "Two roads diverged in a yellow wood,\n" +
                        "And sorry I could not travel both\n" +
                        "And be one traveler, long I stood\n" +
                        "And looked down one as far as I could\n" +
                        "To where it bent in the undergrowth;" ;

        try (FDBRecordContext context = useProp ? openContext(insertProps) : openContext()) {
            final Subspace subspace = path.toSubspace(context);
            final AgilityContext agilityContext =
                    useProp ? getAgilityContextAgileProp(context) : AgilityContext.agile(context, timeLimit, sizeLimit);
            for (int i = 0; i < loopCount; i++) {
                byte[] key = subspace.pack(Tuple.from(2023, i));
                byte[] val = Tuple.from(i, RobertFrost, 0).pack();
                switch (method) {
                    case Set:
                        agilityContext.set(key, val);
                        break;
                    case Apply:
                        agilityContext.apply(innerContext -> innerContext.ensureActive()
                                .get(key).thenApply(oldVal -> {
                                    if (oldVal == null) {
                                        innerContext.ensureActive().set(key, val);
                                    } else {
                                        final Tuple oldTuple = Tuple.fromBytes(oldVal);
                                        innerContext.ensureActive().set(key,
                                                TupleHelpers.subTuple(oldTuple, 0, 2)
                                                        .add(oldTuple.getLong(2) + 1)
                                                        .pack());
                                    }
                                    return oldVal;
                                })).join();
                        break;
                    case Accept:
                        agilityContext.accept(innerContext -> {
                            innerContext.ensureActive().set(key, val);
                        });
                        break;
                    default:
                        throw new AssertionError("Unexpected enum value " + method);
                }
                if (0 == (i % 8)) {
                    napTime(2); // enforce minimal processing time
                }
            }
            context.commit();
            MatcherAssert.assertThat(timer.getCount(limitType.timerEvent), Matchers.greaterThan(0));
        }
        try (FDBRecordContext context = openContext(insertProps)) {
            final Subspace subspace = path.toSubspace(context);
            final AgilityContext agilityContext = getAgilityContext(context, false);
            for (int i = 0; i < loopCount; i++) {
                byte[] key = subspace.pack(Tuple.from(2023, i));
                final byte[] bytes = agilityContext.get(key).join();
                final Tuple retTuple = Tuple.fromBytes(bytes);
                assertEquals(i, retTuple.getLong(0));
                assertEquals(RobertFrost, retTuple.getString(1));
            }
        }
    }

    static Stream<Arguments> agilityContextLimitsNotSet() {
        return Stream.of(true, false).flatMap(useProp ->
                Arrays.stream(LimitType.values()).flatMap(limitType ->
                        Arrays.stream(Method.values())
                                .filter(method -> method != Method.Set) // There is no good way for us to inject failure
                                .map(method -> Arguments.of(useProp, method, limitType))));
    }

    @ParameterizedTest(name = "useProp:{0},{1} by {2}")
    @MethodSource("agilityContextLimitsNotSet")
    void testAgilityContextOneLongWriteFail(boolean useProp, Method method, LimitType limitType) {
        int sizeLimit = limitType.sizeLimit;
        int timeLimit = limitType.timeLimit;
        final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeLimit)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, timeLimit);

        String RobertFrost = // (Written in 1916. In the public domain since 2020.)
                "Two roads diverged in a yellow wood,\n" +
                        "And sorry I could not travel both\n" +
                        "And be one traveler, long I stood\n" +
                        "And looked down one as far as I could\n" +
                        "To where it bent in the undergrowth;" ;

        try (FDBRecordContext context = useProp ? openContext(insertProps) : openContext()) {
            final Subspace subspace = path.toSubspace(context);
            final AgilityContext agilityContext =
                    useProp ? getAgilityContextAgileProp(context) : AgilityContext.agile(context, timeLimit, sizeLimit);
            final byte[] unwritableKey = new byte[] { (byte)0xff };
            for (int i = 0; i < loopCount; i++) {
                byte[] key = subspace.pack(Tuple.from(2023, i));
                byte[] val = Tuple.from(i, RobertFrost, 0).pack();
                switch (method) {
                    case Set:
                        Assertions.assertThrows(FailException.class, () ->
                                agilityContext.set(unwritableKey, val)
                        );
                        break;
                    case Apply:
                        final CompletionException completionException = Assertions.assertThrows(CompletionException.class, () ->
                                agilityContext.apply(innerContext -> innerContext.ensureActive()
                                        .get(key).thenApply(oldVal -> {
                                            if (oldVal == null) {
                                                innerContext.ensureActive().set(key, val);
                                            } else {
                                                final Tuple oldTuple = Tuple.fromBytes(oldVal);
                                                innerContext.ensureActive().set(key,
                                                        TupleHelpers.subTuple(oldTuple, 0, 2)
                                                                .add(oldTuple.getLong(2) + 1)
                                                                .pack());
                                            }
                                            throw new FailException();
                                        })).join()
                        );
                        MatcherAssert.assertThat(completionException.getCause(), Matchers.instanceOf(FailException.class));
                        break;
                    case Accept:
                        Assertions.assertThrows(FailException.class, () ->
                                agilityContext.accept(innerContext -> {
                                    innerContext.ensureActive().set(key, val);
                                    throw new FailException();
                                }));
                        break;
                    default:
                        throw new AssertionError("Unexpected enum value " + method);
                }
                if (0 == (i % 8)) {
                    napTime(2); // enforce minimal processing time
                }
            }
            MatcherAssert.assertThat(timer.getCount(limitType.timerEvent), Matchers.equalTo(0));

            // we shouldn't have committed anything yet, since everything fails
            try (FDBRecordContext validationContext = openContext(insertProps)) {
                for (int i = 0; i < loopCount; i++) {
                    byte[] key = subspace.pack(Tuple.from(2023, i));
                    Assertions.assertNull(validationContext.ensureActive().get(key).join());
                }
            }
            agilityContext.flushAndClose();
        }
        try (FDBRecordContext context = openContext(insertProps)) {
            final Subspace subspace = path.toSubspace(context);
            for (int i = 0; i < loopCount; i++) {
                byte[] key = subspace.pack(Tuple.from(2023, i));
                final byte[] bytes = context.ensureActive().get(key).join();
                final Tuple retTuple = Tuple.fromBytes(bytes);
                assertEquals(i, retTuple.getLong(0));
                assertEquals(RobertFrost, retTuple.getString(1));
            }
        }
    }

    void napTime(int napTimeMilliseconds) {
        try {
            Thread.sleep(napTimeMilliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testAgilityContextAtomicAttribute(boolean useAgile) {
        // assert that commits doesn't happen in he middle of an accept or apply call
        for (int sizeQuota : new int[] {1, 21, 100, 10000}) {
            final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                    .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeQuota);

            IntStream.rangeClosed(0, threadCount).parallel().forEach(threadNum -> {
                try (FDBRecordContext context = openContext(insertProps)) {
                    final AgilityContext agilityContext = getAgilityContext(context, useAgile);
                    for (int i = 1700; i < 1900; i += 17) {
                        final long iFinal = i;
                        // occasionally, we wish to have multiple writers
                        int numWriters = 1 + Integer.numberOfTrailingZeros(i) / 2;
                        if (threadNum < numWriters) {
                            agilityContext.accept(aContext -> {
                                final Transaction tr = aContext.ensureActive();
                                for (int j = 0; j < 5; j++) {
                                    tr.set(Tuple.from(prefix, iFinal, j).pack(),
                                            Tuple.from(iFinal).pack());
                                    napTime(1);
                                }
                            });
                            napTime(3); // give a chance to other threads to run
                        } else {
                            napTime(3);
                            agilityContext.accept(aContext -> {
                                long[] values = new long[5];
                                final Transaction tr = aContext.ensureActive();
                                for (int j = 4; j >= 0; j--) {
                                    byte[] val = tr.get(Tuple.from(prefix, iFinal, j).pack()).join();
                                    values[j] = val == null ? 0 : Tuple.fromBytes(val).getLong(0);
                                }
                                for (int j = 1; j < 5; j++) {
                                    assertEquals(values[0], values[j]);
                                }
                            });
                        }
                    }
                    agilityContext.flush();
                }
            });
        }
    }


    @ParameterizedTest
    @BooleanSource
    void testAgilityContextAtomicAttributeMultiContext(boolean useAgile) {
        // assert that commits doesn't happen in he middle of an accept or apply call
        IntStream.rangeClosed(0, threadCount).parallel().forEach(threadNum -> {
            for (int i = 1700; i < 1900; i += 17) {
                final long iFinal = i;
                // occasionally, we wish to have multiple writers
                int numWriters = 1; // in this test, two writes may conflict each other
                if (threadNum < numWriters) {
                    try (FDBRecordContext context = openContext()) {
                        final AgilityContext agilityContext = getAgilityContext(context, useAgile);
                        agilityContext.accept(aContext -> {
                            for (int j = 0; j < 5; j++) {
                                final Transaction tr = aContext.ensureActive();
                                tr.set(Tuple.from(prefix, iFinal, j).pack(),
                                        Tuple.from(iFinal).pack());
                                napTime(1);
                            }
                            napTime(3); // give a chance to other threads to run
                        });
                        agilityContext.flush();
                    }
                } else {
                    napTime(3);
                    try (FDBRecordContext context = openContext()) {
                        final AgilityContext agilityContext = getAgilityContext(context, useAgile);
                        agilityContext.accept(aContext -> {
                            long[] values = new long[5];
                            final Transaction tr = aContext.ensureActive();
                            for (int j = 4; j >= 0; j--) {
                                byte[] val = tr.get(Tuple.from(prefix, iFinal, j).pack()).join();
                                values[j] = val == null ? 0 : Tuple.fromBytes(val).getLong(0);
                            }
                            for (int j = 1; j < 5; j++) {
                                assertEquals(values[0], values[j]);
                            }
                        });
                        agilityContext.flush();
                    }
                }
            }
        });
    }

    @SuppressWarnings("serial")
    private static class FailException extends RuntimeException {

    }
}
