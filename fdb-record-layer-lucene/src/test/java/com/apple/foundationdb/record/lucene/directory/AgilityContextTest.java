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
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for AgilityContext.
 */
@Tag(Tags.RequiresFDB)
class AgilityContextTest extends FDBRecordStoreTestBase {
    int loopCount = 20;
    int threadCount = 8; // if exceed 8, may cause an execution pool deadlock

    void testAgilityContextConcurrentSingleObject(final AgilityContext agilityContext, boolean doFlush) throws ExecutionException, InterruptedException {
        agilityContextTestSingleThread(0, agilityContext, doFlush);
        agilityContextTestSingleThread(1, agilityContext, doFlush);
        agilityContextTestSingleThread(1, agilityContext, doFlush);

        for (int loop = 0; loop < loopCount; loop++) {
            final int loopIndex = loop;
            IntStream.rangeClosed(0, threadCount).parallel().forEach(i -> {
                try {
                    agilityContextTestSingleThread(loopIndex * i, agilityContext, doFlush);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void agilityContextTestSingleThread(int i, AgilityContext agilityContext, boolean explicityFlush) throws ExecutionException, InterruptedException {
        byte[] key = Tuple.from(500, i).pack();
        byte[] val = Tuple.from(i).pack();
        try {
            agilityContext.set(key, val);
            if (explicityFlush && i % 3 == 0) { // keep some un-flushed values
                final byte[] bytes = agilityContext.get(key).get();
                long readVal = Tuple.fromBytes(bytes).getLong(0); // maybe flushed, probably not
                assertEquals(readVal, i);
                agilityContext.flush();
                readVal = Tuple.fromBytes(bytes).getLong(0); // surely flushed
                assertEquals(readVal, i);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertLoopThreadsValues() {
        try (FDBRecordContext context = fdb.openContext()) {
            for (int loop = 0; loop < loopCount; loop++) {
                final AgilityContext agilityContext = AgilityContext.factory(context, false);
                for (int thread = 0; thread < threadCount; thread++) {
                    int i = loop * thread;
                    byte[] key = Tuple.from(500, i).pack();
                    byte[] val = Tuple.from(i).pack();
                    agilityContext.get(key).thenApply(bytes -> {
                        assertEquals(val, bytes);
                        final long existingVal = Tuple.fromBytes(bytes).getLong(0);
                        assertEquals(i, existingVal);
                        return null;
                    });
                }
            }
            context.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAgilityContextConcurrent(boolean useAgile)  throws ExecutionException, InterruptedException {
        try (FDBRecordContext context = fdb.openContext()) {
            final AgilityContext agilityContext = AgilityContext.factory(context, useAgile);
            testAgilityContextConcurrentSingleObject(agilityContext, true);
            context.commit();
        }
        assertLoopThreadsValues();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAgilityContextConcurrentNonExplicitCommits(boolean useAgile) throws ExecutionException, InterruptedException {
        for (int sizeQuota : new int[] {1, 2, 7, 21, 100, 10000}) {
            final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                    .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeQuota);

            try (FDBRecordContext context = openContext(insertProps)) {
                final AgilityContext agilityContext = AgilityContext.factory(context, useAgile);
                testAgilityContextConcurrentSingleObject(agilityContext, false);
                context.commit();
            }
        }
        assertLoopThreadsValues();
    }

    void testAgilityContextOneLongWrite(int loopCount, int sizeLimit, int timeLimit) {
        final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, sizeLimit)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, timeLimit);

        String ShelSilverstein =
                "Oh, if you’re a bird, be an early bird\n" +
                "And catch the worm for your breakfast plate.\n" +
                "If you’re a bird, be an early early bird--\n" +
                "But if you’re a worm, sleep late.";
        try (FDBRecordContext context = openContext(insertProps)) {
            final AgilityContext agilityContext = AgilityContext.factory(context, true);
            for (int i = 0; i < loopCount; i++) {
                byte[] key = Tuple.from(2023, i).pack();
                byte[] val = Tuple.from(i, ShelSilverstein).pack();
                agilityContext.set(key, val);
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext(insertProps)) {
            final AgilityContext agilityContext = AgilityContext.factory(context, false);
            for (int i = 0; i < loopCount; i++) {
                byte[] key = Tuple.from(2023, i).pack();
                byte[] val = Tuple.from(i, ShelSilverstein).pack();
                agilityContext.get(key).thenApply(bytes -> {
                    assertEquals(val, bytes);
                    return null;
                });
            }
        }
    }

    @Test
    void testAgilityContextSizeLimit() {
        testAgilityContextOneLongWrite(73, 100, 100000);
    }

    @Test
    void testAgilityContextTimeLimit() {
        testAgilityContextOneLongWrite(77, 100000, 10);
    }

    void napTime(int napTimeMilliseconds) {
        try {
            Thread.sleep(napTimeMilliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAgilityContextAtomicAttribute(boolean useAgile) {
        final long prefix = 0x1abc1356; // avoid other tests' data
        IntStream.rangeClosed(0, threadCount).parallel().forEach(threadNum -> {
            try (FDBRecordContext context = openContext()) {
                final AgilityContext agilityContext = AgilityContext.factory(context, useAgile);
                for (long i = 1700; i < 1900; i += 17) {
                    final long iFinal = i;
                    if (threadNum == 0) {
                        agilityContext.accept(aContext -> {
                            final Transaction tr = aContext.ensureActive();
                            for (int j = 0; j < 5; j++) {
                                tr.set(Tuple.from(prefix, iFinal, j).pack(),
                                        Tuple.from(iFinal).pack());
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
                context.commit();
            }
        });
    }
}
