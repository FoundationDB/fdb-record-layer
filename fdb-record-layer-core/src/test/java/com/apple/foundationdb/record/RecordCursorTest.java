/*
 * RecordCursorTest.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.cursors.FilterCursor;
import com.apple.foundationdb.record.cursors.FirableCursor;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.cursors.MapResultCursor;
import com.apple.foundationdb.record.cursors.RowLimitedCursor;
import com.apple.foundationdb.record.cursors.SkipCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.test.BooleanSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link RecordCursor}.
 */
public class RecordCursorTest {

    static final Executor EXECUTOR = TestExecutors.defaultThreadPool();
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordCursorTest.class);

    Timer timer;

    @BeforeEach
    void setup() {
        timer = new Timer("RecordCursorTest");
    }

    protected <T> CompletableFuture<T> delayedFuture(T value, int delay) {
        CompletableFuture<T> future = new CompletableFuture<T>();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                future.complete(value);
            }
        }, delay);
        return future;
    }

    protected class AsyncCountdown implements RecordCursor<Integer> {
        int count;
        int onNextCalled;
        boolean closed;

        public AsyncCountdown(int count) {
            this.count = count;
            closed = false;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<Integer>> onNext() {
            onNextCalled++;
            return CompletableFuture.completedFuture(count).thenApplyAsync(c -> {
                count--;
                if (c > 0) {
                    return RecordCursorResult.withNextValue(c, ByteArrayContinuation.fromInt(count));
                } else {
                    return RecordCursorResult.withoutNextValue(RecordCursorEndContinuation.END, NoNextReason.SOURCE_EXHAUSTED);
                }
            }, getExecutor());
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return EXECUTOR;
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }
    }

    @Test
    void mapPipelinedReuseTest() {
        AsyncCountdown cursor = new AsyncCountdown(100);
        RecordCursor<Integer> map = cursor.mapPipelined(i -> delayedFuture(i, 10), 10);
        assertEquals(IntStream.range(0, 100).mapToObj(i -> 100 - i).collect(Collectors.toList()), map.asList().join());
        assertThat(cursor.onNextCalled, Matchers.lessThanOrEqualTo(102));
    }

    @Test
    void forEachAsyncTest() {
        RecordCursor<Integer> cursor = RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        long start = System.currentTimeMillis();
        cursor.forEachAsync(i -> MoreAsyncUtil.delayedFuture(10L, TimeUnit.MILLISECONDS), 2).join();
        long end = System.currentTimeMillis();
        assertThat(end - start, Matchers.greaterThanOrEqualTo(40L));

        AtomicInteger integer = new AtomicInteger(0);
        cursor = RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        start = System.currentTimeMillis();
        cursor.forEachAsync(i -> MoreAsyncUtil.delayedFuture(5L, TimeUnit.MILLISECONDS).thenAccept(vignore -> {
            // Purposefully not trying to be conscious of contention.
            int val = integer.get();
            val += i;
            integer.set(val);
        }), 1).join();
        end = System.currentTimeMillis();
        assertEquals(integer.get(), 36);
        assertThat(end - start, Matchers.greaterThanOrEqualTo(40L));

        // It should be fine if they all complete immediately.
        integer.set(0);
        cursor = RecordCursor.fromIterator(IntStream.range(1, 10001).iterator());
        cursor.forEachAsync(i -> {
            int val = integer.get();
            val += i;
            integer.set(val);
            return AsyncUtil.DONE;
        }, 1).join();
        assertEquals(50005000, integer.get());
    }

    @Test
    void orElseTest() {
        List<Integer> ints = Arrays.asList(1, 2, 3);
        BiFunction<Executor, byte[], RecordCursor<Integer>> elseZero = (executor, cont) -> RecordCursor.fromFuture(executor, CompletableFuture.completedFuture(0), cont);
        assertEquals(ints, RecordCursor.fromList(ints).asList().join());
        assertEquals(ints, RecordCursor.orElse(cont -> RecordCursor.fromList(ints, cont), elseZero, null).asList().join());

        assertEquals(Arrays.asList(0), RecordCursor.orElse(
                cont -> RecordCursor.fromList(Collections.<Integer>emptyList(), cont), elseZero, null).asList().join());
    }

    //@Test @Slow
    // Will get either NPE or NoSuchElementException after a while.
    void orElseTimingErrorTest() {
        BiFunction<Executor, byte[], RecordCursor<Integer>> elseZero = (executor, cont) -> RecordCursor.fromFuture(executor, CompletableFuture.completedFuture(0), cont);
        for (int i = 0; i < 100000; i++) {
            RecordCursorIterator<Integer> cursor = RecordCursor.orElse(cont -> RecordCursor.fromList(Collections.<Integer>emptyList(), cont), elseZero, null).asIterator();
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                futures.add(cursor.onHasNext());
            }
            AsyncUtil.whenAny(futures).thenApply(vignore -> {
                assertEquals(Integer.valueOf(0), cursor.next());
                return null;
            }).join();
        }
    }

    @Test
    void asListWithContinuationTest() throws Exception {
        final List<Integer> ints = IntStream.range(0, 50).boxed().collect(Collectors.toList());

        final AtomicReference<RecordCursorResult<Integer>> finalResult = new AtomicReference<>();

        int iterations = 0;
        byte[] continuation = null;
        do {
            ++iterations;
            List<Integer> values = RecordCursor.fromList(ints, continuation).limitRowsTo(10).asList(finalResult).get();
            if (values.size() > 0) {
                assertEquals(values.size(), 10);
                assertEquals(values.get(0), (iterations - 1) * 10);
                assertTrue(finalResult.get().getNoNextReason().isLimitReached());
            }
            continuation = finalResult.get().getContinuation().toBytes();
        } while (continuation != null);

        assertEquals(finalResult.get().getNoNextReason(), RecordCursor.NoNextReason.SOURCE_EXHAUSTED);

        assertEquals(iterations, 6); // 5 with data, 6th to return final EOF
    }

    @Test
    void limitTest() {
        List<Integer> ints = Arrays.asList(1, 2, 3, 4);

        // Make sure that if the limit is less than the size, we get the thing suppose.
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).limitRowsTo(3);
        assertTrue(cursor instanceof RowLimitedCursor, "Setting limit should create a LimitCursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1, 2, 3), newInts);

        // Make sure that if the limit is greater than the size, we get everything.
        cursor = RecordCursor.fromList(ints).limitRowsTo(5);
        assertTrue(cursor instanceof RowLimitedCursor, "Setting limit should create a LimitCursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1, 2, 3, 4), newInts);

        cursor = RecordCursor.fromList(ints).limitRowsTo(Integer.MAX_VALUE);
        assertFalse(cursor instanceof RowLimitedCursor, "Setting max limit shouldn't actually create a LimitCursor");
    }

    @Test
    void skipTest() {
        List<Integer> ints = Arrays.asList(1, 2, 3, 4);

        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).skip(2);
        assertTrue(cursor instanceof SkipCursor, "Setting skip should create a SkipCursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(3, 4), newInts);

        cursor = RecordCursor.fromList(ints).skip(0);
        assertFalse(cursor instanceof SkipCursor, "Setting skip 0 shouldn't actually create a SkipCursor");
    }

    @Test
    void filterTest() {
        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).filter(i -> i % 2 == 0);
        assertTrue(cursor instanceof FilterCursor, "Creating a filter should create a filter cursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(2, 4, 6), newInts);

        cursor = RecordCursor.fromList(ints).filterAsync(i -> CompletableFuture.completedFuture(i % 2 != 0), 1);
        assertTrue(cursor instanceof MapResultCursor, "Creating an async filter should create a map cursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1, 3, 5, 7), newInts);

        ints = Arrays.asList(1, 2, 3, null, 4, 5, 6, 7);

        cursor = RecordCursor.fromList(ints).filter(i -> {
            if (i == null) {
                return null;
            } else {
                return i % 2 != 0;
            }
        });
        assertTrue(cursor instanceof FilterCursor, "Creating a filter should create a filter cursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1, 3, 5, 7), newInts);

        cursor = RecordCursor.fromList(ints).filterAsync(i -> {
            if (i == null) {
                return CompletableFuture.completedFuture(null);
            } else {
                return CompletableFuture.completedFuture(i % 2 == 0);
            }
        }, 1);
        newInts = cursor.asList().join();
        assertTrue(cursor instanceof MapResultCursor, "Creating an async filter should create a map cursor");
        assertEquals(Arrays.asList(2, 4, 6), newInts);
    }

    private static class PrefixAddingContinuationConvertor implements RecordCursor.ContinuationConvertor {
        private final ByteString prefix;

        private PrefixAddingContinuationConvertor(ByteString prefix) {
            this.prefix = prefix;
        }

        @Nullable
        @Override
        public byte[] unwrapContinuation(@Nullable final byte[] continuation) {
            if (continuation == null) {
                return null;
            }
            ByteString wrappedBytes = ByteString.copyFrom(continuation);
            assertTrue(wrappedBytes.startsWith(prefix), "continuation should begin with expected prefix");
            return wrappedBytes.substring(prefix.size()).toByteArray();
        }

        @Override
        public RecordCursorContinuation wrapContinuation(@Nonnull final RecordCursorContinuation continuation) {
            if (continuation.isEnd()) {
                return RecordCursorEndContinuation.END;
            }
            return new RecordCursorContinuation() {
                @Nonnull
                @Override
                public ByteString toByteString() {
                    return prefix.concat(continuation.toByteString());
                }

                @Nullable
                @Override
                public byte[] toBytes() {
                    return toByteString().toByteArray();
                }

                @Override
                public boolean isEnd() {
                    return false;
                }
            };
        }
    }

    @Test
    void mapContinuationsTest() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6);
        final ByteString prefix = ByteString.copyFromUtf8("prefix+");
        final PrefixAddingContinuationConvertor continuationConvertor = new PrefixAddingContinuationConvertor(prefix);

        RecordCursor<Integer> cursor = RecordCursor.mapContinuation(continuation -> RecordCursor.fromList(ints, continuation), continuationConvertor, null);

        List<Integer> soFar = new ArrayList<>();
        RecordCursorResult<Integer> result;
        do {
            result = cursor.getNext();
            if (result.getContinuation().isEnd()) {
                assertEquals(ByteString.EMPTY, result.getContinuation().toByteString());
                assertNull(result.getContinuation().toBytes());
            } else {
                soFar.add(result.get());

                // Modified continuation should begin with the prefix
                assertTrue(result.getContinuation().toByteString().startsWith(prefix));

                // Stripping away the prefix and resuming the cursor should produce the rest of the list
                byte[] continuation = result.getContinuation().toBytes();
                assertNotNull(continuation);
                RecordCursor<Integer> tailCursor = RecordCursor.mapContinuation(innerContinuation -> RecordCursor.fromList(ints, innerContinuation), continuationConvertor, continuation);
                final List<Integer> resultList = new ArrayList<>(soFar);
                tailCursor.forEach(resultList::add).join();
                assertEquals(ints, resultList);
            }
        } while (result.hasNext());
    }

    @Test
    void firstTest() throws Exception {
        List<Integer> ints = Arrays.asList(1, 2, 3, 4);
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints);
        assertEquals(Optional.of(1), cursor.first().get());

        List<Integer> emptyInts = Collections.emptyList();
        cursor = RecordCursor.fromList(emptyInts);
        assertEquals(Optional.empty(), cursor.first().get());
    }

    @Test
    void pipelineContinuationTest() throws Exception {
        List<Integer> ints = Lists.newArrayList(1, 2, 3, 4, 5);
        List<Integer> expected = ints.stream().flatMap(o -> ints.stream().map(i -> o * 100 + i)).collect(Collectors.toList());

        Function<byte[], RecordCursor<Integer>> outerFunc = cont -> RecordCursor.fromList(ints, cont);
        BiFunction<Integer, byte[], RecordCursor<Integer>> innerFunc = (outer, cont) -> RecordCursor.fromList(ints, cont)
                .map(inner -> outer.intValue() * 100 + inner.intValue());
        assertEquals(expected, RecordCursor.flatMapPipelined(outerFunc, innerFunc, null, 1).asList().join());

        List<Integer> pieces = new ArrayList<>();
        byte[] continuation = null;
        do {
            // Keep stopping and restarting every 3 items.
            int limit = 3;
            RecordCursorIterator<Integer> cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 7).asIterator();
            while (cursor.hasNext()) {
                pieces.add(cursor.next());
                if (--limit <= 0) {
                    break;
                }
            }
            continuation = cursor.getContinuation();
        } while (continuation != null);
        assertEquals(expected, pieces);

        // Given a "record" (an integer), return its "primary key": that is, something to uniquely identify it, its serialized value.
        Function<Integer, byte[]> checkFunc = i -> ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();

        // When the item we were on is removed, we skip to the next.
        pieces.clear();
        continuation = null;
        RecordCursor<Integer> partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, checkFunc, continuation, 7).limitRowsTo(12);
        pieces.addAll(partCursor.asList().get());
        continuation = partCursor.getNext().getContinuation().toBytes();
        ints.remove(2); // The 3, of which we've done 301, 302
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, checkFunc, continuation, 7);
        pieces.addAll(partCursor.asList().get());
        List<Integer> adjusted = new ArrayList<>(expected);
        // Everything after where we restarted that involves the removed item (3).
        adjusted.removeIf(i -> i > 302 && ((i / 100) % 10 == 3 || i % 10 == 3));
        assertEquals(adjusted, pieces);

        // When an item is added right at the one we were on, we do it all first and then the rest of the next one.
        ints.add(2, 3);
        pieces.clear();
        continuation = null;
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, checkFunc, continuation, 7).limitRowsTo(12);
        pieces.addAll(partCursor.asList().get());
        continuation = partCursor.getNext().getContinuation().toBytes();
        ints.add(2, 22); // Before the 3, of which we've done 301, 302
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, checkFunc, continuation, 7);
        pieces.addAll(partCursor.asList().get());
        adjusted = new ArrayList<>();
        // Before we stopped.
        adjusted.addAll(expected.subList(0, 12));
        // All of the new outer item's.
        adjusted.addAll(ints.stream().map(i -> 2200 + i).collect(Collectors.toList()));
        // Then the new one (22) interleaved between 2 and 3 for each of the outer.
        adjusted.add(322);
        adjusted.addAll(expected.subList(12, 17));
        adjusted.add(422);
        adjusted.addAll(expected.subList(17, 22));
        adjusted.add(522);
        adjusted.addAll(expected.subList(22, expected.size()));
        assertEquals(adjusted, pieces);
    }

    private int iterateGrid(@Nonnull Function<byte[], RecordCursor<Pair<Integer, Integer>>> cursorFunction,
                            @Nonnull RecordCursor.NoNextReason[] possibleNoNextReasons) {
        int results = 0;
        int leftSoFar = -1;
        int rightSoFar = -1;
        boolean done = false;
        byte[] continuation = null;
        while (!done) {
            RecordCursorIterator<Pair<Integer, Integer>> cursor = cursorFunction.apply(continuation).asIterator();
            while (cursor.hasNext()) {
                Pair<Integer, Integer> value = cursor.next();
                assertNotNull(value);
                assertThat(value.getLeft(), greaterThan(value.getRight()));
                assertThat(value.getLeft(), greaterThanOrEqualTo(leftSoFar));
                if (value.getLeft() == leftSoFar) {
                    assertThat(value.getRight(), greaterThan(rightSoFar));
                    rightSoFar = value.getRight();
                } else {
                    leftSoFar = value.getLeft();
                    rightSoFar = value.getRight();
                }
                results++;
            }
            assertThat(cursor.getNoNextReason(), is(oneOf(possibleNoNextReasons)));
            continuation = cursor.getContinuation();
            if (cursor.getNoNextReason().isSourceExhausted()) {
                assertNull(continuation);
                done = true;
            } else {
                assertNotNull(continuation);
            }
        }
        return results;
    }

    @ParameterizedTest(name = "pipelineWithInnerLimits [outOfBand = {0}]")
    @BooleanSource
    void pipelineWithInnerLimits(boolean outOfBand) {
        final RecordCursor.NoNextReason[] possibleNoNextReasons = new RecordCursor.NoNextReason[] {
                RecordCursor.NoNextReason.SOURCE_EXHAUSTED,
                outOfBand ? RecordCursor.NoNextReason.TIME_LIMIT_REACHED : RecordCursor.NoNextReason.RETURN_LIMIT_REACHED
        };
        final List<Integer> ints = IntStream.range(0, 10).boxed().collect(Collectors.toList());

        final FDBStoreTimer timer = new FDBStoreTimer();
        final BiFunction<Integer, byte[], RecordCursor<Pair<Integer, Integer>>> innerFunc = (x, continuation) -> {
            final RecordCursor<Integer> intCursor = RecordCursor.fromList(ints, continuation);
            final RecordCursor<Integer> limitedCursor;
            if (outOfBand) {
                limitedCursor = new FakeOutOfBandCursor<>(intCursor, 3);
            } else {
                limitedCursor = intCursor.limitRowsTo(3);
            }
            return limitedCursor
                    .filterInstrumented(y -> y < x, timer, FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Events.QUERY_FILTER, FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_DISCARDED)
                    .map(y -> Pair.of(x, y));
        };
        final Function<byte[], RecordCursor<Integer>> outerFunc = continuation -> RecordCursor.fromList(ints, continuation);

        int results = iterateGrid(continuation -> RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 5), possibleNoNextReasons);
        int expectedResults = ints.size() * (ints.size() - 1) / 2;
        assertEquals(expectedResults, results);
        // assertEquals(ints.size() * ints.size(), timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN));
        // assertEquals(expectedResults, timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_PASSED));
        assertEquals(ints.size() * ints.size() - expectedResults, timer.getCount(FDBStoreTimer.Counts.QUERY_DISCARDED));
    }

    @ParameterizedTest(name = "pipelineWithOuterLimits [outOfBand = {0}]")
    @BooleanSource
    void pipelineWithOuterLimits(boolean outOfBand) {
        final RecordCursor.NoNextReason[] possibleNoNextReasons = new RecordCursor.NoNextReason[] {
                RecordCursor.NoNextReason.SOURCE_EXHAUSTED,
                outOfBand ? RecordCursor.NoNextReason.TIME_LIMIT_REACHED : RecordCursor.NoNextReason.RETURN_LIMIT_REACHED
        };
        final List<Integer> ints = IntStream.range(0, 10).boxed().collect(Collectors.toList());

        final FDBStoreTimer timer = new FDBStoreTimer();
        final BiFunction<Integer, byte[], RecordCursor<Pair<Integer, Integer>>> innerFunc = (x, continuation) -> {
            final RecordCursor<Integer> intCursor = RecordCursor.fromList(ints, continuation);
            final RecordCursor<Integer> limitedCursor;
            if (outOfBand) {
                limitedCursor = new FakeOutOfBandCursor<>(intCursor, 3);
            } else {
                limitedCursor = intCursor.limitRowsTo(3);
            }
            return limitedCursor.filter(y -> y < x).map(y -> Pair.of(x, y));
        };
        final Function<byte[], RecordCursor<Integer>> outerFunc = continuation -> {
            final RecordCursor<Integer> intCursor = RecordCursor.fromList(ints, continuation);
            final RecordCursor<Integer> limitedCursor;
            if (outOfBand) {
                limitedCursor = new FakeOutOfBandCursor<>(intCursor, 3);
            } else {
                limitedCursor = intCursor.limitRowsTo(3);
            }
            return limitedCursor.filterInstrumented(x -> x >= 7 && x < 9, timer, FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Events.QUERY_FILTER, FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_DISCARDED);
        };

        int results = iterateGrid(continuation -> RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 5), possibleNoNextReasons);
        assertEquals(15, results);

        // Note that as only the outer filter is instrumented, these assertions are based on only the outer filter.
        // Should be:
        //  Itr 1: 0, 1, 2
        //  Itr 2: 3, 4, 5
        //  Itr 3: 6, 7 (0, 1, 2), 8
        //  Itr 4: 7 (3, 4, 5), 8, 9
        //  Itr 5: 7 (6, 7, 8), 8, 9
        //  Itr 6: 7 (9), 8 (0, 1, 2), 9
        //  Itr 7: 8 (3, 4, 5), 9
        //  Itr 8: 8 (6, 7, 8), 9
        //  Itr 9: 8 (9), 9
        assertEquals(24, timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN));
        assertEquals(11, timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_PASSED));
        assertEquals(13, timer.getCount(FDBStoreTimer.Counts.QUERY_DISCARDED));
    }

    @ParameterizedTest(name = "pipelineWithOuterLimitsWithSomeDelay [outOfBand = {0}]")
    @BooleanSource
    void pipelineWithOuterLimitsWithSomeDelay(boolean outOfBand) {
        final RecordCursor.NoNextReason limitReason = outOfBand ? RecordCursor.NoNextReason.TIME_LIMIT_REACHED : RecordCursor.NoNextReason.RETURN_LIMIT_REACHED;
        final List<Integer> ints = IntStream.range(0, 10).boxed().collect(Collectors.toList());

        final BiFunction<Integer, byte[], RecordCursor<Pair<Integer, Integer>>> innerFunc = (x, continuation) -> RecordCursor.fromList(ints, continuation).map(y -> Pair.of(x, y));
        final AtomicReference<FirableCursor<Integer>> outerCursorRef = new AtomicReference<>();
        final Function<byte[], RecordCursor<Integer>> outerFunc = continuation -> {
            final RecordCursor<Integer> intCursor = RecordCursor.fromList(ints, continuation);
            final RecordCursor<Integer> limitedCursor;
            if (outOfBand) {
                limitedCursor = new FakeOutOfBandCursor<>(intCursor, 3);
            } else {
                limitedCursor = intCursor.limitRowsTo(3);
            }
            final FirableCursor<Integer> outerCursor = new FirableCursor<>(limitedCursor.filter(x -> x > 7));
            outerCursorRef.set(outerCursor);
            return outerCursor;
        };

        // Outer cursor = 0, 1, 2, all filtered
        RecordCursorIterator<Pair<Integer, Integer>> cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, null, 5).asIterator();
        assertThat(cursor.onHasNext().isDone(), is(false));
        outerCursorRef.get().fire();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(limitReason, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        byte[] continuation = cursor.getContinuation();

        // Outer cursor = 3, 4, 5, all filtered
        cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 5).asIterator();
        assertThat(cursor.onHasNext().isDone(), is(false));
        outerCursorRef.get().fire();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(limitReason, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        continuation = cursor.getContinuation();

        // Outer cursor = 6 (filtered), 7 (filtered), 8 (not filtered)
        cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 5).asIterator();
        outerCursorRef.get().fire();
        for (int i = 0; i < ints.size(); i++) {
            Pair<Integer, Integer> nextValue = cursor.next();
            assertEquals(8, (int)nextValue.getLeft());
            assertEquals(i, (int)nextValue.getRight());
        }
        assertThat(cursor.onHasNext().isDone(), is(false));
        outerCursorRef.get().fire();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(limitReason, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        continuation = cursor.getContinuation();

        // Outer cursor = 9 (not filtered)
        cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc, continuation, 5).asIterator();
        outerCursorRef.get().fire();
        for (int i = 0; i < ints.size(); i++) {
            Pair<Integer, Integer> nextValue = cursor.next();
            assertEquals(9, (int)nextValue.getLeft());
            assertEquals(i, (int)nextValue.getRight());
        }
        assertThat(cursor.onHasNext().isDone(), is(false));
        outerCursorRef.get().fire();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    void flatMapPipelineErrorPropagation() throws ExecutionException, InterruptedException {
        FirableCursor<String> firableCursor1 = new FirableCursor<>(RecordCursor.fromList(Collections.singletonList("hello")));
        FirableCursor<String> firableCursor2 = new FirableCursor<>(new BrokenCursor());
        List<FirableCursor<String>> firableCursors = Arrays.asList(firableCursor1, firableCursor2);
        FirableCursor<Integer> outerCursor = new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)));
        RecordCursor<String> cursor = RecordCursor.flatMapPipelined(cont -> outerCursor,
                (a, cont) -> firableCursors.get(a), null, 10);
        outerCursor.fire();
        firableCursor1.fire();
        RecordCursorResult<String> cursorResult = cursor.onNext().get();
        assertTrue(cursorResult.hasNext());
        assertEquals("hello", cursorResult.get());
        CompletableFuture<RecordCursorResult<String>> nextResultFuture = cursor.onNext();
        firableCursor1.fire();
        assertFalse(nextResultFuture.isDone());
        outerCursor.fire();
        firableCursor2.fire();
        ExecutionException e = assertThrows(ExecutionException.class, nextResultFuture::get);
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertEquals("sorry", e.getCause().getMessage());

        RecordCursor<Integer> outerCursorError = RecordCursor.flatMapPipelined(cont -> new BrokenCursor(),
                (s, cont) -> RecordCursor.fromList(Collections.singletonList(s.length())), null, 10);
        e = assertThrows(ExecutionException.class, () -> outerCursorError.onNext().get());
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertEquals("sorry", e.getCause().getMessage());
    }

    /**
     * Test that when the outer cursor and an inner future complete "at the same time" (as close as we can) that the
     * error is propagated.
     *
     * @throws ExecutionException from futures joined in the test
     * @throws InterruptedException from futures joined in the test
     */
    @Test
    void mapPipelinedErrorAtConcurrentCompletion() throws ExecutionException, InterruptedException {
        final RuntimeException runtimeEx = new RuntimeException("some random exception");

        List<CompletableFuture<Integer>> futures = Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        FirableCursor<Integer> firableCursor = new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1, 2)));
        RecordCursor<Integer> cursor = firableCursor.mapPipelined(futures::get, 2);
        CompletableFuture<RecordCursorResult<Integer>> resultFuture = cursor.onNext();
        assertFalse(resultFuture.isDone());
        firableCursor.fire();
        assertFalse(resultFuture.isDone());
        futures.get(0).complete(1066);
        RecordCursorResult<Integer> result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1066, (int)result.get());

        // The (non-exceptional) firable cursor completes at "the same time" as the exceptional inner result.
        CompletableFuture<RecordCursorResult<Integer>> secondResultFuture = cursor.onNext();
        assertFalse(secondResultFuture.isDone());
        firableCursor.fire();
        assertFalse(secondResultFuture.isDone());
        firableCursor.fire();
        futures.get(1).completeExceptionally(runtimeEx);
        ExecutionException executionEx = assertThrows(ExecutionException.class, secondResultFuture::get);
        assertNotNull(executionEx.getCause());
        assertEquals(runtimeEx, executionEx.getCause());

        // Should get the same exception again
        executionEx = assertThrows(ExecutionException.class, () -> cursor.onNext().get());
        assertEquals(runtimeEx, executionEx.getCause());
    }

    /**
     * Test that when an exceptional future gets put in the pipeline of a {@code MapPipelinedCursor} that the
     * error is (eventually) propagated.
     *
     * @throws ExecutionException from futures joined in the test
     * @throws InterruptedException from futures joined in the test
     */
    @Test
    void mapPipelinedErrorPropagationInPipeline() throws ExecutionException, InterruptedException {
        final RuntimeException runtimeEx = new RuntimeException("some random exception");
        List<CompletableFuture<Integer>> futures = Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        RecordCursor<Integer> cursor = RecordCursor.fromList(Arrays.asList(0, 1, 2)).mapPipelined(futures::get, 2);
        CompletableFuture<RecordCursorResult<Integer>> resultFuture = cursor.onNext();
        assertFalse(resultFuture.isDone());
        futures.get(1).completeExceptionally(runtimeEx);
        assertFalse(resultFuture.isDone());
        futures.get(0).complete(1066);
        RecordCursorResult<Integer> result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1066, (int)result.get());

        CompletableFuture<RecordCursorResult<Integer>> secondResultFuture = cursor.onNext();
        ExecutionException executionEx = assertThrows(ExecutionException.class, secondResultFuture::get);
        assertNotNull(executionEx.getCause());
        assertEquals(runtimeEx, executionEx.getCause());

        // Should get the same exception again
        executionEx = assertThrows(ExecutionException.class, () -> cursor.onNext().get());
        assertEquals(runtimeEx, executionEx.getCause());
    }

    /**
     * Check the continuation returned when there is a time limit encountered while filling the pipeline of a
     * map pipelined cursor. Here, make sure that if the first future to be returned after the limit limit is reached,
     * then "time limit reached" is returned immediately and the continuation matches the last returned result.
     *
     * @throws ExecutionException from futures joined in the test
     * @throws InterruptedException from futures joined in the test
     */
    @Test
    void mapPipelinedContinuationWithTimeLimit() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Integer>> futures = Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(0, 1, 2, 3)), 3).mapPipelined(futures::get, 3);
        futures.get(2).complete(1415); // complete a future that is not immediately returned
        CompletableFuture<RecordCursorResult<Integer>> resultFuture = cursor.onNext();
        assertFalse(resultFuture.isDone());
        futures.get(0).complete(1066);
        RecordCursorResult<Integer> result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1066, (int)result.get());
        final RecordCursorContinuation lastContinuation = result.getContinuation();

        // When the time limit is reached, we are still waiting on futures[1], so we don't get any more results.
        resultFuture = cursor.onNext();
        assertTrue(resultFuture.isDone());
        result = resultFuture.get();
        assertFalse(result.hasNext());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, result.getNoNextReason());
        assertEquals(lastContinuation, result.getContinuation());

        assertEquals(1, (int)RecordCursor.fromList(Arrays.asList(0, 1, 2, 3), lastContinuation.toBytes()).onNext().get().get());
    }

    /**
     * Check the continuation returned when there is a time limit encountered while filling the pipeline of a
     * map pipelined cursor. Here, make sure that if the first result to be returned after the time limit is reached
     * is ready that we get that result back, but all future results (including any that were already ready) are not
     * returned. Verify that the continuation matches the last <em>returned</em> result, which is different from the
     * last result before the time limit is reached.
     *
     * @throws ExecutionException from futures joined in the test
     * @throws InterruptedException from futures joined in the test
     */
    @Test
    void mapPipelinedContinuationWithTimeLimitWithMoreToReturn() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Integer>> futures = Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(0, 1, 2, 3, 4)), 4).mapPipelined(futures::get, 4);
        futures.get(1).complete(1415);
        futures.get(3).complete(1807);
        CompletableFuture<RecordCursorResult<Integer>> resultFuture = cursor.onNext();
        assertFalse(resultFuture.isDone());
        futures.get(0).complete(1066);
        RecordCursorResult<Integer> result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1066, (int)result.get());

        // The time limit should be reached now by the pipelined cursor. As the second future has already completed,
        // it will be returned. However, the fourth future, despite also being completed, should *not* be returned
        // as there is an incomplete future in the middle.
        resultFuture = cursor.onNext();
        assertTrue(resultFuture.isDone());
        result = resultFuture.get();
        assertEquals(1415, (int)result.get());
        final RecordCursorContinuation lastContinuation = result.getContinuation();

        resultFuture = cursor.onNext();
        assertTrue(resultFuture.isDone());
        result = resultFuture.get();
        assertFalse(result.hasNext());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, result.getNoNextReason());
        assertEquals(lastContinuation, result.getContinuation());

        assertEquals(2, (int)RecordCursor.fromList(Arrays.asList(0, 1, 2, 3, 4), lastContinuation.toBytes()).onNext().get().get());
    }


    /**
     * Check the continuation returned when there is a time limit encountered while filling the pipeline of a
     * map pipelined cursor. Here, make sure that if the time limit is reached on the very first element that we
     * wait for the first future nonetheless (essentially to get a meaningful continuation). Validate that the
     * continuation we get back is from the last returned result.
     *
     * @throws ExecutionException from futures joined in the test
     * @throws InterruptedException from futures joined in the test
     */
    @Test
    void mapPipelinedContinuationWithTimeLimitBeforeFirstEntry() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Integer>> futures = Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>(), new CompletableFuture<>());
        futures.get(1).complete(1415);
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(0, 1, 2)), 2).mapPipelined(futures::get, 3);
        CompletableFuture<RecordCursorResult<Integer>> resultFuture = cursor.onNext();
        assertFalse(resultFuture.isDone());
        futures.get(0).complete(1066);
        RecordCursorResult<Integer> result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1066, (int)result.get());

        // The time limit has been reached, but futures[1] should already be in the pipeline so is returned
        resultFuture = cursor.onNext();
        assertTrue(resultFuture.isDone());
        result = resultFuture.get();
        assertTrue(result.hasNext());
        assertEquals(1415, (int)result.get());
        final RecordCursorContinuation lastContinuation = result.getContinuation();

        // This should be the time limit being reached
        resultFuture = cursor.onNext();
        assertTrue(resultFuture.isDone());
        result = resultFuture.get();
        assertFalse(result.hasNext());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, result.getNoNextReason());
        assertEquals(lastContinuation, result.getContinuation());

        assertEquals(2, (int)RecordCursor.fromList(Arrays.asList(0, 1, 2), lastContinuation.toBytes()).onNext().get().get());
    }

    @Test
    void lazyCursorTest() {
        RecordCursorIterator<Integer> cursor = new LazyCursor<>(
                CompletableFuture.completedFuture(RecordCursor.fromList(Lists.newArrayList(1, 2, 3, 4, 5)))).asIterator();
        int i = 1;
        while (i <= 5 && cursor.hasNext()) {
            assertEquals(i, (int)cursor.next());
            ++i;
        }
        assertEquals(6, i);
    }

    @Test
    void lazyCursorExceptionTest() {
        LazyCursor<Integer> cursor = new LazyCursor<>(
                CompletableFuture.supplyAsync(() -> {
                    throw new IllegalArgumentException("Uh oh");
                }));
        assertThrows(RecordCoreException.class, () -> cursor.getNext());
    }

    /**
     * A cursor that simulates out of band stopping by actually counting in band records returned.
     *
     * @param <T> type of elements of the cursor
     */
    public static class FakeOutOfBandCursor<T> extends RowLimitedCursor<T> {
        private final NoNextReason noNextReason;

        public FakeOutOfBandCursor(@Nonnull RecordCursor<T> inner, int limit, NoNextReason noNextReason) {
            super(inner, limit);
            assertTrue(noNextReason.isOutOfBand());
            this.noNextReason = noNextReason;
        }

        public FakeOutOfBandCursor(@Nonnull RecordCursor<T> inner, int limit) {
            this(inner, limit, NoNextReason.TIME_LIMIT_REACHED);
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<T>> onNext() {
            return super.onNext().thenApply(result -> {
                if (!result.hasNext() && result.getNoNextReason() == NoNextReason.RETURN_LIMIT_REACHED) {
                    nextResult = RecordCursorResult.withoutNextValue(result.getContinuation(), noNextReason);
                }
                return nextResult;
            });
        }
    }

    @Test
    void testFakeTimeLimitReasons() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3);
        assertEquals(Arrays.asList(1, 2, 3), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNext().getNoNextReason());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getNext().getContinuation().toBytes()), 3);
        assertEquals(Arrays.asList(4, 5), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNext().getNoNextReason());
    }

    @Test
    void testMapAsyncTimeLimitReasons() {
        // If stopped for a timeout, we additionally don't wait for incomplete items in the pipeline.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = i -> i != 2 ? CompletableFuture.completedFuture(i) : new CompletableFuture<>();
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3).mapPipelined(map, 10);
        assertEquals(Arrays.asList(1), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 3).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(2, 3, 4), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 3).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(5), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        assertNull(noNextResult.getContinuation().toBytes());
    }

    @Test
    void testMapAsyncScanLimitReasons() {
        // If stopped for a scan limit, no special handling.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED).mapPipelined(map, 10);
        assertEquals(Arrays.asList(1, 2, 3), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 3, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(4, 5), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        assertNull(noNextResult.getContinuation().toBytes());
    }

    @Test
    void testFilteredMapAsyncReasons1() {
        // May need continuation before the first record in the pipeline.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        final Function<Integer, Boolean> filter = i -> i % 2 == 0;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(2), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(4), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        // we've only just looked at the last element of the list so the cursor doesn't know that we've exhausted yet
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        assertNull(noNextResult.getContinuation().toBytes());
    }

    @Test
    void testFilteredMapAsyncReasons2() {
        // May need continuation before the first record in the pipeline.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
        final Function<Integer, CompletableFuture<Integer>> delay = (i) -> {
            CompletableFuture<Integer> result = new CompletableFuture<>();
            scheduler.schedule(() -> result.complete(i), 1, TimeUnit.MILLISECONDS);
            return result;
        };
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        final Function<Integer, Boolean> filter = i -> i == 7;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list).mapPipelined(delay, 1), 5).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation().toBytes());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()).mapPipelined(delay, 1), 5).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(7), cursor.asList().join());
        // may need to call hasNext() once more, to find out that we have really exhausted the cursor
        noNextResult = cursor.getNext();
        if (!noNextResult.getContinuation().isEnd()) {
            cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()).mapPipelined(delay, 1), 5).filter(filter).mapPipelined(map, 10);
        }
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        assertNull(noNextResult.getContinuation().toBytes());
    }

    @Test
    void testFilteredMapAsyncReasons3() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        final Function<Integer, Boolean> filter = i -> i % 2 == 0;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(2), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        assertNotNull(noNextResult.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, noNextResult.getContinuation().toBytes()), 3).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(4), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        assertNull(noNextResult.getContinuation().toBytes());
    }

    @Test
    void testFlatMapReasons() {
        // If the inside stops prematurely, the whole pipeline shuts down.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<byte[], RecordCursor<Integer>> outer = continuation -> RecordCursor.fromList(list, continuation);
        final BiFunction<Integer, byte[], RecordCursor<Integer>> baseInner = (i, continuation) ->
                RecordCursor.fromList(list.stream().map(j -> i * 10 + j).collect(Collectors.toList()), continuation);
        final BiFunction<Integer, byte[], RecordCursor<Integer>> timedInner = baseInner.andThen(cursor -> new FakeOutOfBandCursor<>(cursor, 3));
        RecordCursor<Integer> cursor = RecordCursor.flatMapPipelined(outer, timedInner, null, 10);
        assertEquals(Arrays.asList(11, 12, 13), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, noNextResult.getContinuation().toBytes(), 10);
        assertEquals(Arrays.asList(14, 15, 21, 22, 23), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, noNextResult.getContinuation().toBytes(), 10);
        assertEquals(Arrays.asList(24, 25, 31, 32, 33), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, noNextResult.getContinuation().toBytes(), 10);
        assertEquals(Arrays.asList(34, 35, 41, 42, 43), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, noNextResult.getContinuation().toBytes(), 10);
        assertEquals(Arrays.asList(44, 45, 51, 52, 53), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, noNextResult.getContinuation().toBytes(), 10);
        assertEquals(Arrays.asList(54, 55), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
    }

    @Test
    void testOrElseReasons() {
        // Don't take else path if inside stops prematurely.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final BiFunction<Executor, byte[], RecordCursor<Integer>> orElse = (x, cont) -> RecordCursor.fromList(Collections.singletonList(0), cont);
        RecordCursor<Integer> cursor = RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cont), 3)
                .filter(i -> false), orElse, null);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());

        cursor = RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cont), 3)
                .filter(i -> false), orElse, noNextResult.getContinuation().toBytes());
        assertEquals(Collections.singletonList(0), cursor.asList().join());
        noNextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
    }

    @Test
    void orElseWithEventuallyNonEmptyInner() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        RecordCursor<Integer> cursor = getOrElseOfFilteredFakeOutOfBandCursor(list, 3, 4, null);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        RecordCursorResult<Integer> nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, nextResult.getNoNextReason());
        cursor = getOrElseOfFilteredFakeOutOfBandCursor(list, 3, 4, nextResult.getContinuation().toBytes());

        // Choose the inner branch since we eventually get a value
        assertEquals(Collections.singletonList(5), cursor.asList().join());
        nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, nextResult.getNoNextReason());
    }

    @Test
    void orElseContinueWithInnerBranchAfterDecision() {
        final List<Integer> longList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18);
        RecordCursor<Integer> cursor = getOrElseOfFilteredFakeOutOfBandCursor(longList, 3, 10, null);

        RecordCursorResult<Integer> nextResult = null;
        for (int i = 0; i < 3; i++) { // three rounds with no results
            nextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, nextResult.getNoNextReason());
            cursor = getOrElseOfFilteredFakeOutOfBandCursor(longList, 3, 10, nextResult.getContinuation().toBytes());
        }

        // Choose the inner branch
        assertEquals(ImmutableList.of(11, 12), cursor.asList().join());
        nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, nextResult.getNoNextReason());

        byte[] continuation = nextResult.getContinuation().toBytes();
        int i = 13;
        while (continuation != null) {
            cursor = getOrElseOfFilteredFakeOutOfBandCursor(longList, 3, 10, continuation);
            nextResult = cursor.getNext();
            if (nextResult.hasNext()) {
                assertEquals(i, nextResult.get());
                i++;
            }

            continuation = nextResult.getContinuation().toBytes();
        }
    }

    @Test
    void orElseContinueWithElseBranchAfterDecision() {
        final List<Integer> innerList = Arrays.asList(1, 2, 3, 4, 5);
        final List<Integer> elseList = Arrays.asList(-1, -2, -3, -4, -5);
        final BiFunction<Executor, byte[], RecordCursor<Integer>> orElse = (x, cont) ->
                new FakeOutOfBandCursor<>(RecordCursor.fromList(elseList, cont), 3, RecordCursor.NoNextReason.TIME_LIMIT_REACHED);

        // Go through inner cursor to determine that it's empty.
        RecordCursor<Integer> cursor = RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(innerList, cont), 3)
                .filter(i -> false), orElse, null);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        RecordCursorResult<Integer> nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, nextResult.getNoNextReason());
        cursor = RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(innerList, cont), 3)
                .filter(i -> false), orElse, nextResult.getContinuation().toBytes());

        // Switch to else branch, but get stuck with an out-of-band limit.
        assertEquals(ImmutableList.of(-1, -2, -3), cursor.asList().join());
        nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, nextResult.getNoNextReason());

        // Continue else branch
        cursor = RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(innerList, cont), 3)
                .filter(i -> false), orElse, nextResult.getContinuation().toBytes());
        assertEquals(ImmutableList.of(-4, -5), cursor.asList().join());
        nextResult = cursor.getNext();
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, nextResult.getNoNextReason());
    }

    @Nonnull
    private static RecordCursor<Integer> getOrElseOfFilteredFakeOutOfBandCursor(@Nonnull List<Integer> list, int limit, int threshold,
                                                                                @Nullable byte[] continuation) {
        final BiFunction<Executor, byte[], RecordCursor<Integer>> orElse = (x, cont) -> RecordCursor.fromList(Collections.singletonList(0), cont);
        return RecordCursor.orElse(cont -> new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cont), limit)
                .filter(i -> i > threshold), orElse, continuation);
    }

    static class BrokenCursor implements RecordCursor<String> {
        private boolean closed = false;

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<String>> onNext() {
            return CompletableFuture.supplyAsync(() -> {
                throw new RuntimeException("sorry");
            }, getExecutor());
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return TestExecutors.defaultThreadPool();
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }
    }

    @Test
    void hasNextErrorStack() {
        final Iterator<String> erring = new BrokenCursor().asIterator();
        try {
            Iterators.getLast(erring, null);
        } catch (Exception ex) {
            for (Throwable t = ex; t != null; t = t.getCause()) {
                if (Arrays.stream(t.getStackTrace()).anyMatch(s -> s.getMethodName().equals("getLast"))) {
                    return;
                }
            }
            fail("did not find getLast() on stack");
        }
    }

    @Test
    void reduceTest() throws Exception {
        RecordCursor<Integer> cursor = RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        CompletableFuture<Integer> sum = cursor.reduce(0, Integer::sum);
        assertEquals(28, sum.get());

        cursor = RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        sum = cursor.reduce(0, Integer::sum, x -> false);
        assertEquals(28, sum.get());

        cursor = RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        sum = cursor.reduce(0, Integer::sum, x -> x > 10);
        assertEquals(15, sum.get());
    }

    @Test
    void asStreamTest() {
        assertEquals(7, RecordCursor.fromList(Arrays.asList(1, 2, 3, 4, 5, 6, 7)).asStream().count());
        assertEquals(2, RecordCursor.fromList(Arrays.asList(2, 3)).asStream().findFirst().get());
        assertEquals(3, RecordCursor.fromList(Arrays.asList(2, 3)).map(x -> x + 1).asStream().findFirst().get());
        assertThrows(RuntimeException.class, () -> RecordCursor.fromList(Arrays.asList(2, 3)).asStream(
                () -> {
                    throw new RuntimeException("on close");
                }).close());

    }

    static Stream<Arguments> pipelinedCursors() {
        return Stream.<BiFunction<Integer, CompletableFuture<Void>, RecordCursor<?>>>of(
                named("mapPipelined", RecordCursorTest::mapPipelinedCursorToClose),
                named("singletonFlatMapPipelined", RecordCursorTest::singletonFlatMapPipelinedCursorToClose),
                named("flatMapPipelined", RecordCursorTest::flatMapPipelinedCursorToClose)
        ).map(Arguments::of);
    }

    private static <T, U, R> BiFunction<T, U, R> named(String name, BiFunction<T, U, R> inner) {
        // TODO junit 5.8 has `Named.named` which allows adding a name for the arguments
        return new BiFunction<>() {
            @Override
            public R apply(T t, U u) {
                return inner.apply(t, u);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    @Nonnull
    private static RecordCursor<Integer> mapPipelinedCursorToClose(int iteration, CompletableFuture<Void> signal) {
        return RecordCursor.fromList(EXECUTOR, IntStream.range(0, iteration % 199).boxed().collect(Collectors.toList()))
                .mapPipelined(val -> signal.thenApplyAsync(ignore -> val + 349, EXECUTOR), iteration % 19 + 2);
    }

    @Nonnull
    private static RecordCursor<String> singletonFlatMapPipelinedCursorToClose(int iteration, CompletableFuture<Void> signal) {
        return RecordCursor.flatMapPipelined(
                outerContinuation -> RecordCursor.fromList(EXECUTOR, IntStream.range(0, iteration % 199).boxed().collect(Collectors.toList()), outerContinuation),
                (outerValue, innerContinuation) -> RecordCursor.fromFuture(EXECUTOR, signal.thenApplyAsync(ignore -> String.valueOf(outerValue), EXECUTOR), innerContinuation),
                null,
                null,
                iteration % 19 + 2
        );
    }

    @Nonnull
    private static RecordCursor<String> flatMapPipelinedCursorToClose(int iteration, CompletableFuture<Void> signal) {
        return RecordCursor.flatMapPipelined(
                outerContinuation -> RecordCursor.fromList(EXECUTOR, IntStream.range(0, iteration % 199).boxed().collect(Collectors.toList()), outerContinuation),
                (outerValue, innerContinuation) -> {
                    if (outerValue % 37 == 0) {
                        return RecordCursor.empty(EXECUTOR);
                    } else {
                        return new LazyCursor<>(signal.thenApply(ignore ->
                                RecordCursor.fromList(EXECUTOR, IntStream.range(0, outerValue % 37)
                                        .mapToObj(innerValue -> outerValue + ":" + innerValue).collect(Collectors.toList()),
                                        innerContinuation)));

                    }
                },
                null,
                null,
                iteration % 19 + 2
        );
    }

    @ParameterizedTest
    @MethodSource("pipelinedCursors")
    void closePipelineWhileCancelling(@Nonnull BiFunction<Integer, CompletableFuture<Void>, RecordCursor<?>> cursorGenerator) {
        Map<Class<? extends Throwable>, Integer> exceptionCount = new HashMap<>();
        for (int i = 0; i < 20_000; i++) {
            try {
                LOGGER.info(KeyValueLogMessage.of("running pipeline close test", "iteration", i));
                CompletableFuture<Void> signal = new CompletableFuture<>();
                RecordCursor<?> cursor = cursorGenerator.apply(i, signal);
                CompletableFuture<? extends RecordCursorResult<?>> resultFuture = cursor.onNext();

                signal.complete(null);
                cursor.close();
                resultFuture.get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                Throwable errToCount;
                if (e instanceof ExecutionException && e.getCause() != null) {
                    errToCount = e.getCause();
                    errToCount.addSuppressed(e);
                } else {
                    errToCount = e;
                }
                if (errToCount instanceof CancellationException) {
                    continue;
                }
                LOGGER.error(KeyValueLogMessage.of("error during test"), errToCount);
                exceptionCount.compute(errToCount.getClass(), (k, v) -> v == null ? 1 : v + 1);
            }
        }
        KeyValueLogMessage msg = KeyValueLogMessage.build("exception counts");
        msg.addKeysAndValues(exceptionCount);
        LOGGER.info(msg.toString());
        assertThat(exceptionCount, Matchers.anEmptyMap());
    }

    @ParameterizedTest
    @MethodSource("pipelinedCursors")
    void pipelinedCursorAfterClosing(@Nonnull BiFunction<Integer, CompletableFuture<Void>, RecordCursor<?>> cursorGenerator) {
        for (int i = 0; i < 2000; i++) {
            LOGGER.info(KeyValueLogMessage.of("running map pipeline close test", "iteration", i));
            CompletableFuture<Void> signal = new CompletableFuture<>();
            LOGGER.info(EXECUTOR.toString());
            RecordCursor<?> cursor = cursorGenerator.apply(i, signal);

            signal.complete(null);
            cursor.close();
            CompletableFuture<? extends RecordCursorResult<?>> resultFuture = cursor.onNext();
            final ExecutionException executionException = assertThrows(ExecutionException.class,
                    () -> resultFuture.get(2, TimeUnit.SECONDS));
            assertThat(executionException.getCause(), Matchers.instanceOf(CancellationException.class));
        }
    }

    @Test
    void futureCursorTest() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        final RecordCursorContinuation continuation;
        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, future, null)) {
            assertSame(EXECUTOR, fromFuture.getExecutor());
            future.complete(42);
            RecordCursorResult<Integer> result = fromFuture.getNext();
            assertTrue(result.hasNext(), "first result from future should have value");
            assertEquals(42, result.get());
            assertFalse(result.getContinuation().isEnd());
            continuation = result.getContinuation();

            result = fromFuture.getNext();
            assertFalse(result.hasNext(), "second result from future should have value");
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());
        }

        CompletableFuture<Integer> secondFuture = new CompletableFuture<>();
        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, secondFuture, continuation.toBytes())) {
            assertSame(EXECUTOR, fromFuture.getExecutor());

            RecordCursorResult<Integer> result = fromFuture.getNext();
            assertFalse(result.hasNext(), "future should not have value when resuming from a continuation");
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());
        }
    }

    @Test
    void futureCursorFromSupplierTest() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        final RecordCursorContinuation continuation;
        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, () -> future, null)) {
            assertSame(EXECUTOR, fromFuture.getExecutor());
            future.complete(1066);
            RecordCursorResult<Integer> result = fromFuture.getNext();
            assertTrue(result.hasNext(), "first result from future should have value");
            assertEquals(1066, result.get());
            assertFalse(result.getContinuation().isEnd());
            continuation = result.getContinuation();

            result = fromFuture.getNext();
            assertFalse(result.hasNext(), "second result from future should have value");
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());
        }

        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, () -> fail("should not be run"), continuation.toBytes())) {
            assertSame(EXECUTOR, fromFuture.getExecutor());

            RecordCursorResult<Integer> result = fromFuture.getNext();
            assertFalse(result.hasNext(), "future should not have value when resuming from a continuation");
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());
        }
    }

    @Test
    void futureCursorCompletesWhenUnderlyingCompletes() throws Exception {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final RecordCursorContinuation continuation;
        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, future, null)) {
            assertSame(EXECUTOR, fromFuture.getExecutor());
            CompletableFuture<RecordCursorResult<Integer>> resultFuture = fromFuture.onNext();
            assertFalse(resultFuture.isDone(), "result should not be done until underlying completes");
            future.complete(1819);

            // The previous result future should complete quickly. The timeout here is
            // a bit of a hedge, but as currently written, the resultFuture should actually complete
            // on this thread, and so the resultFuture should already be completed
            // by now. But that's not necessarily part of the contract of the cursor, so
            // instead, just assert here that we complete in a reasonable amount of time
            RecordCursorResult<Integer> result = resultFuture.get(1, TimeUnit.SECONDS);
            assertTrue(result.hasNext());
            assertEquals(1819, result.get());
            continuation = result.getContinuation();
            assertFalse(continuation.isEnd());
            assertFalse(continuation.toByteString().isEmpty());
            assertThat(continuation.toBytes(), notNullValue());
        }

        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, () -> fail("should not run"), continuation.toBytes())) {
            assertSame(EXECUTOR, fromFuture.getExecutor());
            CompletableFuture<RecordCursorResult<Integer>> resultFuture = fromFuture.onNext();
            assertTrue(resultFuture.isDone(), "empty result should not need to wait");
            RecordCursorResult<Integer> result = resultFuture.get();
            assertFalse(result.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            assertTrue(result.getContinuation().isEnd());
        }
    }

    @Test
    void futureCursorPropagatesError() {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try (RecordCursor<Integer> fromFuture = RecordCursor.fromFuture(EXECUTOR, future, null)) {
            assertSame(EXECUTOR, fromFuture.getExecutor());
            CompletableFuture<RecordCursorResult<Integer>> resultFuture = fromFuture.onNext();
            assertFalse(resultFuture.isDone(), "result should not be done until underlying completes");

            final RecordCoreException error = new RecordCoreException("test error");
            future.completeExceptionally(error);
            CompletionException futureError = assertThrows(CompletionException.class, future::join);
            assertSame(error, futureError.getCause(), "result should complete exception while propagating the cause");
        }
    }
}
