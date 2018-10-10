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
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.cursors.MapCursor;
import com.apple.foundationdb.record.cursors.RowLimitedCursor;
import com.apple.foundationdb.record.cursors.SkipCursor;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link RecordCursor}.
 */
public class RecordCursorTest {
    static final Executor EXECUTOR = ForkJoinPool.commonPool();

    Timer timer;

    @BeforeEach
    public void setup() throws Exception {
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
        int onHasNextCalled;

        public AsyncCountdown(int count) {
            this.count = count;
        }

        @Nonnull
        @Override
        public CompletableFuture<Boolean> onHasNext() {
            onHasNextCalled++;
            // Use thenApplyAsync to deliberately introduce a delay.
            return CompletableFuture.completedFuture(count).thenApplyAsync(c -> c > 0, getExecutor());
        }

        @Nullable
        @Override
        public Integer next() {
            return count--;
        }

        @Nullable
        @Override
        public byte[] getContinuation() {
            return null;
        }

        @Override
        public NoNextReason getNoNextReason() {
            return NoNextReason.SOURCE_EXHAUSTED;
        }

        @Override
        public void close() {
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
    public void mapPipelinedReuseTest() throws Exception {
        AsyncCountdown cursor = new AsyncCountdown(100);
        RecordCursor<Integer> map = cursor.mapPipelined(i -> delayedFuture(i, 10), 10);
        assertEquals(IntStream.range(0, 100).mapToObj(i -> 100 - i).collect(Collectors.toList()), map.asList().join());
        assertThat(cursor.onHasNextCalled, Matchers.lessThanOrEqualTo(102));
    }

    @Test
    public void forEachAsyncTest() {
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
        assertThat(end  - start, Matchers.greaterThanOrEqualTo(40L));

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
    public void orElseTest() throws Exception {
        List<Integer> ints = Arrays.asList(1, 2, 3);
        Function<Executor, RecordCursor<Integer>> elseZero = (executor) -> RecordCursor.fromFuture(executor, CompletableFuture.completedFuture(0));
        assertEquals(ints, RecordCursor.fromList(ints).asList().join());
        assertEquals(ints, RecordCursor.fromList(ints).orElse(elseZero).asList().join());

        assertEquals(Arrays.asList(0), RecordCursor.fromList(Collections.<Integer>emptyList()).orElse(elseZero).asList().join());
    }

    //@Test @Slow
    // Will get either NPE or NoSuchElementException after a while.
    public void orElseTimingErrorTest() throws Exception {
        Function<Executor, RecordCursor<Integer>> elseZero = (executor) -> RecordCursor.fromFuture(executor, CompletableFuture.completedFuture(0));
        for (int i = 0; i < 100000; i++) {
            RecordCursor<Integer> cursor = RecordCursor.fromList(Collections.<Integer>emptyList()).orElse(elseZero);
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
    public void limitTest() {
        List<Integer> ints = Arrays.asList(1,2,3,4);

        // Make sure that if the limit is less than the size, we get the thing suppose.
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).limitRowsTo(3);
        assertTrue(cursor instanceof RowLimitedCursor, "Setting limit should create a LimitCursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1,2,3), newInts);

        // Make sure that if the limit is greater than the size, we get everything.
        cursor = RecordCursor.fromList(ints).limitRowsTo(5);
        assertTrue(cursor instanceof RowLimitedCursor, "Setting limit should create a LimitCursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1,2,3,4), newInts);

        cursor = RecordCursor.fromList(ints).limitRowsTo(Integer.MAX_VALUE);
        assertFalse(cursor instanceof RowLimitedCursor, "Setting max limit shouldn't actually create a LimitCursor");
    }

    @Test
    public void skipTest() {
        List<Integer> ints = Arrays.asList(1,2,3,4);

        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).skip(2);
        assertTrue(cursor instanceof SkipCursor, "Setting skip should create a SkipCursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(3,4), newInts);

        cursor = RecordCursor.fromList(ints).skip(0);
        assertFalse(cursor instanceof SkipCursor, "Setting skip 0 shouldn't actually create a SkipCursor");
    }

    @Test
    public void filterTest() {
        List<Integer> ints = Arrays.asList(1,2,3,4,5,6,7);
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints).filter(i -> i % 2 == 0);
        assertTrue(cursor instanceof FilterCursor, "Creating a filter should create a filter cursor");
        List<Integer> newInts = cursor.asList().join();
        assertEquals(Arrays.asList(2,4,6), newInts);

        cursor = RecordCursor.fromList(ints).filterAsync(i -> CompletableFuture.completedFuture(i % 2 != 0), 1);
        assertTrue(cursor instanceof MapCursor, "Creating an async filter should create a map cursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1,3,5,7), newInts);

        ints = Arrays.asList(1,2,3, null, 4, 5, 6, 7);

        cursor = RecordCursor.fromList(ints).filter(i -> {
            if (i == null) {
                return null;
            } else {
                return i % 2 != 0;
            }
        });
        assertTrue(cursor instanceof FilterCursor, "Creating a filter should create a filter cursor");
        newInts = cursor.asList().join();
        assertEquals(Arrays.asList(1,3,5,7), newInts);

        cursor = RecordCursor.fromList(ints).filterAsync(i -> {
            if (i == null) {
                return CompletableFuture.completedFuture(null);
            } else {
                return CompletableFuture.completedFuture(i % 2 == 0);
            }
        }, 1);
        newInts = cursor.asList().join();
        assertTrue(cursor instanceof MapCursor, "Creating an async filter should create a map cursor");
        assertEquals(Arrays.asList(2,4,6), newInts);
    }

    @Test
    public void firstTest() throws Exception {
        List<Integer> ints = Arrays.asList(1,2,3,4);
        RecordCursor<Integer> cursor = RecordCursor.fromList(ints);
        assertEquals(Optional.of(1), cursor.first().get());

        List<Integer> emptyInts = Collections.emptyList();
        cursor = RecordCursor.fromList(emptyInts);
        assertEquals(Optional.empty(), cursor.first().get());
    }

    @Test
    public void pipelineContinuationTest() throws Exception {
        List<Integer> ints = Lists.newArrayList(1,2,3,4,5);
        List<Integer> expected = ints.stream().flatMap(o -> ints.stream().map(i -> o * 100 + i)).collect(Collectors.toList());

        Function<byte[], RecordCursor<Integer>> outerFunc = cont -> RecordCursor.fromList(ints, cont);
        Function<Integer, RecordCursor<Integer>> innerFunc1 = outer -> RecordCursor.fromList(ints)
                .map(inner -> outer.intValue() * 100 + inner.intValue());
        assertEquals(expected, outerFunc.apply(null).flatMapPipelined(innerFunc1, 1).asList().join());

        BiFunction<Integer, byte[], RecordCursor<Integer>> innerFunc2 = (outer, cont) -> RecordCursor.fromList(ints, cont)
                .map(inner -> outer.intValue() * 100 + inner.intValue());
        assertEquals(expected, RecordCursor.flatMapPipelined(outerFunc, innerFunc2, null, 1).asList().join());

        List<Integer> pieces = new ArrayList<>();
        byte[] continuation = null;
        do {
            // Keep stopping and restarting every 3 items.
            int limit = 3;
            RecordCursor<Integer> cursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc2, continuation, 7);
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
        RecordCursor<Integer> partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc2, checkFunc, continuation, 7).limitRowsTo(12);
        pieces.addAll(partCursor.asList().get());
        continuation = partCursor.getContinuation();
        ints.remove(2); // The 3, of which we've done 301, 302
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc2, checkFunc, continuation, 7);
        pieces.addAll(partCursor.asList().get());
        List<Integer> adjusted = new ArrayList<>(expected);
        // Everything after where we restarted that involves the removed item (3).
        adjusted.removeIf(i -> i > 302 && ((i / 100) % 10 == 3 || i % 10 == 3));
        assertEquals(adjusted, pieces);

        // When an item is added right at the one we were on, we do it all first and then the rest of the next one.
        ints.add(2, 3);
        pieces.clear();
        continuation = null;
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc2, checkFunc, continuation, 7).limitRowsTo(12);
        pieces.addAll(partCursor.asList().get());
        continuation = partCursor.getContinuation();
        ints.add(2, 22); // Before the 3, of which we've done 301, 302
        partCursor = RecordCursor.flatMapPipelined(outerFunc, innerFunc2, checkFunc, continuation, 7);
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

    @Test
    public void lazyCursorTest() {
        LazyCursor<Integer> cursor = new LazyCursor<>(
                CompletableFuture.completedFuture(RecordCursor.fromList(Lists.newArrayList(1, 2, 3, 4, 5))));
        int i = 1;
        while (i <= 5 && cursor.hasNext()) {
            assertEquals(i, (int) cursor.next());
            ++i;
        }
        assertEquals(6, i);
    }

    /**
     * A cursor that simulates out of band stopping by actually counting in band records returned.
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

        @Override
        public NoNextReason getNoNextReason() {
            if (limitReached() && getContinuation() != null) {
                return noNextReason;
            } else {
                return super.getNoNextReason();
            }
        }
    }

    @Test
    public void testFakeTimeLimitReasons() throws Exception {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3);
        assertEquals(Arrays.asList(1, 2, 3), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3);
        assertEquals(Arrays.asList(4, 5), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
    }

    @Test
    public void testMapAsyncTimeLimitReasons() throws Exception {
        // If stopped for a timeout, we additionally don't wait for incomplete items in the pipeline.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = i -> i != 2 ? CompletableFuture.completedFuture(i) : new CompletableFuture<>();
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3).mapPipelined(map, 10);
        assertEquals(Arrays.asList(1), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(2, 3, 4), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(5), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    public void testMapAsyncScanLimitReasons() throws Exception {
        // If stopped for a scan limit, no special handling.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED).mapPipelined(map, 10);
        assertEquals(Arrays.asList(1, 2, 3), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED).mapPipelined(CompletableFuture::completedFuture, 10);
        assertEquals(Arrays.asList(4, 5), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    public void testFilteredMapAsyncReasons1() throws Exception {
        // May need continuation before the first record in the pipeline.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        final Function<Integer, Boolean> filter = i -> i % 2 == 0;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(2), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(4), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 1).filter(filter).mapPipelined(map, 10);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    public void testFilteredMapAsyncReasons2() throws Exception {
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
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()).mapPipelined(delay, 1), 5).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(7), cursor.asList().join());
        // may need to call hasNext() once more, to find out that we have really exhausted the cursor
        if (cursor.getContinuation() != null) {
            cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()).mapPipelined(delay, 1), 5).filter(filter).mapPipelined(map, 10);
        }
        assertFalse(cursor.hasNext());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    public void testFilteredMapAsyncReasons3() throws Exception {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Integer, CompletableFuture<Integer>> map = CompletableFuture::completedFuture;
        final Function<Integer, Boolean> filter = i -> i % 2 == 0;
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(2), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        assertNotNull(cursor.getContinuation());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3).filter(filter).mapPipelined(map, 10);
        assertEquals(Arrays.asList(4), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
        assertNull(cursor.getContinuation());
    }

    @Test
    public void testFlatMapReasons() throws Exception {
        // If the inside stops prematurely, the whole pipeline shuts down.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<byte[], RecordCursor<Integer>> outer = continuation -> RecordCursor.fromList(list, continuation);
        final BiFunction<Integer, byte[], RecordCursor<Integer>> baseInner = (i, continuation) ->
                RecordCursor.fromList(list.stream().map(j -> i * 10 + j).collect(Collectors.toList()), continuation);
        final BiFunction<Integer, byte[], RecordCursor<Integer>> timedInner = baseInner.andThen(cursor -> new FakeOutOfBandCursor<>(cursor, 3));
        RecordCursor<Integer> cursor = RecordCursor.flatMapPipelined(outer, timedInner, null, 10);
        assertEquals(Arrays.asList(11, 12, 13), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, cursor.getContinuation(), 10);
        assertEquals(Arrays.asList(14, 15, 21, 22, 23), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, cursor.getContinuation(), 10);
        assertEquals(Arrays.asList(24, 25, 31, 32, 33), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, cursor.getContinuation(), 10);
        assertEquals(Arrays.asList(34, 35, 41, 42, 43), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, cursor.getContinuation(), 10);
        assertEquals(Arrays.asList(44, 45, 51, 52, 53), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = RecordCursor.flatMapPipelined(outer, timedInner, cursor.getContinuation(), 10);
        assertEquals(Arrays.asList(54, 55), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
    }

    @Test
    public void testOrElseReasons() throws Exception {
        // Don't take else path if inside stops prematurely.
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Function<Executor, RecordCursor<Integer>> orElse = x -> RecordCursor.fromList(Collections.singletonList(0));
        RecordCursor<Integer> cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list), 3)
                .filter(i -> false)
                .orElse(orElse);
        assertEquals(Collections.emptyList(), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
        cursor = new FakeOutOfBandCursor<>(RecordCursor.fromList(list, cursor.getContinuation()), 3)
                .filter(i -> false)
                .orElse(orElse);
        assertEquals(Collections.singletonList(0), cursor.asList().join());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
    }

    static class BrokenCursor implements RecordCursor<String> {
        @Nonnull
        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return CompletableFuture.supplyAsync(() -> {
                throw new RuntimeException("sorry");
            }, getExecutor());
        }

        @Nullable
        @Override
        public String next() {
            throw new NoSuchElementException();
        }

        @Nullable
        @Override
        public byte[] getContinuation() {
            return null;
        }

        @Override
        public NoNextReason getNoNextReason() {
            return NoNextReason.SOURCE_EXHAUSTED;
        }

        @Override
        public void close() {
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return ForkJoinPool.commonPool();
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }
    }

    @Test
    public void hasNextErrorStack() throws Exception {
        final Iterator<String> erring = new BrokenCursor();
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

}
