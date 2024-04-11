/*
 * LazyOpenerTest.java
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated // To avoid contention on the thread pool with other tests running in parallel
class LazyOpenerTest {

    @Test
    void testOpensLazilyExactlyOnce() throws IOException {
        AtomicInteger supplier = new AtomicInteger(0);
        final LazyOpener<Integer> opener = LazyOpener.supply(supplier::incrementAndGet);
        assertEquals(0, supplier.get());
        assertEquals(1, opener.get());
        assertEquals(1, opener.get());
        assertEquals(1, opener.getUnchecked());
        assertEquals(1, supplier.get());
    }

    @Test
    void testOpensLazilyExactlyOnceThreaded() {
        AtomicInteger starts = new AtomicInteger(0);
        AtomicInteger ends = new AtomicInteger(0);
        final int concurrency = 100;
        final LazyOpener<Integer> opener = LazyOpener.supply(() -> {
            starts.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new AssertionError("Timed out waiting for latch");
            }
            return ends.incrementAndGet();
        });
        ConcurrentHashMap<Thread, Integer> threads = new ConcurrentHashMap<>();
        final List<Integer> allValues = IntStream.range(0, concurrency)
                .parallel()
                .map(i -> {
                    threads.compute(Thread.currentThread(), (k, v) -> v == null ? 1 : v + 1);
                    return opener.getUnchecked();
                })
                .boxed()
                .collect(Collectors.toList());
        MatcherAssert.assertThat(allValues, Matchers.everyItem(Matchers.is(1)));
        // Stream.parallel doesn't guarantee that it will run in 100 parallel threads
        MatcherAssert.assertThat(threads.keySet(), Matchers.hasSize(Matchers.greaterThan(10)));
    }

    @Test
    void testForkJoinPoolDeadlock() throws ExecutionException, InterruptedException, TimeoutException {
        // Lucene heavily uses `LazyCloseable` to Lazily open inputs from FDB. For StoredFieldsFormat (at the time
        // LazyCloseable was added), Lucene would "open" one, but not actually call get, it would then fork a bunch of
        // threads, in a forkJoinPool, each of which would in turn, try to call get. This created a deadlock situation when
        // LazyOpener was implemented with Suppliers.memoize. This test explicitly tests this, and with the previous
        // attempt that had Suppliers.memoize, this test would timeout, instead of taking the 2 seconds in the delayed
        // future.
        final ForkJoinPool forkJoinPool = new ForkJoinPool(2);
        final AtomicInteger openCounter = new AtomicInteger(0);
        LazyOpener<String> initial = LazyOpener.supply(() -> {
            int openCount = openCounter.incrementAndGet();
            try {
                return CompletableFuture.runAsync(() -> { }, forkJoinPool)
                        .thenCompose(ignored -> MoreAsyncUtil.delayedFuture(2, TimeUnit.SECONDS))
                        .thenApplyAsync(v -> "Opened " + openCount, forkJoinPool)
                        .get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        final List<CompletableFuture<String>> result = IntStream.range(0, 50).parallel()
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> initial.getUnchecked() + " " + i, forkJoinPool))
                .collect(Collectors.toList());
        final List<String> strings = AsyncUtil.getAll(result).get(10, TimeUnit.SECONDS);
        MatcherAssert.assertThat(strings, Matchers.containsInAnyOrder(IntStream.range(0, 50)
                .mapToObj(i -> Matchers.is("Opened 1 " + i)).collect(Collectors.toList())));
    }

    @Test
    void testThrowsIoException() {
        IOException thrownException = new IOException("test foo");
        final LazyOpener<Object> opener = LazyOpener.supply(() -> {
            throw thrownException;
        });
        final IOException resultingException = assertThrows(IOException.class, opener::get);
        assertSame(thrownException, resultingException);
    }

    @Test
    void testThrowsUncheckedIoException() {
        IOException thrownException = new IOException("test foo");
        final LazyOpener<Object> opener = LazyOpener.supply(() -> {
            throw thrownException;
        });
        final UncheckedIOException resultingException = assertThrows(UncheckedIOException.class, opener::getUnchecked);
        assertSame(thrownException, resultingException.getCause());
    }
}
