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

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
