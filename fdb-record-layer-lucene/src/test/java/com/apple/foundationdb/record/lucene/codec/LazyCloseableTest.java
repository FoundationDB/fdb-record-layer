/*
 * LazyCloseableTest.java
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

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LazyCloseableTest {

    @Test
    void testOpensLazilyExactlyOnce() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        try (LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(
                () -> new CountingCloseable(openCounter, closeCounter))) {
            assertEquals(0, openCounter.get());
            assertEquals(1, opener.get().openCounts);
            assertEquals(1, opener.get().openCounts);
            assertEquals(1, opener.getUnchecked().openCounts);
            assertSame(opener.get(), opener.get());
            assertEquals(1, openCounter.get());
        }
        assertEquals(1, closeCounter.get());
    }

    @Test
    void testOpensLazilyExactlyOnceThreaded() {
        AtomicInteger starts = new AtomicInteger(0);
        AtomicInteger ends = new AtomicInteger(0);
        AtomicInteger opens = new AtomicInteger(0);
        AtomicInteger closes = new AtomicInteger(0);
        final int concurrency = 100;
        final LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(() -> {
            starts.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new AssertionError("Timed out waiting for latch");
            }
            ends.incrementAndGet();
            return new CountingCloseable(opens, closes);
        });
        ConcurrentHashMap<Thread, Integer> threads = new ConcurrentHashMap<>();
        final List<CountingCloseable> allValues = IntStream.range(0, concurrency)
                .parallel()
                .mapToObj(i -> {
                    threads.compute(Thread.currentThread(), (k, v) -> v == null ? 1 : v + 1);
                    return opener.getUnchecked();
                })
                .collect(Collectors.toList());
        MatcherAssert.assertThat(allValues, Matchers.everyItem(Matchers.sameInstance(allValues.get(0))));
        // Stream.parallel doesn't guarantee that it will run in 100 parallel threads
        MatcherAssert.assertThat(threads.keySet(), Matchers.hasSize(Matchers.greaterThan(10)));
    }

    @Test
    void testCloseDoesNotOpen() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        LazyCloseable.supply(() -> new CountingCloseable(openCounter, closeCounter)).close();
        assertEquals(0, openCounter.get());
    }

    @Test
    void testCloseCloses() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        try (LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(() -> new CountingCloseable(openCounter, closeCounter))) {
            opener.get();
        }
        assertEquals(1, openCounter.get());
        assertEquals(1, closeCounter.get());
    }

    @Test
    void testCloseMultipleTimes() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(() -> new CountingCloseable(openCounter, closeCounter));
        try {
            opener.get();
        } finally {
            for (int i = 0; i < 5; i++) {
                opener.close();
            }
        }
        assertEquals(1, openCounter.get());
        assertEquals(5, closeCounter.get());
    }

    @Test
    void testCloseFails() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(() -> new CountingCloseable(openCounter, closeCounter, true));
        try {
            opener.get();
        } finally {
            assertThrows(IOException.class, opener::close, "an error");
            assertThrows(IOException.class, opener::close, "an error");
        }
        assertEquals(1, openCounter.get());
        assertEquals(0, closeCounter.get());
    }

    @Test
    void testThrowsIoException() throws IOException {
        IOException thrownException = new IOException("test foo");
        try (LazyCloseable<Closeable> opener = failingOpener(thrownException)) {
            final IOException resultingException = assertThrows(IOException.class, opener::get);
            assertSame(thrownException, resultingException);
        } // close should not throw
    }

    @Test
    void testThrowsUncheckedIoException() throws IOException {
        IOException thrownException = new IOException("test foo");
        try (LazyCloseable<Closeable> opener = failingOpener(thrownException)) {
            UncheckedIOException resultingException = assertThrows(UncheckedIOException.class, opener::getUnchecked);
            assertSame(thrownException, resultingException.getCause());
        } // close should not throw
    }

    @Test
    void testUnusedDoesNotThrowOnClose() {
        IOException thrownException = new IOException("test foo");
        LazyCloseable<Closeable> opener = failingOpener(thrownException);
        assertDoesNotThrow(opener::close);
    }

    @Nonnull
    private static LazyCloseable<Closeable> failingOpener(final IOException thrownException) {
        return LazyCloseable.supply(() -> {
            throw thrownException;
        });
    }

    private static class CountingCloseable implements Closeable {
        final int openCounts;
        final AtomicInteger closeCounter;
        final boolean failOnClose;

        private CountingCloseable(final AtomicInteger openCounter, final AtomicInteger closeCounter) {
            this.openCounts = openCounter.incrementAndGet();
            this.closeCounter = closeCounter;
            this.failOnClose = false;
        }

        private CountingCloseable(final AtomicInteger openCounter, final AtomicInteger closeCounter, boolean failOnClose) {
            this.openCounts = openCounter.incrementAndGet();
            this.closeCounter = closeCounter;
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() throws IOException {
            if (failOnClose) {
                throw new IOException("an error");
            } else {
                closeCounter.incrementAndGet();
            }
        }
    }
}
