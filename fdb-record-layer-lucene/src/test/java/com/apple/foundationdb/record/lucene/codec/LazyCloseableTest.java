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

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LazyCloseableTest {

    @Test
    void testOpensLazilyExactlyOnce() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        try (LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(
                () -> new CountingCloseable(openCounter.incrementAndGet(), closeCounter))) {
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
    void testCloseDoesNotOpen() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        LazyCloseable.supply(() -> new CountingCloseable(openCounter.incrementAndGet(), closeCounter)).close();
        assertEquals(0, openCounter.get());
    }

    @Test
    void testCloseCloses() throws IOException {
        final AtomicInteger openCounter = new AtomicInteger(0);
        final AtomicInteger closeCounter = new AtomicInteger(0);
        try (LazyCloseable<CountingCloseable> opener = LazyCloseable.supply(() -> new CountingCloseable(openCounter.incrementAndGet(), closeCounter))) {
            opener.get();
        }
        assertEquals(1, openCounter.get());
        assertEquals(1, closeCounter.get());
    }

    @Test
    void testThrowsIoException() {
        IOException thrownException = new IOException("test foo");
        final LazyCloseable<Closeable> opener = LazyCloseable.supply(() -> {
            throw thrownException;
        });
        final IOException resultingException = assertThrows(IOException.class, opener::get);
        assertSame(thrownException, resultingException);
    }

    @Test
    void testThrowsUncheckedIoException() {
        IOException thrownException = new IOException("test foo");
        final LazyCloseable<Closeable> opener = LazyCloseable.supply(() -> {
            throw thrownException;
        });
        final UncheckedIOException resultingException = assertThrows(UncheckedIOException.class, opener::getUnchecked);
        assertSame(thrownException, resultingException.getCause());
    }

    private static class CountingCloseable implements Closeable {
        final int openCounts;
        final AtomicInteger closeCounter;

        private CountingCloseable(final int openCounts, final AtomicInteger closeCounter) {
            this.openCounts = openCounts;
            this.closeCounter = closeCounter;
        }

        @Override
        public void close() {
            closeCounter.incrementAndGet();
        }
    }
}
