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
    void testThrowsIOException() {
        IOException thrownException = new IOException("test foo");
        final LazyCloseable<Closeable> opener = LazyCloseable.supply(() -> {
            throw thrownException;
        });
        final IOException resultingException = assertThrows(IOException.class, opener::get);
        assertSame(thrownException, resultingException);
    }

    @Test
    void testThrowsUncheckedIOException() {
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
