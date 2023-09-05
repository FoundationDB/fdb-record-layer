package com.apple.foundationdb.record.lucene.codec;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;

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
    void testThrowsIOException() {
        IOException thrownException = new IOException("test foo");
        final LazyOpener<Object> opener = LazyOpener.supply(() -> {
            throw thrownException;
        });
        final IOException resultingException = assertThrows(IOException.class, opener::get);
        assertSame(thrownException, resultingException);
    }

    @Test
    void testThrowsUncheckedIOException() {
        IOException thrownException = new IOException("test foo");
        final LazyOpener<Object> opener = LazyOpener.supply(() -> {
            throw thrownException;
        });
        final UncheckedIOException resultingException = assertThrows(UncheckedIOException.class, opener::getUnchecked);
        assertSame(thrownException, resultingException.getCause());
    }
}
