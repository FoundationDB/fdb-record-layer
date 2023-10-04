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
