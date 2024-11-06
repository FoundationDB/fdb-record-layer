/*
 * AsyncLoadingCacheTest.java
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
import com.apple.foundationdb.async.MoreAsyncUtil.DeadlineExceededException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static com.apple.foundationdb.record.TestHelpers.consistently;
import static com.apple.foundationdb.record.TestHelpers.eventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class AsyncLoadingCacheTest {
    private final Random random = new Random();

    private static <K, V> AsyncLoadingCache<K, V> createCache(long refreshTimeMillis) {
        return new AsyncLoadingCache<>(refreshTimeMillis, AsyncLoadingCache.DEFAULT_DEADLINE_TIME_MILLIS, AsyncLoadingCache.UNLIMITED, MoreAsyncUtil.getDefaultScheduledExecutor());
    }

    @Test
    public void testRefreshTime() {
        AsyncLoadingCache<String, Boolean> cachedResult = createCache(100);
        final AtomicBoolean called = new AtomicBoolean(false);

        Supplier<CompletableFuture<Boolean>> supplier = () -> CompletableFuture.completedFuture(called.getAndSet(true));

        consistently("we see the cached value up until the expiration",
                () -> cachedResult.orElseGet("a", supplier).join(), is(false), 75, 10);
        eventually("the cached result expires and we start reading true",
                () -> cachedResult.orElseGet("a", supplier).join(), is(true), 100, 10);
        consistently("any subsequent reads are true",
                () -> cachedResult.orElseGet("a", supplier).join(), is(true), 50, 10);
    }

    @Test
    public void testSupplierExceptionDoesNotCacheValue() {
        AsyncLoadingCache<Integer, Boolean> cachedResult = createCache(30000);
        final AtomicInteger counter = new AtomicInteger();
        final Supplier<CompletableFuture<Boolean>> supplier = () -> {
            counter.incrementAndGet();
            throw new RecordCoreException("this is only a test");
        };

        for (int i = 0; i < 10; i++) {
            try {
                cachedResult.orElseGet(1, supplier).join();
                fail("should throw RecordCoreException");
            } catch (RecordCoreException e) {
                assertThat(e.getMessage(), containsString("failed getting value"));
                assertThat(e.getCause().getMessage(), containsString("this is only a test"));
            }
        }

        assertThat("we have to call the supplier each time", counter.get(), is(10));
    }

    @Test
    public void testGettingAsyncFailures() {
        AsyncLoadingCache<Integer, Boolean> cachedResult = createCache(30000);
        final AtomicInteger callCount = new AtomicInteger();
        final Supplier<CompletableFuture<Boolean>> supplier = () ->
                MoreAsyncUtil.delayedFuture(1 + random.nextInt(5), TimeUnit.MILLISECONDS).thenApply(ignore -> {
                    int count = callCount.getAndIncrement();
                    if (count == 0) {
                        // fail on first call
                        throw new RecordCoreException("this is only a test");
                    }
                    return true;
                });

        try {
            cachedResult.orElseGet(1, supplier).join();
            fail("should throw exception");
        } catch (CompletionException ex) {
            assertThat("we got the expected exception", ex.getCause(), is(instanceOf(RecordCoreException.class)));
            assertThat("it's the test exception", ex.getCause().getMessage(), containsString("this is only a test"));
        }
        assertThat("before future is ready we return the in progress cached future", callCount.get(), is(1));

        cachedResult.orElseGet(1, supplier).join();
        assertThat("after cached future completes exceptionally we attempt to get the value again", callCount.get(), is(2));
    }

    @Test
    public void testGettingImmediateFailure() {
        AsyncLoadingCache<Integer, Boolean> cachedResult = createCache(30000);
        final AtomicInteger callCount = new AtomicInteger();
        final Supplier<CompletableFuture<Boolean>> supplier = () -> {
            int count = callCount.getAndIncrement();
            if (count == 0) {
                // fail on first call
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                RecordCoreException e = new RecordCoreException("this is only a test");
                future.completeExceptionally(e);
                return future;
            }
            return CompletableFuture.completedFuture(true);
        };

        try {
            cachedResult.orElseGet(1, supplier).join();
            fail("should throw exception");
        } catch (CompletionException ex) {
            assertThat("we got the expected exception", ex.getCause(), is(instanceOf(RecordCoreException.class)));
            assertThat("it's the test exception", ex.getCause().getMessage(), containsString("this is only a test"));
        }
        assertThat("before future is ready we return the in progress cached future", callCount.get(), is(1));

        cachedResult.orElseGet(1, supplier).join();
        assertThat("after cached future completes exceptionally we attempt to get the value again", callCount.get(), is(2));
    }

    @Test
    public void testReloadFailedGets() throws Exception {
        AsyncLoadingCache<String, Integer> cachedResult = createCache(250);
        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();

        for (int i = 1; i <= 3; i++) {
            assertThat(cachedResult.orElseGet("k1", getSupplier(234, counter1, false)).join(), is(234));
            assertThat("we do not call the supplier while the cache is valid", counter1.get(), is(1));

            assertThrows(CompletionException.class, () -> cachedResult.orElseGet("k2", getSupplier(987, counter2, true)).join());
            assertThat("we retry the supplier after a failure", counter2.get(), is(i));
        }

    }

    private Supplier<CompletableFuture<Integer>> getSupplier(int result, AtomicInteger counter, boolean shouldFail) {
        return () -> MoreAsyncUtil.delayedFuture(1 + random.nextInt(5), TimeUnit.MILLISECONDS).thenApply(ignore -> {
            counter.incrementAndGet();
            if (shouldFail) {
                throw new RecordCoreException("async failure");
            }
            return result;
        });
    }

    @Test
    public void testClear() {
        AsyncLoadingCache<String, Integer> cachedResult = createCache(30000);
        AtomicInteger value = new AtomicInteger(111);
        Supplier<CompletableFuture<Integer>> supplier = () -> CompletableFuture.supplyAsync(value::get);

        consistently("we get the original value", () -> cachedResult.orElseGet("a-key", supplier).join(), is(111), 10, 2);
        value.getAndSet(222);
        consistently("we still see the cached value", () -> cachedResult.orElseGet("a-key", supplier).join(), is(111), 10, 2);
        cachedResult.clear();
        consistently("we see the new value", () -> cachedResult.orElseGet("a-key", supplier).join(), is(222), 10, 2);
    }

    @Test
    public void testParallelGets() {
        AsyncLoadingCache<String, Boolean> cachedResult = createCache(100);
        final AtomicInteger counter = new AtomicInteger();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        final Supplier<CompletableFuture<Boolean>> supplier = () -> {
            counter.incrementAndGet();
            return signal.thenApply(ignored -> true);
        };

        List<String> keys = ImmutableList.of("key-1", "key-2", "key-3");
        List<CompletableFuture<Boolean>> parallelOperations = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            for (String key : keys) {
                parallelOperations.add(cachedResult.orElseGet(key, supplier));
            }
        }

        signal.complete(null);
        List<Boolean> values = AsyncUtil.getAll(parallelOperations).join();
        for (Boolean value : values) {
            assertTrue(value);
        }

        // Don't increment after futures have already completed
        List<CompletableFuture<Boolean>> afterCompleteOperations = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (String key : keys) {
                afterCompleteOperations.add(cachedResult.orElseGet(key, supplier));
            }
        }
        values = AsyncUtil.getAll(afterCompleteOperations).join();
        for (Boolean value : values) {
            assertTrue(value);
        }
        assertThat("supplier is called once per incomplete access", counter.get(), is(parallelOperations.size()));
    }

    @Test
    public void cacheNulls() {
        AsyncLoadingCache<String, String> cache = createCache(100);
        final AtomicInteger counter = new AtomicInteger();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        final Supplier<CompletableFuture<String>> supplier = () -> {
            counter.incrementAndGet();
            return signal.thenApply(ignored -> null);
        };

        CompletableFuture<String> future = cache.orElseGet("key", supplier);
        assertEquals(1, counter.get());
        signal.complete(null);
        String value = future.join();
        assertNull(value);

        CompletableFuture<String> cachedFuture = cache.orElseGet("key", supplier);
        assertEquals(1, counter.get()); // supplier should not need to run
        value = cachedFuture.join();
        assertNull(value);
    }

    @Test
    public void testDeadline() {
        AsyncLoadingCache<String, Integer> cachedResult = new AsyncLoadingCache<>(100, 10, AsyncLoadingCache.UNLIMITED, MoreAsyncUtil.getDefaultScheduledExecutor());
        final Supplier<CompletableFuture<Integer>> tooLateSupplier = () -> MoreAsyncUtil.delayedFuture(1, TimeUnit.SECONDS)
                .thenApply(ignore -> 2);
        final Supplier<CompletableFuture<Integer>> onTimeSupplier = () -> MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS)
                .thenApply(ignore -> 3);

        try {
            cachedResult.orElseGet("a-key", tooLateSupplier).join();
            fail("should throw CompletionException");
        } catch (CompletionException ex) {
            assertThat("it is caused by a deadline exception", ex.getCause(),
                    is(instanceOf(DeadlineExceededException.class)));
            assertThat(ex.getCause(), hasMessageContaining("deadline exceeded"));
        }

        assertThat("we get the value before the deadline", cachedResult.orElseGet("a-key", onTimeSupplier).join(), is(3));
    }
}
