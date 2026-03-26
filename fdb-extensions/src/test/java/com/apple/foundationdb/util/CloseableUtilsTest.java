/*
 * CloseableUtilsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link CloseableUtils} class.
 */
class CloseableUtilsTest {

    @Test
    void closeAllNoIssue() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(false, null);
        SimpleCloseable c2 = new SimpleCloseable(false, null);
        SimpleCloseable c3 = new SimpleCloseable(false, null);

        CloseableUtils.closeAll(c1, c2, c3);

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    @Test
    void closeAllEmpty() {
        // No closeables — should succeed without exception
        Assertions.assertDoesNotThrow(() -> CloseableUtils.closeAll());
    }

    @Test
    void closeAllSingleSuccess() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(false, null);
        CloseableUtils.closeAll(c1);
        Assertions.assertTrue(c1.isClosed());
    }

    @Test
    void closeAllSingleFailure() {
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        final CloseException exception = assertThrows(CloseException.class, () -> CloseableUtils.closeAll(c1));
        Assertions.assertEquals("c1", exception.getCause().getMessage());
        Assertions.assertEquals(0, exception.getSuppressed().length);
        Assertions.assertTrue(c1.isClosed());
    }

    @Test
    void closeAllInterrupted() {
        Thread.interrupted(); // clear any pre-existing interrupt flag
        InterruptingCloseable ic = new InterruptingCloseable();
        final CloseException exception = assertThrows(CloseException.class,
                () -> CloseableUtils.closeAll(ic));
        Assertions.assertInstanceOf(InterruptedException.class, exception.getCause());
        Assertions.assertEquals(0, exception.getSuppressed().length);
        Assertions.assertTrue(ic.isClosed());
        Assertions.assertTrue(Thread.interrupted()); // flag should have been restored; also clears it
    }

    @Test
    void closeAllInterruptedAmongOtherFailures() {
        Thread.interrupted(); // clear any pre-existing interrupt flag
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        InterruptingCloseable c2 = new InterruptingCloseable();
        SimpleCloseable c3 = new SimpleCloseable(false, null);
        final CloseException exception = assertThrows(CloseException.class,
                () -> CloseableUtils.closeAll(c1, c2, c3));
        Assertions.assertEquals("c1", exception.getCause().getMessage());
        Assertions.assertEquals(1, exception.getSuppressed().length);
        Assertions.assertInstanceOf(InterruptedException.class, exception.getSuppressed()[0]);
        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
        Assertions.assertTrue(Thread.interrupted()); // flag should have been restored; also clears it
    }

    @Test
    void closeAllFailed() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        SimpleCloseable c2 = new SimpleCloseable(true, "c2");
        SimpleCloseable c3 = new SimpleCloseable(true, "c3");

        final CloseException exception = assertThrows(CloseException.class, () -> CloseableUtils.closeAll(c1, c2, c3));

        Assertions.assertEquals("c1", exception.getCause().getMessage());
        final Throwable[] suppressed = exception.getSuppressed();
        Assertions.assertEquals(2, suppressed.length);
        Assertions.assertEquals("c2", suppressed[0].getMessage());
        Assertions.assertEquals("c3", suppressed[1].getMessage());

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    @Test
    void closeSomeFailed() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        SimpleCloseable c2 = new SimpleCloseable(false, null);
        SimpleCloseable c3 = new SimpleCloseable(true, "c3");

        final CloseException exception = assertThrows(CloseException.class, () -> CloseableUtils.closeAll(c1, c2, c3));

        Assertions.assertEquals("c1", exception.getCause().getMessage());
        final Throwable[] suppressed = exception.getSuppressed();
        Assertions.assertEquals(1, suppressed.length);
        Assertions.assertEquals("c3", suppressed[0].getMessage());

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    @Test
    void invokeAllSuccess() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(
                List.of(successCallback("s1"), successCallback("s2"), successCallback("s3")));
        Assertions.assertNull(result.getAccumulatedException());
        Assertions.assertEquals(List.of("s1", "s2", "s3"), result.getResults());
    }

    @Test
    void invokeAllEmpty() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(List.of());
        Assertions.assertNull(result.getAccumulatedException());
        Assertions.assertEquals(List.of(), result.getResults());
    }

    @Test
    void invokeAllSomeFail() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(
                List.of(successCallback("s1"), failureCallback("s2"), successCallback("s3")));
        Assertions.assertNotNull(result.getAccumulatedException());
        Assertions.assertEquals(CloseException.class, result.getAccumulatedException().getClass());
        Assertions.assertEquals("s2", result.getAccumulatedException().getCause().getMessage());
        Assertions.assertEquals(List.of("s1", "s3"), result.getResults());
    }

    @Test
    void invokeAllAllFail() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(
                List.of(failureCallback("f1"), failureCallback("f2"), failureCallback("f3")));
        Assertions.assertNotNull(result.getAccumulatedException());
        Assertions.assertEquals("f1", result.getAccumulatedException().getCause().getMessage());
        final Throwable[] suppressed = result.getAccumulatedException().getSuppressed();
        Assertions.assertEquals(2, suppressed.length);
        Assertions.assertEquals("f2", suppressed[0].getMessage());
        Assertions.assertEquals("f3", suppressed[1].getMessage());
        Assertions.assertEquals(List.of(), result.getResults());
    }

    @Test
    void invokeAllLastFails() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(
                List.of(successCallback("s1"), successCallback("s2"), failureCallback("f3")));
        Assertions.assertNotNull(result.getAccumulatedException());
        Assertions.assertEquals("f3", result.getAccumulatedException().getCause().getMessage());
        Assertions.assertEquals(0, result.getAccumulatedException().getSuppressed().length);
        Assertions.assertEquals(List.of("s1", "s2"), result.getResults());
    }

    @Test
    void invokeAllNullReturn() {
        final CloseableUtils.InvokeResults<String> result = CloseableUtils.invokeAll(
                List.of(successCallback(null), successCallback("s2")));
        Assertions.assertNull(result.getAccumulatedException());
        Assertions.assertEquals(2, result.getResults().size());
        Assertions.assertNull(result.getResults().get(0));
        Assertions.assertEquals("s2", result.getResults().get(1));
    }

    @Test
    void invokeAllFuturesEmpty() {
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(List.of());
        Assertions.assertNull(future.join());
    }

    @Test
    void invokeAllFuturesSuccess() {
        final AtomicBoolean f1Done = new AtomicBoolean(false);
        final AtomicBoolean f2Done = new AtomicBoolean(false);
        final AtomicBoolean f3Done = new AtomicBoolean(false);
        final CompletableFuture<Void> result = CloseableUtils.invokeAllFutures(List.of(
                tracked(futureSuccess("s1"), f1Done),
                tracked(futureSuccess("s2"), f2Done),
                tracked(futureSuccess("s3"), f3Done)));
        Assertions.assertNull(result.join());
        Assertions.assertTrue(f1Done.get());
        Assertions.assertTrue(f2Done.get());
        Assertions.assertTrue(f3Done.get());
    }

    @Test
    void invokeAllFuturesOneFails() {
        final AtomicBoolean f1Done = new AtomicBoolean(false);
        final AtomicBoolean f2Done = new AtomicBoolean(false);
        final AtomicBoolean f3Done = new AtomicBoolean(false);
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(List.of(
                tracked(futureSuccess("s1"), f1Done),
                tracked(futureFailure("f2"), f2Done),
                tracked(futureSuccess("s3"), f3Done)));
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        Assertions.assertInstanceOf(CloseException.class, exception.getCause());
        final CloseException closeException = (CloseException) exception.getCause();
        // allOf wraps non-CompletionException causes in a CompletionException before propagating
        Assertions.assertInstanceOf(CompletionException.class, closeException.getCause());
        Assertions.assertEquals("f2", closeException.getCause().getCause().getMessage());
        Assertions.assertEquals(0, closeException.getSuppressed().length);
        Assertions.assertTrue(f1Done.get());
        Assertions.assertTrue(f2Done.get());
        Assertions.assertTrue(f3Done.get());
    }

    @Test
    void invokeAllFuturesMultipleFail() {
        final AtomicBoolean f1Done = new AtomicBoolean(false);
        final AtomicBoolean f2Done = new AtomicBoolean(false);
        final AtomicBoolean f3Done = new AtomicBoolean(false);
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(List.of(
                tracked(futureFailure("f1"), f1Done),
                tracked(futureSuccess("s2"), f2Done),
                tracked(futureFailure("f3"), f3Done)));
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        final CloseException closeException = (CloseException) exception.getCause();
        Assertions.assertEquals("f1", closeException.getCause().getCause().getMessage());
        Assertions.assertEquals(0, closeException.getSuppressed().length);
        // When multiple futures fail, whenAll only throws one of the exceptions, so we can't assert on f3
        Assertions.assertTrue(f1Done.get());
        Assertions.assertTrue(f2Done.get());
        Assertions.assertTrue(f3Done.get());
    }

    @Test
    void invokeAllFuturesSupplierThrows() {
        // The supplier itself throws before returning a future; the other futures still complete
        final AtomicBoolean f1Done = new AtomicBoolean(false);
        final AtomicBoolean f3Done = new AtomicBoolean(false);
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(List.of(
                tracked(futureSuccess("s1"), f1Done),
                throwingSupplier("t2"),
                tracked(futureSuccess("s3"), f3Done)));
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        Assertions.assertInstanceOf(CloseException.class, exception.getCause());
        final CloseException closeException = (CloseException) exception.getCause();
        // The CloseException's cause is the exception thrown by the supplier
        Assertions.assertEquals("t2", closeException.getCause().getMessage());
        Assertions.assertEquals(0, closeException.getSuppressed().length);
        Assertions.assertTrue(f1Done.get());
        Assertions.assertTrue(f3Done.get());
    }

    @Test
    void invokeAllFuturesAllSupplierThrow() {
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(
                List.of(throwingSupplier("t1"), throwingSupplier("t2")));
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        Assertions.assertInstanceOf(CloseException.class, exception.getCause());
        final CloseException closeException = (CloseException) exception.getCause();
        Assertions.assertEquals("t1", closeException.getCause().getMessage());
        Assertions.assertEquals(1, closeException.getSuppressed().length);
        Assertions.assertEquals("t2", closeException.getSuppressed()[0].getMessage());
    }

    @Test
    void invokeAllFuturesSupplierThrowsAndFutureFails() {
        // One supplier throws during creation; a separate successfully-created future also fails
        final AtomicBoolean f1Done = new AtomicBoolean(false);
        final CompletableFuture<Void> future = CloseableUtils.invokeAllFutures(List.of(
                tracked(futureFailure("f1"), f1Done),
                throwingSupplier("t2")));
        final CompletionException exception = assertThrows(CompletionException.class, future::join);
        Assertions.assertInstanceOf(CloseException.class, exception.getCause());
        final CloseException closeException = (CloseException) exception.getCause();
        // The supplier exception is the primary cause; the future failure is suppressed
        Assertions.assertEquals("t2", closeException.getCause().getMessage());
        Assertions.assertEquals(1, closeException.getSuppressed().length);
        Assertions.assertEquals("f1", closeException.getSuppressed()[0].getCause().getMessage());
        Assertions.assertTrue(f1Done.get());
    }

    private Supplier<String> successCallback(final String str) {
        return () -> str;
    }

    private Supplier<String> failureCallback(final String str) {
        return () -> {
            throw new RuntimeException(str);
        };
    }

    private CompletableFuture<String> futureSuccess(final String str) {
        return CompletableFuture.supplyAsync(() -> str);
    }

    private CompletableFuture<String> futureFailure(final String msg) {
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException(msg);
        });
    }

    // Utility to track that a future is complete by setting a given AtomicBoolean to TRUE
    private <T> Supplier<CompletableFuture<T>> tracked(final CompletableFuture<T> future, final AtomicBoolean completedFlag) {
        return () -> future.whenComplete((r, e) -> completedFlag.set(true));
    }

    private Supplier<CompletableFuture<String>> throwingSupplier(final String msg) {
        return () -> {
            throw new RuntimeException(msg);
        };
    }

    private static class SimpleCloseable implements AutoCloseable {
        private final boolean fail;
        private final String message;
        private boolean closed = false;

        public SimpleCloseable(boolean fail, String message) {
            this.fail = fail;
            this.message = message;
        }

        @Override
        public void close() {
            closed = true;
            if (fail) {
                throw new RuntimeException(message);
            }
        }

        public boolean isClosed() {
            return closed;
        }
    }

    @SuppressWarnings("try")
    private static class InterruptingCloseable implements AutoCloseable {
        private boolean closed = false;

        @Override
        public void close() throws InterruptedException {
            closed = true;
            throw new InterruptedException("interrupted");
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
