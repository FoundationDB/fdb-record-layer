/*
 * FutureAutoCloseTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.runners;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class FutureAutoCloseTest {
    @Test
    void closeClosesFutures() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.registerFuture(new CompletableFuture<>());
            f2 = classUnderTest.newFuture();
        }

        assertCompletedExceptionally(f1);
        assertCompletedExceptionally(f2);
    }

    @Test
    void registerAfterClose() {
        FutureAutoClose classUnderTest = new FutureAutoClose();
        classUnderTest.registerFuture(new CompletableFuture<>());
        classUnderTest.newFuture();
        classUnderTest.close();

        Assertions.assertThatThrownBy(classUnderTest::newFuture).isInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
    }

    @Test
    void closeIgnoresCompletedFutures() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.registerFuture(new CompletableFuture<>());
            f2 = classUnderTest.newFuture();

            f1.complete(null);
            f2.complete(null);
        }

        assertCompletedNormally(f1);
        assertCompletedNormally(f2);
    }

    @Test
    void closeIgnoreCompletedFuturesMixed() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        CompletableFuture<Void> f3;
        CompletableFuture<Void> f4;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.registerFuture(new CompletableFuture<>());
            f2 = classUnderTest.registerFuture(new CompletableFuture<>());
            f3 = classUnderTest.newFuture();
            f4 = classUnderTest.newFuture();

            f1.complete(null);
            f3.complete(null);
        }

        assertCompletedNormally(f1);
        assertCompletedExceptionally(f2);
        assertCompletedNormally(f3);
        assertCompletedExceptionally(f4);
    }

    @Test
    void futureWithDependentCompletesExceptionally() {
        CompletableFuture<Void> f1;
        CompletableFuture<Boolean> f2;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.newFuture();
            f2 = f1.thenApply(ignore -> true);
        }

        assertCompletedExceptionally(f1);
        assertCompletedExceptionally(f2);
    }

    @Test
    void futureWithDependentCompletesNormally() {
        CompletableFuture<Void> f1;
        CompletableFuture<Boolean> f2;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.newFuture();
            f2 = f1.thenApply(ignore -> true);
            f1.complete(null);
        }

        assertCompletedNormally(f1);
        assertCompletedNormally(f2);
    }

    @Test
    void futureCreatesAnotherCompletesNormally() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        final AtomicReference<CompletableFuture<Void>> f3 = new AtomicReference<>();
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.newFuture();
            f2 = f1.thenApply(ignore -> {
                f3.set(classUnderTest.newFuture());
                return null;
            });
            f1.complete(null);
        }

        assertCompletedNormally(f1);
        assertCompletedNormally(f2);
        assertCompletedExceptionally(f3.get());
    }

    @Test
    void futureCreatesAnotherAfterClose() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        final AtomicReference<CompletableFuture<Void>> f3 = new AtomicReference<>();
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.newFuture();
            f2 = f1.handle((val, ex) -> {
                // This would happen during the close() call, in the same thread
                Assertions.assertThatThrownBy(classUnderTest::newFuture).isInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
                return null;
            });
        }

        assertCompletedExceptionally(f1);
        assertCompletedNormally(f2);
    }

    @Test
    void nestedClose() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        FutureAutoClose classUnderTest = new FutureAutoClose();
        f1 = classUnderTest.newFuture();
        f2 = f1.handle((val, ex) -> {
            // This would happen during the close() call, in the same thread
            classUnderTest.close();
            return null;
        });
        classUnderTest.close();

        assertCompletedExceptionally(f1);
        assertCompletedNormally(f2);
    }

    @Test
    void futureThrowsException() {
        CompletableFuture<Void> f1;
        CompletableFuture<Void> f2;
        CompletableFuture<Void> f3;
        try (FutureAutoClose classUnderTest = new FutureAutoClose()) {
            f1 = classUnderTest.newFuture();
            f2 = f1.handle((val, ex) -> {
                throw new RuntimeException("Blah");
            });
            f3 = classUnderTest.newFuture();
        }

        assertCompletedExceptionally(f1);
        assertCompletedExceptionally(f2, RuntimeException.class);
        assertCompletedExceptionally(f3);
    }

    private void assertCompletedNormally(final CompletableFuture<?> future) {
        future.join();
    }

    private void assertCompletedExceptionally(final CompletableFuture<?> future) {
        assertCompletedExceptionally(future, FDBDatabaseRunner.RunnerClosed.class);
    }

    private void assertCompletedExceptionally(final CompletableFuture<?> future, Class<? extends Exception> ex) {
        Assertions.assertThat(future.isCompletedExceptionally()).isTrue();
        Assertions.assertThatThrownBy(future::join).hasCauseInstanceOf(ex);
    }
}
