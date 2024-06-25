/*
 * futuretest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FutureTest {


    public static Stream<Combination> combinations() {
        return Stream.of(
                /*
                 * What {@code FDBRecordStoreStateCacheEntry} does now. This does not work well because, as far as I can tell,
                 * in situation where the Batch GRV limit is exceeded,
                 * {@code readTransaction(isolationLevel.isSnapshot()).get(SystemKeyspace.METADATA_VERSION_KEY)} will never complete,
                 * whereas {@code recordStore.loadRecordStoreStateAsync} will complete with a Batch GRV limit exceeded error.
                 * It looks like {@code thenCombine} will always wait for both to complete, even if it could throw an error sooner.
                 */
                named("thenCombine", (future1, future2) -> future1.thenCombine(future2, (a, b) -> a + ":" + b)),
                /*
                 * One workaround to this problem, would be to have the second future completed exceptionally if the first future
                 * completed exceptionally. This works, but feels less than ideal because the causes get confused, not that the
                 * stacks will be particularly informative in the end.
                 */
                named("completeExceptionallyAndCombine", (future1, future2) -> {
                    future1.whenComplete((result, error) -> {
                        if (error != null) {
                            future2.completeExceptionally(error);
                        }
                    });

                    return future1.thenCombine(future2, (a, b) -> a + ":" + b);
                }),
                named("manuallyCombine" ,(future1, future2) -> {
                    final CompletableFuture<String> combined = new CompletableFuture<>();

                    future1.whenComplete((result, error) -> {
                        if (error != null) {
                            combined.completeExceptionally(error);
                        } else {
                            future2.whenComplete((result2, error2) -> {
                                if (error2 != null) {
                                    combined.completeExceptionally(error2);
                                } else {
                                    combined.complete(result + ":" + result2);
                                }
                            });
                        }
                    });
                    return combined;
                })


        );
    }

    private static Combination named(final String name, final Combination input) {
        return new Combination() {
            @Override
            public CompletableFuture<String> apply(final CompletableFuture<String> stringCompletableFuture, final CompletableFuture<Integer> integerCompletableFuture) {
                return input.apply(stringCompletableFuture, integerCompletableFuture);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void failure1uncompleted2(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        final RuntimeException ex = new RuntimeException("Fail 1");
        future1.completeExceptionally(ex);

        // Times out for `thenCombine`, and succeeds for the other two
        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex, executionException.getCause());
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void uncompleted1failure2(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        final RuntimeException ex = new RuntimeException("Fail 2");
        future2.completeExceptionally(ex);

        // Times out for all 3
        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex, executionException.getCause());
    }


    @ParameterizedTest
    @MethodSource("combinations")
    void failure1failure2(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        final RuntimeException ex2 = new RuntimeException("Fail 2");
        future2.completeExceptionally(ex2);
        final RuntimeException ex = new RuntimeException("Fail 1");
        future1.completeExceptionally(ex);

        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex, executionException.getCause());
    }


    @ParameterizedTest
    @MethodSource("combinations")
    void success1failure2(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        future1.complete("result1");
        final RuntimeException ex2 = new RuntimeException("Fail 2");
        future2.completeExceptionally(ex2);

        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex2, executionException.getCause());
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void success1failure2Reversed(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        final RuntimeException ex2 = new RuntimeException("Fail 2");
        future2.completeExceptionally(ex2);
        future1.complete("result1");

        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex2, executionException.getCause());
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void failure1success2(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        final RuntimeException ex = new RuntimeException("Fail 1");
        future1.completeExceptionally(ex);
        future2.complete(7);

        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex, executionException.getCause());
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void failure1success2Reversed(Combination combine) {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        future2.complete(7);
        final RuntimeException ex = new RuntimeException("Fail 1");
        future1.completeExceptionally(ex);

        final ExecutionException executionException = assertThrows(ExecutionException.class, () -> combined.get(10, TimeUnit.SECONDS));
        assertSame(ex, executionException.getCause());
    }


    @ParameterizedTest
    @MethodSource("combinations")
    void success1success2(Combination combine) throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        future1.complete("foo");
        future2.complete(7);

        assertEquals("foo:7", combined.get(10, TimeUnit.SECONDS));
    }


    @ParameterizedTest
    @MethodSource("combinations")
    void success1success2Reversed(Combination combine) throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<String> future1 = new CompletableFuture<>();
        final CompletableFuture<Integer> future2 = new CompletableFuture<>();
        final CompletableFuture<String> combined = combine.apply(future1, future2);

        future2.complete(7);
        future1.complete("foo");

        assertEquals("foo:7", combined.get(10, TimeUnit.SECONDS));
    }


    private interface Combination {
        CompletableFuture<String> apply(final CompletableFuture<String> future1, final CompletableFuture<Integer> future2);
    }
}
