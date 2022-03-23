/*
 * LimitedRunnerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// if any of these tests take longer than 2 seconds, it almost certainly indicates a bug resulting in the future
// never completing
@Timeout(value = 2, unit = TimeUnit.SECONDS)
class LimitedRunnerTest {

    private final Executor executor = ForkJoinPool.commonPool();

    @BeforeAll
    static void beforeAll() {
        // We do this because checking whether an exception is retryable or not requires the version to be set
        FDB.selectAPIVersion(630);
    }

    public static Stream<Arguments> allCauseTypes() {
        return Stream.of(retriableNonLessenWorkException(),
                retryAndLessenWorkException(),
                lessenWorkException(),
                nonRetriableException())
                .map(Arguments::of);
    }

    @Test
    void completesInOnePass() {
        List<Integer> limits = new ArrayList<>();
        new LimitedRunner(executor, 10, mockDelay()).runAsync(limit -> {
            limits.add(limit);
            return AsyncUtil.READY_FALSE;
        }, List.of()).join();
        assertEquals(List.of(10), limits);
    }

    @Test
    void loopsSuccessfully() {
        List<Integer> limits = new ArrayList<>();
        new LimitedRunner(executor, 10, mockDelay()).runAsync(limit -> {
            limits.add(limit);
            return limits.size() < 20 ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
        }, List.of()).join();
        assertEquals(20, limits.size());
        // If we ever change the starting limit to be less than the max limit, this should start at 10, but go up
        assertEquals(Set.of(10), Set.copyOf(limits), "The limit should not decrease");
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void failsWithRetriableNonLessenWork(ExceptionStyle exceptionStyle) {
        final RuntimeException cause = exceptionStyle.wrap(retriableNonLessenWorkException());
        List<Integer> limits = new ArrayList<>();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay())
                        .setDecreaseLimitAfter(3)
                        .runAsync(limit -> {
                            limits.add(limit);
                            return exceptionStyle.hasMore(cause);
                        }, List.of()).join());
        assertEquals(cause, completionException.getCause());
        assertThat(limits, Matchers.hasSize(3));
        for (Integer limit : limits) {
            assertEquals(10, limit);
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void failWithLessenWork(ExceptionStyle exceptionStyle) {
        final RuntimeException cause = exceptionStyle.wrap(lessenWorkException());
        List<Integer> limits = new ArrayList<>();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay()).runAsync(limit -> {
                    limits.add(limit);
                    return exceptionStyle.hasMore(cause);
                }, List.of()).join());
        assertEquals(cause, completionException.getCause());
        assertEquals(10, limits.get(0));
        assertEquals(1, limits.get(limits.size() - 1));
        for (int i = 1; i < limits.size(); i++) {
            assertThat(limits.get(i), Matchers.lessThanOrEqualTo(limits.get(i - 1)));
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void failWithRetryAndLessenWork(ExceptionStyle exceptionStyle) {
        // If the exception being thrown is retriable, but could indicate that we are also doing too much
        // work, we want to retry a few times at each limit.
        final RuntimeException cause = exceptionStyle.wrap(retryAndLessenWorkException());
        List<Integer> limits = new ArrayList<>();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay())
                        .setDecreaseLimitAfter(3)
                        .runAsync(limit -> {
                            limits.add(limit);
                            return exceptionStyle.hasMore(cause);
                        }, List.of()).join());
        assertEquals(cause, completionException.getCause());
        assertEquals(10, limits.get(0));
        assertEquals(1, limits.get(limits.size() - 1));
        for (int i = 1; i < limits.size(); i++) {
            assertThat(buildPointerMessage(limits, i),
                    limits.get(i), Matchers.lessThanOrEqualTo(limits.get(i - 1)));
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(10, limits.get(i), buildPointerMessage(limits, i));
        }
        for (int i = 4; i < 6; i++) {
            String message = buildPointerMessage(limits, i);
            assertThat(message, limits.get(i), Matchers.lessThan(10));
            assertEquals(limits.get(3), limits.get(i), message);
        }
        for (int i = 7; i < 9; i++) {
            String message = buildPointerMessage(limits, i);
            assertThat(message, limits.get(i), Matchers.lessThan(limits.get(5)));
            assertEquals(limits.get(6), limits.get(i), message);
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void failWithNonFDBException(ExceptionStyle exceptionStyle) {
        failWithNonRetriableException(exceptionStyle, new NullPointerException());
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void failWithNonRetriableException(ExceptionStyle exceptionStyle) {
        failWithNonRetriableException(exceptionStyle, nonRetriableException());
    }

    void failWithNonRetriableException(ExceptionStyle exceptionStyle, final RuntimeException rootCause) {
        final RuntimeException cause = exceptionStyle.wrap(rootCause);
        List<Integer> limits = new ArrayList<>();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay())
                        .setDecreaseLimitAfter(3)
                        .runAsync(limit -> {
                            limits.add(limit);
                            return exceptionStyle.hasMore(cause);
                        }, List.of()).join());
        assertEquals(cause, completionException.getCause());
        assertEquals(List.of(10), limits);
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void increaseAfter(ExceptionStyle exceptionStyle) {
        final RuntimeException cause = exceptionStyle.wrap(lessenWorkException());
        List<Integer> limits = new ArrayList<>();
        // we decrease on the 4th of 5 elements, and increase after 3 successes
        // so that should be something like:
        // 12 12 12 12 08 08 08 12 12 12
        //  1  2  3  4  0  1  2  3  4  0
        //           F                 F
        // Note: I'm picking an even multiple of 4 here, because we do 3/4 decrease and 4/3 and this means there's
        // no rounding
        new LimitedRunner(executor, 12, mockDelay()).setIncreaseLimitAfter(3).runAsync(limit -> {
            limits.add(limit);
            if (limits.size() % 5 == 4) {
                return exceptionStyle.hasMore(cause);
            } else {
                return limits.size() < 40 ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
            }
        }, List.of());
        assertEquals(12, limits.get(0));
        for (int i = 1; i < limits.size(); i++) {
            String message = buildPointerMessage(limits, i);
            if (i % 5 == 4) {
                assertThat(message, limits.get(i), Matchers.lessThan(limits.get(i - 1)));
            } else if (i % 5 == 2 && i > 5) {
                assertThat(message, limits.get(i), Matchers.greaterThan(limits.get(i - 1)));
            } else {
                assertThat(message, limits.get(i), Matchers.equalTo(limits.get(i - 1)));
            }
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void increaseAfterABunch(ExceptionStyle exceptionStyle) {
        final RuntimeException cause = exceptionStyle.wrap(lessenWorkException());
        List<Integer> limits = new ArrayList<>();
        AtomicBoolean increasing = new AtomicBoolean(false);
        final int maxLimit = 93;
        final int minLimit = 1;
        new LimitedRunner(executor, maxLimit, mockDelay()).setIncreaseLimitAfter(5).runAsync(limit -> {
            limits.add(limit);
            if (limit == maxLimit) {
                increasing.set(false);
            }
            if (limit == minLimit) {
                increasing.set(true);
            }
            if (increasing.get()) {
                return limits.size() < 100 ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
            } else {
                return exceptionStyle.hasMore(cause);
            }
        }, List.of());
        increasing.set(false);
        assertEquals(93, limits.get(0));
        int changedDirection = 0;
        for (int i = 1; i < limits.size(); i++) {
            String message = buildPointerMessage(limits, i);
            if (increasing.get()) {
                // we want a bunch of successes before increasing
                assertThat(message, limits.get(i), Matchers.greaterThanOrEqualTo(limits.get(i - 1)));
                if (limits.get(i) > 2) {
                    // we don't want to increase too much
                    assertThat(message, limits.get(i), Matchers.lessThan(limits.get(i - 1) * 2));
                }
            } else {
                assertThat(message, limits.get(i), Matchers.lessThan(limits.get(i - 1)));
                if (limits.get(i) > 2) {
                    // we don't want to decrease too much
                    assertThat(message, limits.get(i), Matchers.greaterThan(limits.get(i - 1) / 2));
                }
            }
            if (limits.get(i) == maxLimit) {
                if (increasing.get()) {
                    changedDirection++;
                }
                increasing.set(false);
            }
            if (limits.get(i) == minLimit) {
                if (!increasing.get()) {
                    changedDirection++;
                }
                increasing.set(true);
            }
        }
        // make sure that the constants result in the limiter going all the way down, and back up a couple times
        assertThat(changedDirection, Matchers.greaterThan(4));
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void increaseAfterWithRetriableNonLessenWorkException(ExceptionStyle exceptionStyle) {
        // If we're failing intermittently with a retriable exception that doesn't lessen the work,
        // we shouldn't increase the limit.
        final RuntimeException cause = exceptionStyle.wrap(retriableNonLessenWorkException());
        final RuntimeException lessenCause = exceptionStyle.wrap(lessenWorkException());
        List<Integer> limits = new ArrayList<>();
        new LimitedRunner(executor, 12, mockDelay()).setIncreaseLimitAfter(3).runAsync(limit -> {
            limits.add(limit);
            if (limits.size() < 3) {
                // Cause the limit to go down, so that it could go back up, if it were reliably successful
                return exceptionStyle.hasMore(lessenCause);
            } else if (limits.size() % 2 == 0) {
                // Fail every other attempt
                return exceptionStyle.hasMore(cause);
            } else {
                return limits.size() < 40 ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
            }
        }, List.of());
        assertThat(buildPointerMessage(limits, 3), limits.get(3), Matchers.lessThan(12));
        for (int i = 3; i < limits.size(); i++) {
            assertEquals(limits.get(3), limits.get(i), buildPointerMessage(limits, i));
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @EnumSource(ExceptionStyle.class)
    void retryAtMinLimit(ExceptionStyle exceptionStyle) {
        final RuntimeException cause = exceptionStyle.wrap(lessenWorkException());
        List<Integer> limits = new ArrayList<>();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay()).runAsync(limit -> {
                    limits.add(limit);
                    return exceptionStyle.hasMore(cause);
                }, List.of()).join());
        assertEquals(cause, completionException.getCause());
        assertEquals(10, limits.get(0));
        assertEquals(1, limits.get(limits.size() - 1));
        assertThat(limits, Matchers.hasSize(Matchers.greaterThan(10)));
        for (int i = 1; i < limits.size(); i++) {
            assertThat(limits.get(i), Matchers.lessThanOrEqualTo(limits.get(i - 1)));
        }
        for (int i = limits.size() - 10; i < limits.size(); i++) {
            assertEquals(1, limits.get(i));
        }
    }

    @ParameterizedTest(name = "{displayName} ({argumentsWithNames})")
    @MethodSource("allCauseTypes")
    void delaysWhenRetrying(FDBException cause) {
        final ExceptionStyle exceptionStyle = ExceptionStyle.WrappedAsFuture;
        final RuntimeException wrappedCause = exceptionStyle.wrap(cause);
        List<Integer> limits = new ArrayList<>();
        final MockDelay mockDelay = mockDelay();
        final CompletionException completionException = assertThrows(CompletionException.class,
                () -> new LimitedRunner(executor, 10, mockDelay)
                        .runAsync(limit -> {
                            limits.add(limit);
                            System.out.println(mockDelay.getNextDelayMillis());
                            return exceptionStyle.hasMore(wrappedCause);
                        }, List.of()).join());
        assertEquals(wrappedCause, completionException.getCause());
        assertEquals(limits.size() - 1, mockDelay.delayCount);
    }

    @Test
    void closesFuture() {
        final CompletableFuture<Void> future;
        try (LimitedRunner limitedRunner = new LimitedRunner(executor, 10, mockDelay())) {
            future = limitedRunner.runAsync(limit -> new CompletableFuture<>(), List.of());
        }
        CompletionException completionException = assertThrows(CompletionException.class, future::join);
        assertThat(completionException.getCause(), Matchers.instanceOf(FDBDatabaseRunner.RunnerClosed.class));
    }

    @Test
    void closesDelay() {
        final ExceptionStyle exceptionStyle = ExceptionStyle.WrappedAsFuture;
        final RuntimeException wrappedCause = exceptionStyle.wrap(retryAndLessenWorkException());
        final CompletableFuture<Void> future;
        final InfiniteDelay infiniteDelay = new InfiniteDelay();
        LimitedRunner limitedRunner = new LimitedRunner(executor, 10, infiniteDelay);
        try {
            future = limitedRunner.runAsync(limit -> exceptionStyle.hasMore(wrappedCause)
                    .whenComplete((ignoredResult, ignoredError) -> {
                        limitedRunner.close();
                    }), List.of());
        } finally {
            limitedRunner.close();
        }
        assertTrue(infiniteDelay.future.isCompletedExceptionally());
        CompletionException completionException = assertThrows(CompletionException.class,
                () -> infiniteDelay.future.join());
        assertThat(completionException.getCause(), Matchers.instanceOf(FDBDatabaseRunner.RunnerClosed.class));

        completionException = assertThrows(CompletionException.class, future::join);
        assertThat(completionException.getCause(), Matchers.instanceOf(FDBDatabaseRunner.RunnerClosed.class));
    }

    @Nonnull
    private String buildPointerMessage(final List<Integer> limits, final int i) {
        return limits.stream()
                       .map(limit -> String.format("%02d", limit))
                       .collect(Collectors.joining(", ", "[", "]\n"))
               + limits.stream().limit(i).map(limit -> "  ")
                       .collect(Collectors.joining("  ", " ", "  ^^"));
    }

    @Nonnull
    private static FDBException retriableNonLessenWorkException() {
        return new FDBException("A retriable", FDBError.FUTURE_VERSION.code());
    }

    @Nonnull
    private static FDBException retryAndLessenWorkException() {
        return new FDBException("A retriable that could indicate the transaction is too large",
                FDBError.TRANSACTION_TOO_OLD.code());
    }

    @Nonnull
    private static FDBException lessenWorkException() {
        return new FDBException("Transaction too large", FDBError.TRANSACTION_TOO_LARGE.code());
    }

    @Nonnull
    private static FDBException nonRetriableException() {
        return new FDBException("Non Retriable", FDBError.INTERNAL_ERROR.code());
    }

    @Nonnull
    private MockDelay mockDelay() {
        return new MockDelay();
    }

    enum ExceptionStyle {
        Wrapped(true, false),
        Raw(false, false),
        WrappedAsFuture(true, true),
        RawAsFuture(false, true);

        private final boolean isWrapped;
        private final boolean isFuture;

        ExceptionStyle(final boolean isWrapped, final boolean isFuture) {

            this.isWrapped = isWrapped;
            this.isFuture = isFuture;
        }

        RuntimeException wrap(FDBException rawCause) {
            return isWrapped ? new FDBExceptions.FDBStoreRetriableException(rawCause) : rawCause;
        }

        RuntimeException wrap(RuntimeException rawCause) {
            return isWrapped ? new RuntimeException(rawCause) : rawCause;
        }

        CompletableFuture<Boolean> hasMore(RuntimeException cause) {
            if (isFuture) {
                final CompletableFuture<Boolean> future = new CompletableFuture<>();
                future.completeExceptionally(cause);
                return future;
            } else {
                throw cause;
            }
        }
    }

    private static class InfiniteDelay extends ExponentialDelay {
        public CompletableFuture<Void> future = new CompletableFuture<>();

        public InfiniteDelay() {
            super(Long.MAX_VALUE, Long.MAX_VALUE);
        }

        @Override
        public CompletableFuture<Void> delay() {
            return future;
        }
    }

    private static class MockDelay extends ExponentialDelay {

        int delayCount = 0;

        public MockDelay() {
            super(3000, 10000);
        }

        @Override
        public CompletableFuture<Void> delay() {
            delayCount++;
            return CompletableFuture.runAsync(this::calculateNextDelay);
        }
    }

}
