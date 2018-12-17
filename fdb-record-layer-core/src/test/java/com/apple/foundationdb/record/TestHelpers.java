/*
 * TestHelpers.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.util.LoggableException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.function.Executable;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Helper methods for testing.
 */
public class TestHelpers {

    /**
     * This enum exists so that tests can use them as an enumeration source in
     * parameterized tests. JUnit5, for whatever reason, doesn't allow booleans
     * as values for parameters--only strings, ints, longs, and doubles.
     */
    public enum BooleanEnum {
        FALSE(false),
        TRUE(true);

        private final boolean value;

        BooleanEnum(boolean value) {
            this.value = value;
        }

        public boolean toBoolean() {
            return value;
        }
    }

    public static Executable toCallable(DangerousRunnable danger) {
        return () -> {
            try {
                danger.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new AssertionError("Callable threw non-RuntimeException", e);
            }
        };
    }

    public static void assertThrows(Class<? extends Exception> expectedType, Callable<?> callable, Object ...keyValues) throws Exception {
        assertThrows("", expectedType, callable, keyValues);
    }

    public static void assertThrows(String message, Class<? extends Exception> expectedType, Callable<?> callable,
                                    Object ...keyValues) throws Exception {
        if (keyValues.length > 0) {
            if (!LoggableException.class.isAssignableFrom(expectedType)) {
                fail("Log value checks can only be provided with a LoggableException");
            }
            if ((keyValues.length % 2) != 0) {
                fail("Invalid key/value pairs for log property check");
            }
        }

        try {
            callable.call();
            message = message.isEmpty() ? "" : (message + ": ");
            fail(message + "Expected Exception of type " + expectedType + " but none was thrown");
        } catch (Exception e) {
            if (!e.getClass().equals(expectedType)) {
                throw e;
            }
            if (keyValues.length > 0) {
                Map<String, Object> loggedKeys = ((LoggableException) e).getLogInfo();
                for (int i = 0; i < keyValues.length; i += 2) {
                    final String name = (String) keyValues[i];
                    final Object value = keyValues[i + 1];
                    if (!loggedKeys.containsKey(name)) {
                        fail("Expected additional logging info name '" + name + "', but it is not present");
                    }
                    assertEquals(value, loggedKeys.get(name), "Wrong value for logging property '" + name + "'");
                }
            }
        }
    }

    public static <T> void eventually(String description, Supplier<T> underTest, Matcher<T> matchCondition,
                                      int timeoutMillis, int pollIntervalMillis) {
        long start = System.currentTimeMillis();
        long current = start;
        while (current - start < timeoutMillis) {
            if (matchCondition.matches(underTest.get())) {
                // success
                return;
            }
            try {
                Thread.sleep(pollIntervalMillis);
            } catch (InterruptedException e) {
                fail("interrupted", e);
            }
            current = System.currentTimeMillis();
        }
        fail(description + ", timed out waiting to match " + matchCondition);
    }

    public static <T> void consistently(String description, Supplier<T> underTest, Matcher<T> matchCondition,
                                      int totalTimeMillis, int pollIntervalMillis) {
        long start = System.currentTimeMillis();
        long current = start;
        while (current - start < totalTimeMillis) {
            T value = underTest.get();
            if (!matchCondition.matches(value)) {
                fail("expected consistently " + description + " but value " + value + " does not satisfy " + matchCondition);
            }
            try {
                Thread.sleep(pollIntervalMillis);
            } catch (InterruptedException e) {
                fail("interrupted", e);
            }
            current = System.currentTimeMillis();
        }
    }

    /**
     * A matcher that an exception's message contains a given string.
     */
    public static class ExceptionMessageMatcher extends TypeSafeMatcher<Throwable> {
        private Matcher<String> messageMatcher;

        public ExceptionMessageMatcher(String message) {
            this.messageMatcher = containsString(message);
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            return messageMatcher.matches(item.getMessage());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("an exception with message that satisfies: ");
            messageMatcher.describeTo(description);
        }

        public static ExceptionMessageMatcher hasMessageContaining(String message) {
            return new ExceptionMessageMatcher(message);
        }
    }

    /**
     * A consumer that can throw exceptions.
     * @param <T> type of the consumer argument
     */
    @FunctionalInterface
    public interface DangerousConsumer<T> {
        void accept(T t) throws Exception;
    }

    /**
     * A runnable that can throw exceptions.
     */
    @FunctionalInterface
    public interface DangerousRunnable {
        void run() throws Exception;
    }

    /**
     * The default Hamcrest anything matcher returns a <code>Matcher&lt;Object&gt;</code>, which leads to either type errors
     * or terrible casting. This is a type-safe anything matcher.
     * @param <T> the required type
     */
    public static class RealAnythingMatcher<T> extends BaseMatcher<T> {
        @Override
        public boolean matches(Object o) {
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("anything()");
        }

        @Nonnull
        public static <T> Matcher<T> anything() {
            return new RealAnythingMatcher<>();
        }
    }

    public static void assertDiscardedAtMost(int expected, @Nonnull FDBRecordContext context) {
        assertNotNull(context.getTimer());
        int discarded = context.getTimer().getCount(FDBStoreTimer.Counts.QUERY_DISCARDED);
        assertTrue(discarded <= expected, "discarded too many records\nExpected maximum: " + expected + "\nActual discarded: " + discarded);
    }

    public static void assertDiscardedAtLeast(int expected, @Nonnull FDBRecordContext context) {
        assertNotNull(context.getTimer());
        int discarded = context.getTimer().getCount(FDBStoreTimer.Counts.QUERY_DISCARDED);
        assertTrue(discarded >= expected, "discarded too few records\nExpected minimum: " + expected + "\nActual discarded: " + discarded);
    }

    public static void assertDiscardedExactly(int expected, @Nonnull FDBRecordContext context) {
        assertNotNull(context.getTimer());
        int discarded = context.getTimer().getCount(FDBStoreTimer.Counts.QUERY_DISCARDED);
        assertTrue(discarded == expected, "discarded wrong number of records\nExpected: " + expected + "\nActual: " + discarded);
    }

    public static void assertDiscardedNone(@Nonnull FDBRecordContext context) {
        assertNotNull(context.getTimer());
        int discarded = context.getTimer().getCount(FDBStoreTimer.Counts.QUERY_DISCARDED);
        assertTrue(discarded == 0, "discarded records unnecessarily\nExpected: 0\nActual: " + discarded);
    }
}
