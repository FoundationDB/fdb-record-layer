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

import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.util.LoggableException;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.function.Executable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Helper methods for testing.
 */
public class TestHelpers {

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

    public static List<String> assertLogs(Class<?> loggingClass, Pattern pattern, Callable<?> callable) {
        return assertLogs(loggingClass.getName(), pattern, callable);
    }

    public static List<String> assertLogs(String loggerName, Pattern pattern, Callable<?> callable) {
        MatchingAppender appender = new MatchingAppender(UUID.randomUUID().toString(), pattern);
        return assertLogs(loggerName, appender, callable);
    }

    public static List<String> assertLogs(Class<?> loggingClass, String messagePrefix, Callable<?> callable) {
        return assertLogs(loggingClass.getName(), messagePrefix, callable);
    }

    public static List<String> assertLogs(String loggerName, String messagePrefix, Callable<?> callable) {
        MatchingAppender appender = new MatchingAppender(UUID.randomUUID().toString(), messagePrefix);
        return assertLogs(loggerName, appender, callable);
    }

    private static List<String> assertLogs(String loggerName, MatchingAppender appender, Callable<?> callable) {
        callAndMonitorLogging(loggerName, appender, callable);
        assertTrue(appender.matched(), () -> "No messages were logged matching [" + appender + "]");
        return appender.getMatchedEvents();
    }

    public static void assertDidNotLog(Class<?> loggingClass, Pattern pattern, Callable<?> callable) {
        assertDidNotLog(loggingClass.getName(), pattern, callable);
    }

    public static void assertDidNotLog(String loggerName, Pattern pattern, Callable<?> callable) {
        MatchingAppender appender = new MatchingAppender(UUID.randomUUID().toString(), pattern);
        assertDidNotLog(loggerName, appender, callable);
    }

    public static void assertDidNotLog(Class<?> loggingClass, String messagePrefix, Callable<?> callable) {
        assertDidNotLog(loggingClass.getName(), messagePrefix, callable);
    }

    public static void assertDidNotLog(String loggerName, String messagePrefix, Callable<?> callable) {
        MatchingAppender appender = new MatchingAppender(UUID.randomUUID().toString(), messagePrefix);
        assertDidNotLog(loggerName, appender, callable);
    }

    private static void assertDidNotLog(String loggerName, MatchingAppender appender, Callable<?> callable) {
        callAndMonitorLogging(loggerName, appender, callable);
        assertFalse(appender.matched(), () -> "Test should not have produced log message matching [" + appender + "]");
    }

    /**
     * Returns a pattern that matches the count of an event in a log message. The pattern contains
     * one group, which matches the count of the event in a log message.
     *
     * @param event store timer event to search for
     * @return a {@link Pattern} that can be used to find event counts
     */
    public static Pattern eventCountPattern(StoreTimer.Event event) {
        return Pattern.compile(".*" + event.logKeyWithSuffix("_count") + "=\"(\\d+)\".*");
    }

    /**
     * Extract the count of an event from a log event. The supplied {@link Pattern} should come from
     * {@link #eventCountPattern(StoreTimer.Event)} or should be equivalent.
     *
     * @param pattern pattern to use to extract a log event count
     * @param logEvent a log formatted message string
     * @return the count parsed from the log message
     */
    public static int extractCount(Pattern pattern, String logEvent) {
        java.util.regex.Matcher matcher = pattern.matcher(logEvent);
        assertTrue(matcher.matches(), () -> "expected \"" + logEvent + "\" to match pattern \"" + pattern + "\"");
        return Integer.parseInt(matcher.group(1));
    }

    /**
     * Assert that a string does not match a pattern.
     *
     * @param pattern pattern to search for
     * @param s string to search through
     */
    public static void assertDoesNotMatch(Pattern pattern, String s) {
        java.util.regex.Matcher matcher = pattern.matcher(s);
        assertFalse(matcher.matches(), () -> "did not expect \"" + s + "\" to have pattern \"" + pattern + "\"");
    }

    private static void callAndMonitorLogging(String loggerName, MatchingAppender appender, Callable<?> callable) {
        LoggerContext context = LoggerContext.getContext(false);
        Logger logger = context.getLogger(loggerName);
        logger.addAppender(appender);
        try {
            callable.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception", e);
        } finally {
            logger.removeAppender(appender);
        }

    }

    public static void assertThrows(Class<? extends Exception> expectedType, Callable<?> callable, Object ... keyValues) throws Exception {
        assertThrows("", expectedType, callable, keyValues);
    }

    public static void assertThrows(String message, Class<? extends Exception> expectedType, Callable<?> callable,
                                    Object ... keyValues) throws Exception {
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
            if (e instanceof ExecutionException && e.getCause() instanceof Exception) {
                e = (Exception) e.getCause();
            }
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

    public static void assertLoadRecord(int expected, @Nonnull FDBRecordContext context) {
        assertNotNull(context.getTimer());
        int loads = context.getTimer().getCount(FDBStoreTimer.Events.LOAD_RECORD);
        assertTrue(loads <= expected, "loaded too many records\nExpected maximum: " + expected + "\nActual loaded: " + loads);
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

    @SuppressWarnings("serial")
    private static class MatchingAppender extends AbstractAppender {
        @Nullable
        private final Pattern pattern;
        @Nullable
        private final String messagePrefix;

        private final List<String> matchedEvents = new ArrayList<>();

        protected MatchingAppender(@Nonnull String name, @Nonnull Pattern pattern) {
            super(name, null, null, true, null);
            this.pattern = pattern;
            this.messagePrefix = null;
        }

        protected MatchingAppender(@Nonnull String name, @Nonnull String messagePrefix) {
            super(name, null, null, true, null);
            this.pattern = null;
            this.messagePrefix = messagePrefix;
        }

        public boolean matched() {
            return !matchedEvents.isEmpty();
        }

        @Nonnull
        public List<String> getMatchedEvents() {
            return matchedEvents;
        }

        @Override
        public synchronized void append(@Nonnull LogEvent event) {
            if ((pattern != null && pattern.matcher(event.getMessage().getFormattedMessage()).matches())
                    || (messagePrefix != null && event.getMessage().getFormattedMessage().startsWith(messagePrefix)))  {
                matchedEvents.add(event.getMessage().getFormattedMessage());
            }
        }

        @Override
        public String toString() {
            if (pattern != null) {
                return pattern.toString();
            }
            return messagePrefix;
        }
    }
}
