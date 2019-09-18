/*
 * StoreTimer.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A context-wide accumulator of timing information.
 * <p>
 * A store timer is a thread-safe record of call counts and nanosecond call times for various database operations,
 * classified by an {@link Event}.
 * If a context has a store timer, various operations will record timing information in it. It is up to the caller to
 * provide the necessary integration between this information and any system monitoring tools.
 */
@API(API.Status.MAINTAINED)
public class StoreTimer {
    @Nonnull
    private static final Counter EMPTY_COUNTER = new Counter();

    @Nonnull
    protected final Map<Event, Counter> counters;
    @Nonnull
    protected final Map<Event, Counter> timeoutCounters;
    protected long lastReset;
    @Nonnull
    protected final UUID uuid;

    /**
     * Confirm that there is no naming conflict among the event names that will be used.
     *
     * @param events a stream of events to check for duplicates
     */
    public static void checkEventNameUniqueness(@Nonnull Stream<Event> events) {
        final Set<String> seen = new HashSet<>();
        final Set<String> duplicates = events.map(Event::name).filter(n -> !seen.add(n)).collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new RecordCoreException("Duplicate event names: " + duplicates);
        }
    }

    /**
     * Subtracts counts and times recorded by a snapshot of a timer returning a new timer representing the difference.
     * <p>
     * Subtracting a snapshot of a timer from the original timer after subsequent operations have
     * been performed and timed can provide useful metrics as to the cost of those subsequent operations.
     * <p>
     * The timer must have been previously derived from the snapshot. Moreover,
     * the timer should not have been reset after the snapshot was taken.
     *
     * @param timer timer to subtract the snapshot from
     * @param timerSnapshot snapshot of the provided timer
     *
     * @return a snapshot of the provided timer
     */
    @Nonnull
    public static StoreTimer getDifference(@Nonnull StoreTimer timer, @Nonnull StoreTimerSnapshot timerSnapshot) {

        if (!timerSnapshot.derivedFrom(timer)) {
            throw new RecordCoreArgumentException("Invalid to subtract a snapshot timer from a timer it was not derived from.");
        }

        if (!timerSnapshot.takenAfterReset(timer)) {
            throw new RecordCoreArgumentException("Invalid to substract a snapshot timer from a timer that has been reset after the snapshot was taken");
        }

        StoreTimer resultTimer = new StoreTimer();
        timer.counters.entrySet()
                .stream()
                .forEach(entry -> {
                    int count = 0;
                    long time = 0L;
                    if (timerSnapshot.containsCounter(entry.getKey())) {
                        count = timerSnapshot.getCounterSnapshot(entry.getKey()).getCount();
                        time = timerSnapshot.getCounterSnapshot(entry.getKey()).getTimeNanos();
                    }
                    Counter diffCounter = new Counter();
                    diffCounter.count.set(entry.getValue().count.get() - count);
                    diffCounter.timeNanos.set(entry.getValue().timeNanos.get() - time);
                    resultTimer.counters.put(entry.getKey(), diffCounter);
                });
        timer.timeoutCounters.entrySet()
                .stream()
                .forEach(entry -> {
                    int count = 0;
                    long time = 0L;
                    if (timerSnapshot.containsTimeoutCounter(entry.getKey())) {
                        count = timerSnapshot.getTimeoutCounterSnapshot(entry.getKey()).getCount();
                        time = timerSnapshot.getTimeoutCounterSnapshot(entry.getKey()).getTimeNanos();
                    }
                    Counter diffCounter = new Counter();
                    diffCounter.count.set(entry.getValue().count.get() - count);
                    diffCounter.timeNanos.set(entry.getValue().timeNanos.get() - time);
                    resultTimer.timeoutCounters.put(entry.getKey(), diffCounter);
                });

        //subtracting out the snapshot has effectively made the snapshot time the last reset time
        timerSnapshot.setResetTime(resultTimer);
        return resultTimer;
    }

    @Nonnull
    protected static Counter getCounter(@Nonnull Map<Event, Counter> counters, @Nonnull Event event, boolean create) {
        if (create) {
            return counters.computeIfAbsent(event, evignore -> new Counter());
        } else {
            return counters.getOrDefault(event, EMPTY_COUNTER);
        }
    }

    /**
     * An identifier for occurrences that need to be timed.
     */
    public interface Event {
        /**
         * Get the name of this event for machine processing.
         *
         * @return the name
         */
        String name();

        /**
         * Get the title of this event for user displays.
         *
         * @return the user-visible title
         */
        String title();

        /**
         * Get the logKey of this event for logging purposes.
         *
         * @return the key to use for logging
         */
        String logKey();
    }

    /**
     * {@link Event}s that are a significant part of a larger process.
     * The time in these events should be accounted for within another event and so should not be added to totals.
     */
    public interface DetailEvent extends Event {
    }

    /**
     * {@link Event}s that can be waited on.
     * The time for a {@code Wait} is the time actually waiting, which may be shorter than the time for the asynchronous
     * operation itself.
     */
    public interface Wait extends Event {
    }

    /**
     * {@link Event}s that only count occurrences or total size.
     * There is no meaningful time duration associated with these events.
     */
    public interface Count extends Event {
        /**
         * Get whether the count value is actually a size in bytes.
         *
         * @return {@code true} if the count value is actually a size in bytes
         */
        boolean isSize();
    }

    /**
     * Contains the number of occurrences and cummulative time spent on an associated {@link StoreTimer.Event}.
     */
    public static class Counter {
        private final AtomicLong timeNanos = new AtomicLong();
        private final AtomicInteger count = new AtomicInteger();

        /**
         * Get the number of occurrences of the associated event.
         *
         * @return the number of occurrences of the associated event
         */
        @Nonnull
        public int getCount() {
            return count.get();
        }

        /**
         * Get the cumulative time spent on the associated event.
         *
         * @return the cumulative time spent on the associated event
         */
        public long getTimeNanos() {
            return timeNanos.get();
        }

        /**
         * Add additional time spent performing the associated event.
         *
         * @param timeDifference additional time spent performing the associated event
         */
        public void record(long timeDifference) {
            timeNanos.addAndGet(timeDifference);
            count.incrementAndGet();
        }

        /**
         * Add an additional number of occurrences spent performing the associated event.
         *
         * @param amount additional number of times spent performing the associated event
         */
        public void increment(int amount) {
            count.addAndGet(amount);
        }
    }

    public StoreTimer() {
        counters = new ConcurrentHashMap<>();
        timeoutCounters = new ConcurrentHashMap<>();
        lastReset = System.nanoTime();
        uuid = UUID.randomUUID();
    }

    /**
     * Get the UUID of this timer.
     *
     * @return the UUID of this timer
     */
    @Nonnull
    public UUID geUUID() {
        return uuid;
    }

    /**
     * Record the amount of time each element in a set of events took to run.
     * This applies the same time difference to each event in the set.
     *
     * @param events the set of events being recorded
     * @param timeDifferenceNanos the time that the instrumented events took to run
     */
    public void record(Set<Event> events, long timeDifferenceNanos) {
        for (Event event : events) {
            record(event, timeDifferenceNanos);
        }
    }

    /**
     * Record the amount of time an event took to run.
     * Subclasses can extend this to also update metrics aggregation or
     * monitoring services.
     *
     * @param event the event being recorded
     * @param timeDifferenceNanos the time that instrumented event took to run
     */
    public void record(Event event, long timeDifferenceNanos) {
        getCounter(counters, event, true).record(timeDifferenceNanos);
    }

    /**
     * Deprecated. Record that an event occurred once. Users should use
     * {@link #increment(Count)} instead.
     *
     * @param event the event being recorded
     *
     * @deprecated replaced with {@link #increment(Count)}
     */
    @Deprecated
    public void record(@Nonnull Count event) {
        increment(event);
    }

    /**
     * Record time since given time.
     *
     * @param event the event being recorded
     * @param startTime the {@code System.nanoTime()} when the event started
     */
    public void recordSinceNanoTime(@Nonnull Event event, long startTime) {
        record(event, System.nanoTime() - startTime);
    }

    /**
     * Record that some operation timed out.
     *
     * @param event the event that was waited for
     * @param startTime the {@code System.nanoTime()} when the event started
     */
    public void recordTimeout(Wait event, long startTime) {
        getCounter(timeoutCounters, event, true).record(System.nanoTime() - startTime);
    }

    /**
     * Record that each event in a set occurred once. This increments
     * the counters associated with each event.
     *
     * @param events the set of events being recorded
     */
    public void increment(@Nonnull Set<Count> events) {
        for (Count event : events) {
            increment(event);
        }
    }

    /**
     * Record that an event occurred once. This increments the counter associated
     * with the given event.
     *
     * @param event the event being recorded
     */
    public void increment(@Nonnull Count event) {
        increment(event, 1);
    }

    /**
     * Record that each event occurred one or more times. This increments
     * the counters associated with each event by <code>amount</code>.
     *
     * @param events the set of events being recorded
     * @param amount the number of times each event occurred
     */
    public void increment(@Nonnull Set<Count> events, int amount) {
        for (Count event : events) {
            increment(event, amount);
        }
    }

    /**
     * Record that an event occurred one or more times. This increments the
     * counter associated with the given event by <code>amount</code>.
     *
     * @param event the event being recorded
     * @param amount the number of times the event occurred
     */
    public void increment(Count event, int amount) {
        getCounter(counters, event, true).increment(amount);
    }

    /**
     * Get the total time spent for a given event.
     *
     * @param event the event to get time information for
     *
     * @return the total number of nanoseconds recorded for the event
     */
    public long getTimeNanos(Event event) {
        return getCounter(counters, event, false).timeNanos.get();
    }

    /**
     * Get the total count for a given event.
     *
     * @param event the event to get count information for
     *
     * @return the total number times that event was recorded
     */
    public int getCount(Event event) {
        return getCounter(counters, event, false).count.get();
    }

    /**
     * Get the total time spent for a given event that timed out.
     *
     * @param event the event to get time information for
     *
     * @return the total number of nanoseconds recorded for when the event timed out
     */
    public long getTimeoutTimeNanos(Event event) {
        return getCounter(timeoutCounters, event, false).timeNanos.get();
    }

    /**
     * Get the total count of timeouts for a given event.
     *
     * @param event the event to get timeout information for
     *
     * @return the total number times that event was recorded as timed out
     */
    public int getTimeoutCount(Event event) {
        return getCounter(timeoutCounters, event, false).count.get();
    }

    /**
     * Get all events known to this timer.
     *
     * @return a collection of events for which timing information was recorded
     */
    public Collection<Event> getEvents() {
        return counters.keySet();
    }

    /**
     * Get all events that have timed out.
     *
     * @return a collection of events for which timeout information was recorded
     */
    public Collection<Event> getTimeoutEvents() {
        return timeoutCounters.keySet();
    }

    /**
     * Suitable for {@link KeyValueLogMessage}.
     *
     * @return a map of recorded times and counts for logging
     */
    public Map<String, Number> getKeysAndValues() {
        Collection<Event> timeoutEvents = getTimeoutEvents();
        Map<String, Number> result = new HashMap<>((counters.size() + timeoutEvents.size()) * 2);
        //add counter events to result map
        for (Map.Entry<Event, Counter> entry : counters.entrySet()) {
            Event event = entry.getKey();
            Counter counter = entry.getValue();
            String prefix = event.name().toLowerCase(Locale.ROOT);
            result.put(prefix + "_count", counter.count.get());
            if (!(event instanceof Count)) {
                result.put(prefix + "_micros", counter.timeNanos.get() / 1000);
            }
        }
        // now add recorded timeout events to map
        for (Event timeoutEvent : timeoutEvents) {
            String timeoutPrefix = timeoutEvent.name().toLowerCase(Locale.ROOT);
            result.put(timeoutPrefix + "_timeout_micros", getTimeoutTimeNanos(timeoutEvent) / 1000);
            result.put(timeoutPrefix + "_timeout_count", getTimeoutCount(timeoutEvent));
        }
        return result;
    }

    /**
     * Clear all recorded timing information.
     */
    public void reset() {
        counters.clear();
        timeoutCounters.clear();
        lastReset = System.nanoTime();
    }

    /**
     * Add timing instrumentation to an asynchronous operation.
     *
     * @param event the event type to use to record timing
     * @param future a future that will complete when the operation is finished
     * @param executor an asynchronous executor to use to run the recording
     * @param <T> the type of the future
     *
     * @return a new future that will be complete after also recording timing information
     */
    public <T> CompletableFuture<T> instrument(Event event, CompletableFuture<T> future, Executor executor) {
        if (future.isDone()) {
            record(event, 0);
            return future;
        }
        return instrumentAsync(Collections.singleton(event), future, executor, System.nanoTime());
    }

    /**
     * Add timing instrumentation to an asynchronous operation.
     *
     * @param events the event types to use to record timing
     * @param future a future that will complete when the operation is finished
     * @param executor an asynchronous executor to use to run the recording
     * @param <T> the type of the future
     *
     * @return a new future that will be complete after also recording timing information
     */
    public <T> CompletableFuture<T> instrument(Set<Event> events, CompletableFuture<T> future, Executor executor) {
        if (future.isDone()) {
            for (Event event : events) {
                record(event, 0);
            }
            return future;
        }
        return instrumentAsync(events, future, executor, System.nanoTime());
    }

    /**
     * Add timing instrumentation to an asynchronous operation.
     *
     * @param event the event type to use to record timing
     * @param future a future that will complete when the operation is finished
     * @param executor an asynchronous executor to use to run the recording
     * @param startTime the nanosecond time at which the operation started
     * @param <T> the type of the future
     *
     * @return a new future that will be complete after also recording timing information
     */
    public <T> CompletableFuture<T> instrument(Event event, CompletableFuture<T> future, Executor executor, long startTime) {
        if (future.isDone()) {
            record(event, System.nanoTime() - startTime);
            return future;
        }
        return instrumentAsync(Collections.singleton(event), future, executor, startTime);
    }

    /**
     * Add timing instrumentation to an asynchronous operation.
     *
     * @param events the event types to use to record timing
     * @param future a future that will complete when the operation is finished
     * @param executor an asynchronous executor to use to run the recording
     * @param startTime the nanosecond time at which the operation started
     * @param <T> the type of the future
     *
     * @return a new future that will be complete after also recording timing information
     */
    public <T> CompletableFuture<T> instrument(Set<Event> events, CompletableFuture<T> future, Executor executor, long startTime) {
        if (future.isDone()) {
            long timeDifference = System.nanoTime() - startTime;
            for (Event event : events) {
                record(event, timeDifference);
            }
            return future;
        }
        return instrumentAsync(events, future, executor, startTime);
    }

    /**
     * Instrument an asynchronous cursor.
     * Timing information is recorded for each invocation of the {@link RecordCursor#onHasNext()} asynchronous method.
     *
     * @param event the event type to use to record timing
     * @param inner the cursor to record timing information for
     * @param <T> the type of the cursor elements
     *
     * @return a new cursor that returns the same elements and also records timing information
     */
    public <T> RecordCursor<T> instrument(Event event, RecordCursor<T> inner) {
        return new RecordCursor<T>() {
            CompletableFuture<Boolean> nextFuture = null;
            RecordCursorResult<T> nextResult;

            @Nonnull
            @Override
            public CompletableFuture<RecordCursorResult<T>> onNext() {
                return instrument(event, inner.onNext(), inner.getExecutor()).thenApply(result -> {
                    nextResult = result;
                    return nextResult;
                });
            }

            @Override
            @Nonnull
            @Deprecated
            public CompletableFuture<Boolean> onHasNext() {
                if (nextFuture == null) {
                    nextFuture = onNext().thenApply(RecordCursorResult::hasNext);
                }
                return nextFuture;
            }

            @Override
            @Nullable
            @Deprecated
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                nextFuture = null;
                return nextResult.get();
            }

            @Override
            @Nullable
            @Deprecated
            public byte[] getContinuation() {
                return nextResult.getContinuation().toBytes();
            }

            @Nonnull
            @Override
            @Deprecated
            public NoNextReason getNoNextReason() {
                return nextResult.getNoNextReason();
            }

            @Override
            public void close() {
                if (nextFuture != null) {
                    nextFuture.cancel(false);
                    nextFuture = null;
                }
                inner.close();
            }

            @Nonnull
            @Override
            public Executor getExecutor() {
                return inner.getExecutor();
            }

            @Override
            public boolean accept(@Nonnull RecordCursorVisitor visitor) {
                if (visitor.visitEnter(this)) {
                    inner.accept(visitor);
                }
                return visitor.visitLeave(this);
            }
        };
    }

    protected <T> CompletableFuture<T> instrumentAsync(Set<Event> events, CompletableFuture<T> future, Executor executor, long startTime) {
        return future.whenComplete((result, exception) -> {
            long timeDifference = System.nanoTime() - startTime;
            for (Event event : events) {
                record(event, timeDifference);
            }
        });
    }
}
