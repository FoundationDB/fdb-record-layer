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
import com.apple.foundationdb.record.util.MapUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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
    private static final Counter ZERO_COUNTER = new Counter(true);

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
        computeDifference(timer.counters, timerSnapshot.getCounters(), resultTimer.counters);
        computeDifference(timer.timeoutCounters, timerSnapshot.getTimeoutCounters(), resultTimer.timeoutCounters);

        //subtracting out the snapshot has effectively made the snapshot time the last reset time
        timerSnapshot.setResetTime(resultTimer);
        return resultTimer;
    }

    private static void computeDifference(@Nonnull Map<Event, Counter> timerCounters,
                                          @Nonnull Map<Event, StoreTimerSnapshot.CounterSnapshot> snapShotCounters,
                                          @Nonnull Map<Event, Counter> differenceCounters) {
        for (Map.Entry<Event, Counter> entry : timerCounters.entrySet()) {
            final Event event = entry.getKey();
            final Counter counter = entry.getValue();

            @Nullable final StoreTimerSnapshot.CounterSnapshot snapShotCounter = snapShotCounters.get(event);

            // Add events that appeared in the store timer since the snapshot
            if (snapShotCounter == null) {
                differenceCounters.put(event, new Counter(counter));
            } else {
                int count = counter.getCount() - snapShotCounter.getCount();

                // Do not add events that weren't changed since the snapshot
                if (count > 0) {
                    differenceCounters.put(event, new Counter(count, counter.getCumulativeValue() - snapShotCounter.getCumulativeValue()));
                }
            }
        }
    }

    /**
     * Return the counter for a given event.
     *
     * @param event the event to have its counter retrieved
     * @return the counter for the event or {@code null} if not counter exists
     */
    @Nullable
    public Counter getCounter(@Nonnull Event event) {
        return getCounter(event, false);
    }

    /**
     * Return the counter value for a given event type. When {@code createIfNotExists} is {@code true} and a counter
     * for the {@code event} did not previously exists, a zero-value counter will be created and returned, ensuring
     * that you will never receive a {@code null} value; for {@link Aggregate} events, the zero-value counter will
     * be immutable, however for non-aggregate events the counter returned may be updated by the caller as necessary.
     *
     * @param event the event to have its counter retrieved
     * @param createIfNotExists if true the counter returned will be created if it did not already exist
     * @return the counter for the event or {@code null} if there is no value present for the event and
     *   {@code createIfNotExists} was false
     */
    @Nullable
    protected Counter getCounter(@Nonnull Event event, boolean createIfNotExists) {
        if (event instanceof Aggregate) {
            @Nullable Counter counter = ((Aggregate) event).compute(this);
            if (counter == null && createIfNotExists) {
                return ZERO_COUNTER;
            }
            return counter;
        } else {
            if (createIfNotExists) {
                return MapUtils.computeIfAbsent(counters, event, evignore -> new Counter());
            }
            return counters.get(event);
        }
    }

    /**
     * Return the timeout counter value for a given event type.
     *
     * @param event the event to have its counter retrieved
     * @return the counter for the event or {@code null} if there is no value present for the event
     */
    @Nullable
    public Counter getTimeoutCounter(@Nonnull Event event) {
        return getTimeoutCounter(event, false);
    }

    @Nullable
    protected Counter getTimeoutCounter(@Nonnull Event event, boolean createIfNotExists) {
        if (createIfNotExists) {
            return MapUtils.computeIfAbsent(timeoutCounters, event, evignore -> new Counter());
        }
        return timeoutCounters.get(event);
    }

    /**
     * An identifier for occurrences that need to be timed.
     */
    public interface Event {
        Map<String, Map<Event, String>> LOG_KEY_SUFFIX_CACHE = new ConcurrentHashMap<>();

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
         * Whether a metric should not be recorded until its associated transaction
         * has committed. This is useful for certain metrics, such as the amount of
         * data written, which should only be recorded on committed transactions.
         * If a transaction is not committed or the commit fails, then associated
         * metrics should not be recorded.
         *
         * @return whether this event should delay being published until its associated
         *     transaction commits
         */
        default boolean isDelayedUntilCommit() {
            return false;
        }

        /**
         * Get the key of this event for logging. This should be used with
         * {@link KeyValueLogMessage}s and other key-value based logging
         * systems to log the values from instrumented events. These keys are
         * not expected to change frequently. They may, however, change
         * outside of any minor version change. Their values, therefore,
         * should not be relied upon, other than for the logging purposes.
         *
         * @return the key to use for logging
         */
        default String logKey() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        /**
         * Return the log key with a specified suffix appended.
         *
         * @param suffix the suffix to append to the log key
         * @return the log key with suffix appended
         */
        default String logKeyWithSuffix(@Nonnull String suffix) {
            return MapUtils.computeIfAbsent(MapUtils.computeIfAbsent(LOG_KEY_SUFFIX_CACHE, suffix, ignoredPostfix -> new ConcurrentHashMap<>()),
                    this, event -> event.logKey() + suffix);
        }
    }

    /**
     * An aggregate event is an event whose value is computed over the value of another set of events.
     */
    public interface Aggregate extends Event {

        /**
         * Returns the set of events that make up the aggregate.  Note that theoretically an aggregate could be defined
         * in a manner such that some events contributed to the aggregate in different fashions (for example,
         * {@code eventA + eventB - eventC}), in which case this method would return the fact that the three events
         * comprise the aggregate but with no indication of in which manner they contribute to its value.
         * @return the events that comprise the aggregate
         */
        Set<? extends Event> getComponentEvents();

        /**
         * Helper for implementations to validate that all of the events in the aggregate conform to
         * some basic expectations.
         *
         * @param events the events too validate
         * @param <T> the type of event
         * @return the validated events
         * @throws RecordCoreArgumentException if the validation assumptions are violated
         */
        @SuppressWarnings("unchecked")
        default <T extends Event> T[] validate(@Nonnull T... events) {
            return validate((a, b) -> { return; }, events);
        }

        /**
         * Helper for implementations to validate that all of the events in the aggregate conform to
         * some basic expectations.
         *
         * @param extraCheck a validation that takes the first event in the set of events and another event
         *   in the set of events and ensures that they are sufficiently compatible to aggregatee
         * @param events the events too validate
         * @param <T> the type of event
         * @return the validated events
         * @throws RecordCoreArgumentException if the validation assumptions are violated
         */
        @SuppressWarnings("unchecked")
        default <T extends Event> T[] validate(@Nonnull BiConsumer<T, T> extraCheck, @Nonnull T... events) {
            if (events.length == 0) {
                throw new RecordCoreArgumentException("At least one event must be supplied to aggregate");
            }

            // Ensure that all of the events to be aggregated are compatible with each other
            if (events.length > 1) {
                final T firstEvent = events[0];
                for (int i = 1; i < events.length; i++) {
                    final T event = events[i];
                    extraCheck.accept(firstEvent, event);
                    if (!firstEvent.getClass().isInstance(event)) {
                        // Technically we could allow for this I think, but it feels dangerous to allow this
                        throw new RecordCoreArgumentException("All events must be of the same type");
                    }
                    if (event instanceof Aggregate) {
                        // We could allow for this by adding some cycle checking to ensure we cannot get into a loop.
                        throw new RecordCoreArgumentException("Aggregates may not be constructed from other aggregates");
                    }
                }
            }

            return events;
        }

        /**
         * Compute the value for this aggregate.
         *
         * @param storeTimer the time from which to draw the values that are necessary to compute this aggregate
         * @return the computed result or null if none of the value that comprise this aggregate were available
         */
        @Nullable
        Counter compute(@Nonnull StoreTimer storeTimer);

        /**
         * Compute the value for this aggregate.
         *
         * @param storeTimer the time from which to draw the values that are necessary to compute this aggregate
         * @param events the events that are to be aggregated into the resulting {@code Counteer}
         * @return the computed result or null if none of the value that comprise this aggregate were available
         */
        @Nullable
        default Counter compute(@Nonnull StoreTimer storeTimer, @Nonnull Set<? extends Event> events) {
            @Nullable StoreTimer.Counter counter = null;

            for (Event event : events) {
                @Nullable Counter value = storeTimer.counters.get(event);
                if (value != null) {
                    if (counter == null) {
                        counter = new Counter();
                    }
                    counter.add(value);
                }
            }

            // Make sure the counter returned cannot be mutated.  This is a safety net to ensure that
            // no code is accidentally modifying the returned counter thinking that they are affecting
            // an actual change in the event.
            return counter == null ? null : counter.makeImmutable();
        }
    }

    /**
     * {@link Event}s that are a significant part of a larger process.
     * The time in these events should be accounted for within another event and so should not be added to totals.
     */
    public interface DetailEvent extends StoreTimer.Event {
    }

    /**
     * {@link Event}s that can be waited on.
     * The time for a {@code Wait} is the time actually waiting, which may be shorter than the time for the asynchronous
     * operation itself.
     */
    public interface Wait extends StoreTimer.Event {
    }

    /**
     * {@link Event}s that only count occurrences or total size.
     * There is no meaningful time duration/cumulative value associated with these events.
     */
    public interface Count extends StoreTimer.Event {
        /**
         * Get whether the count value is actually a size in bytes.
         *
         * @return {@code true} if the count value is actually a size in bytes
         */
        boolean isSize();
    }

    /**
     * {@link Event}s that count the number of occurrences of an operation and also the size of the operation.
     * {@link SizeEvent} is similar in implementation to (Time){@link Event} except for the fact that the recorded value
     * is not the time. It is also different from {@link Count} since along with the size of the event in concern, it
     * also records the number of times the event has occurred.
     *
     * Since {@link SizeEvent} tracks count and size together, it is easier for the consumer than having two {@link Count}
     * events that track them separately. In essence, the consumer benefits from recording only a single event to capture
     * both pieces of information and also from the fact that the link between count and size is more obvious when
     * reading metrics.
     */
    public interface SizeEvent extends StoreTimer.Event {
    }

    /**
     * Contains the number of occurrences and cumulative time spent/cumulative value of all occurrences of the associated
     * {@link StoreTimer.Event}.
     */
    public static class Counter {
        private final AtomicLong cumulativeValue;
        private final AtomicInteger count;
        private boolean immutable;

        private Counter() {
            this(false);
        }

        private Counter(Counter counter) {
            this(counter, false);
        }

        private Counter(Counter counter, boolean immutable) {
            this(counter.getCount(), counter.getCumulativeValue(), immutable);
        }

        public Counter(boolean immutable) {
            this(0, 0L, immutable);
        }

        public Counter(int count, long timeNanos) {
            this(count, timeNanos, false);
        }

        public Counter(int count, long cumulativeValue, boolean immutable) {
            this.count = new AtomicInteger(count);
            this.cumulativeValue = new AtomicLong(cumulativeValue);
            this.immutable = immutable;
        }

        /**
         * Get the number of occurrences of the associated event.
         *
         * @return the number of occurrences of the associated event
         */
        public int getCount() {
            return count.get();
        }

        /**
         * Get the cumulative time spent on the associated event. This method is identical to {@link Counter#getCumulativeValue()}
         * in the way that both returns the underlying cumulative value. Hence, this method should strictly be used for
         * time-based events that are cumulating time over different occurrences.
         *
         * @return the cumulative time spent on the associated event
         */
        public long getTimeNanos() {
            return getCumulativeValue();
        }

        /**
         * Get the cumulative value of the associated event.
         *
         * @return the cumulative value of the associated event
         */
        public long getCumulativeValue() {
            return cumulativeValue.get();
        }

        /**
         * Add value incurred in the occurrence of the associated event.
         *
         * @param occurrenceValue additional value incurred in performing the associated event
         */
        public void record(long occurrenceValue) {
            checkImmutable();
            cumulativeValue.addAndGet(occurrenceValue);
            count.incrementAndGet();
        }

        /**
         * Add an additional number of occurrences spent performing the associated event.
         *
         * @param amount additional number of times spent performing the associated event
         */
        public void increment(int amount) {
            checkImmutable();
            count.addAndGet(amount);
        }

        /**
         * Add the value of one counter into another counter.
         *
         * @param counter the other counter to add into this counter
         */
        public void add(@Nonnull Counter counter) {
            checkImmutable();
            cumulativeValue.addAndGet(counter.getCumulativeValue());
            count.addAndGet(counter.getCount());
        }

        /**
         * For internal use during aggregate event computation; the counter can remain mutable until the
         * computation is completed, then the counter is made immutable, ensuring that other code doesn't
         * accidentally think it is updating a regular counter.
         *
         * @return this counter, now immutable
         */
        protected Counter makeImmutable() {
            this.immutable = true;
            return this;
        }

        private void checkImmutable() {
            if (immutable) {
                throw new RecordCoreException("immutable counter");
            }
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
        getCounter(event, true).record(timeDifferenceNanos);
    }

    /**
     * Record an event that has a size. For instance, an IO event having the number of bytes of data read or written.
     * Subclasses can extend this to also update metrics aggregation or
     * monitoring services.
     *
     * @param event the event being recorded
     * @param size size of IO that instrumented event performed.
     */
    public void recordSize(SizeEvent event, long size) {
        getCounter(event, true).record(size);
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
        getTimeoutCounter(event, true).record(System.nanoTime() - startTime);
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
    public void increment(@Nonnull Count event, int amount) {
        getCounter(event, true).increment(amount);
    }

    /**
     * Get the total time spent for a given event.
     *
     * @param event the event to get time information for
     *
     * @return the total number of nanoseconds recorded for the event
     */
    public long getTimeNanos(Event event) {
        @Nullable Counter counter = getCounter(event, false);
        return counter == null ? 0L : counter.getCumulativeValue();
    }

    /**
     * Get the total count for a given event.
     *
     * @param event the event to get count information for
     *
     * @return the total number times that event was recorded
     */
    public int getCount(Event event) {
        @Nullable Counter counter = getCounter(event, false);
        return counter == null ? 0 : counter.getCount();
    }

    /**
     * Get the total size for a given event.
     *
     * @param event the event to get size information for
     *
     * @return the total size of the recorded event
     */
    public long getSize(SizeEvent event) {
        @Nullable Counter counter = getCounter(event, false);
        return counter == null ? 0 : counter.getCumulativeValue();
    }

    /**
     * Get the total time spent for a given event that timed out.
     *
     * @param event the event to get time information for
     *
     * @return the total number of nanoseconds recorded for when the event timed out
     */
    public long getTimeoutTimeNanos(Event event) {
        @Nullable Counter counter = getTimeoutCounter(event, false);
        return counter == null ? 0L : counter.getCumulativeValue();
    }

    /**
     * Get the total count of timeouts for a given event.
     *
     * @param event the event to get timeout information for
     *
     * @return the total number times that event was recorded as timed out
     */
    public int getTimeoutCount(Event event) {
        @Nullable Counter counter = getTimeoutCounter(event, false);
        return counter == null ? 0 : counter.getCount();
    }

    /**
     * Return the set of aggregates that this store timer can produce. This method is expected to be
     * overridden by implementation in order to expose whichever aggregates they produce. Note that
     * the aggregates that are returned are the complete set that are defined for the store timer,
     * however it is possible that a returned aggregate may not have any of the underlying counters
     * necessary to compute its value (see {@link Aggregate#compute(StoreTimer)}).
     *
     * @return the set of aggregates that can be computed by this timer
     */
    @Nonnull
    public Set<Aggregate> getAggregates() {
        return Collections.emptySet();
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

    public void add(StoreTimer other) {
        for (Map.Entry<StoreTimer.Event, Counter> entry : other.counters.entrySet()) {
            Counter thisCounter = Objects.requireNonNull(getCounter(entry.getKey(), true));
            thisCounter.add(entry.getValue());
        }
        for (Map.Entry<StoreTimer.Event, Counter> entry : other.timeoutCounters.entrySet()) {
            Counter thisCounter = Objects.requireNonNull(getTimeoutCounter(entry.getKey(), true));
            thisCounter.add(entry.getValue());
        }
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
            result.put(event.logKeyWithSuffix("_count"), counter.count.get());
            if (event instanceof SizeEvent) {
                result.put(event.logKeyWithSuffix("_size"), counter.getCumulativeValue());
            } else if (!(event instanceof Count)) {
                result.put(event.logKeyWithSuffix("_micros"), counter.getTimeNanos() / 1000L);
            }
        }

        // now add recorded timeout events to map
        for (Event timeoutEvent : timeoutEvents) {
            result.put(timeoutEvent.logKeyWithSuffix("_timeout_micros"), getTimeoutTimeNanos(timeoutEvent) / 1000L);
            result.put(timeoutEvent.logKeyWithSuffix("_timeout_count"), getTimeoutCount(timeoutEvent));
        }

        for (Aggregate aggregate : getAggregates()) {
            @Nullable Counter counter = aggregate.compute(this);
            if (counter != null) {
                result.put(aggregate.logKeyWithSuffix("_count"), counter.count.get());
                if (!(aggregate instanceof Count)) {
                    result.put(aggregate.logKeyWithSuffix("_micros"), counter.getTimeNanos() / 1000L);
                }
            }
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
     * @param <T> the type of the future
     *
     * @return a new future that will be complete after also recording timing information
     */
    public <T> CompletableFuture<T> instrument(Event event, CompletableFuture<T> future) {
        // TODO remove executor form the API as it is not needed
        return instrument(event, future, null);
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
        return instrumentAsync(Collections.singleton(event), future, System.nanoTime());
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
        return instrumentAsync(events, future, System.nanoTime());
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
        return instrumentAsync(Collections.singleton(event), future, startTime);
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
        return instrumentAsync(events, future, startTime);
    }

    /**
     * Instrument an asynchronous cursor.
     * Timing information is recorded for each invocation of the {@link RecordCursor#onNext()} asynchronous method.
     *
     * @param event the event type to use to record timing
     * @param inner the cursor to record timing information for
     * @param <T> the type of the cursor elements
     *
     * @return a new cursor that returns the same elements and also records timing information
     */
    public <T> RecordCursor<T> instrument(Event event, RecordCursor<T> inner) {
        return new RecordCursor<T>() {
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
            public void close() {
                inner.close();
            }

            @Override
            public boolean isClosed() {
                return inner.isClosed();
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

    protected <T> CompletableFuture<T> instrumentAsync(Set<Event> events, CompletableFuture<T> future, long startTime) {
        return future.whenComplete((result, exception) -> {
            long timeDifference = System.nanoTime() - startTime;
            for (Event event : events) {
                record(event, timeDifference);
            }
        });
    }
}
