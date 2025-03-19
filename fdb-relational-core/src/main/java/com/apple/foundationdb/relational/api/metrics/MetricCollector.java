/*
 * MetricCollector.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metrics;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Supplier;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * MetricCollector provides a utility API to register events and counts while performing operations in the Relational
 * system.
 *
 * The probable implementations of it is supposed to be backed by a store/registry of some sort that can hold the
 * metric data and can decide what needs to be done with it. The implementation can also decide on the lifetime of a
 * collector instance - ranging from a singleton object for the system instance to being transaction-bound and
 * short-lived.
 *
 * Given the lifetime variability, the getters in this interface should be carefully implemented to respect the
 * contract.
 * */
public interface MetricCollector {

    /**
     * Increments the count event by 1.
     *
     * @param count     the count event to be modified
     * */
    void increment(@Nonnull RelationalMetric.RelationalCount count);

    /**
     * Records the time taken to execute a function.
     *
     * @param event     the event to be modified
     * @param supplier  the function that is to be timed
     * @return the value returned by the supplier
     * @throws RelationalException that is passed on by the supplier
     * */
    <T> T clock(@Nonnull RelationalMetric.RelationalEvent event, Supplier<T> supplier) throws RelationalException;

    /**
     * Returns the aggregate time taken by all the occurrences of a particular event in the lifetime of collector.
     *
     * @param event     the event whose time is needed
     * @return the aggregate time in {@link java.util.concurrent.TimeUnit#MICROSECONDS}.
     * @throws RelationalException in case the requested event is not held in the collector.
     * */
    default double getAverageTimeMicrosForEvent(@Nonnull RelationalMetric.RelationalEvent event) throws RelationalException {
        throw new RelationalException(String.format(Locale.ROOT, "Requested event metric: %s is not in the collector", event.title()), ErrorCode.INTERNAL_ERROR);
    }

    /**
     * Returns the total counter value of a particular count event in the lifetime of collector.
     *
     * @param count     the count event whose counter is needed
     * @return the counter value
     * @throws RelationalException in case the requested count is not held in the collector.
     * */
    default long getCountsForCounter(@Nonnull RelationalMetric.RelationalCount count) throws RelationalException {
        throw new RelationalException(String.format(Locale.ROOT, "Requested count metric: %s is not in the collector", count.title()), ErrorCode.INTERNAL_ERROR);
    }

    /**
     * Return {@code true} if a particular count event is registered with the collector.
     *
     * @param count the count event whose counter is to be checked.
     * @return {@code  true} if counter is present, else false.
     */
    default boolean hasCounter(@Nonnull RelationalMetric.RelationalCount count) {
        return false;
    }

    /**
     * Processes the registered instruments before probably destroying the collector object. This should be called for
     * safety to ensure that the events are not lost when destroying the collector.
     * */
    default void flush() {
    }
}
