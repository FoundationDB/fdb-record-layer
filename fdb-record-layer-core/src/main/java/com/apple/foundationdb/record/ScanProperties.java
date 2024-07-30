/*
 * ScanProperties.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * A group of properties that pertain to a single scan.
 * Among these properties is an {@link ExecuteProperties}, which holds the properties that pertain to an entire
 * execution.
 */
@API(API.Status.MAINTAINED)
public class ScanProperties {
    public static final ScanProperties FORWARD_SCAN = new ScanProperties(ExecuteProperties.newBuilder()
            .setReturnedRowLimit(Integer.MAX_VALUE)
            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
            .build());
    public static final ScanProperties REVERSE_SCAN = new ScanProperties(ExecuteProperties.newBuilder()
            .setReturnedRowLimit(Integer.MAX_VALUE)
            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
            .build(), true);


    @Nonnull
    private final ExecuteProperties executeProperties;

    // whether to read the entries in reverse order
    private final boolean reverse;

    @Nonnull
    private final CursorStreamingMode cursorStreamingMode;

    /**
     * Creates scan properties.
     * @param executeProperties the execution properties (such as isolation level and row limit) associated with this scan
     */
    public ScanProperties(@Nonnull ExecuteProperties executeProperties) {
        this(executeProperties, false);
    }

    /**
     * Creates scan properties.
     * @param executeProperties the execution properties (such as isolation level and row limit) associated with this scan
     * @param reverse if true, the scan direction will be reversed
     */
    public ScanProperties(@Nonnull ExecuteProperties executeProperties, boolean reverse) {
        this(executeProperties, reverse, executeProperties.getDefaultCursorStreamingMode());
    }

    /**
     * Creates scan properties.
     * @param executeProperties the execution properties (such as isolation level and row limit) associated with this scan
     * @param reverse if true, the scan direction will be reversed
     * @param cursorStreamingMode streaming mode to use if opening an FDB cursor
     */
    public ScanProperties(@Nonnull ExecuteProperties executeProperties, boolean reverse, @Nonnull CursorStreamingMode cursorStreamingMode) {
        this.executeProperties = executeProperties;
        this.reverse = reverse;
        this.cursorStreamingMode = cursorStreamingMode;
    }

    /**
     * Get direction of scans.
     * @return {@code true} if scan is in reverse order
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Get execute properties for this scan properties.
     * @return execute properties for this scan properties.
     */
    @Nonnull
    public ExecuteProperties getExecuteProperties() {
        return executeProperties;
    }

    /**
     * Change execute properties.
     * @param modifier a function to produce a new execute properties from the current one
     * @return a new scan properties with updated execute properties
     */
    @Nonnull
    public ScanProperties with(@Nonnull Function<ExecuteProperties, ExecuteProperties> modifier) {
        return new ScanProperties(modifier.apply(executeProperties), reverse, cursorStreamingMode);
    }

    /**
     * Change direction of scans.
     * @param reverse {@code true} if scan is in reverse order
     * @return a new scan properties with given direction
     */
    @Nonnull
    public ScanProperties setReverse(boolean reverse) {
        if (reverse == isReverse()) {
            return this;
        }
        return new ScanProperties(executeProperties, reverse, cursorStreamingMode);
    }

    /**
     * Get cursor streaming mode.
     * @return cursor streaming mode
     */
    @Nonnull
    public CursorStreamingMode getCursorStreamingMode() {
        return cursorStreamingMode;
    }

    /**
     * Set cursor streaming mode.
     * @param cursorStreamingMode cursor streaming mode to set
     * @return a new scan properties with given streaming mode
     */
    @Nonnull
    public ScanProperties setStreamingMode(@Nonnull CursorStreamingMode cursorStreamingMode) {
        if (cursorStreamingMode == getCursorStreamingMode()) {
            return this;
        }
        return new ScanProperties(executeProperties, reverse, cursorStreamingMode);
    }

    @Override
    public String toString() {
        return "ScanProperties(" + executeProperties
                + ", direction: " + (reverse ? "reverse" : "forward")
                + ", streaming mode: " + cursorStreamingMode
                + ")";
    }
}
