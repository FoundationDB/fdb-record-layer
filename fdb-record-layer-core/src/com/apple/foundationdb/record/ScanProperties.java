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

import com.apple.foundationdb.StreamingMode;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * A group of properties that pertain to a single scan.
 * Among these properties is an {@link ExecuteProperties}, which holds the properties that pertain to an entire
 * execution.
 */
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
    private final StreamingMode streamingMode;

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
        this(executeProperties, reverse, executeProperties.getDefaultStreamingMode());
    }

    /**
     * Creates scan properties.
     * @param executeProperties the execution properties (such as isolation level and row limit) associated with this scan
     * @param reverse if true, the scan direction will be reversed
     * @param streamingMode streaming mode to use if opening an FDB cursor
     */
    public ScanProperties(@Nonnull ExecuteProperties executeProperties, boolean reverse, StreamingMode streamingMode) {
        this.executeProperties = executeProperties;
        this.reverse = reverse;
        this.streamingMode = streamingMode;
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    public ExecuteProperties getExecuteProperties() {
        return executeProperties;
    }

    @Nonnull
    public ScanProperties with(@Nonnull Function<ExecuteProperties, ExecuteProperties> modifier) {
        return new ScanProperties(modifier.apply(executeProperties), reverse, streamingMode);
    }

    @Nonnull
    public ScanProperties setReverse(boolean reverse) {
        if (reverse == isReverse()) {
            return this;
        }
        return new ScanProperties(executeProperties, reverse, streamingMode);
    }

    @Nonnull
    public StreamingMode getStreamingMode() {
        return streamingMode;
    }

    @Nonnull
    public ScanProperties setStreamingMode(@Nonnull StreamingMode streamingMode) {
        if (streamingMode == getStreamingMode()) {
            return this;
        }
        return new ScanProperties(executeProperties, reverse, streamingMode);
    }
}
