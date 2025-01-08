/*
 * FDBTraceFormat.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * The FoundationDB native client can be configured to emit its trace logs (containing important
 * metrics and instrumentation information) in one of several different formats. This enum can be
 * passed to the {@link FDBDatabaseFactory#setTraceFormat(FDBTraceFormat)} method in order to
 * control the client's behavior.
 *
 * <p>
 * This class is {@link API.Status#UNSTABLE}. However, additional values to this enum may be added
 * if more trace format options become available.
 * </p>
 */
@API(API.Status.UNSTABLE)
public enum FDBTraceFormat {

    /**
     * Use the system default. If the {@code FDB_NETWORK_OPTION_TRACE_FORMAT} environment variable
     * is set or if one has already called {@link com.apple.foundationdb.NetworkOptions#setTraceFormat(String)},
     * then the client trace logs will follow whatever format was already specified. If neither of
     * those are set, then the logs will used the default format, which is {@link #XML}.
     */
    DEFAULT("", true),

    /**
     * Format the trace logs as XML. The full logs are contained within {@code Trace} tags, and each
     * log entry is a single-line {@code TraceEvent} XML element. Each {@code TraceEvent} will associate
     * log keys and values as attributes of the element (<em>not</em> as child elements). Logs will
     * be written to files with a {@code .xml} extension.
     */
    XML("xml", false),

    /**
     * Format the trace logs as JSON. Each log entry is a single-line JSON document. Each entry will associate
     * log keys and values as fields of the JSON document. Logs will be written to files with a {@code .json}
     * extension.
     */
    JSON("json", false);

    @Nonnull
    private final String optionValue;
    private final boolean defaultValue;

    FDBTraceFormat(@Nonnull String optionValue, boolean defaultValue) {
        this.optionValue = optionValue;
        this.defaultValue = defaultValue;
    }

    /**
     * Get the value to supply to {@link com.apple.foundationdb.NetworkOptions#setTraceFormat(String) setTraceFormat()}.
     * The exception here is the default value which can be achieved by not supplying any value to that option.
     *
     * @return the value to pass to {@link com.apple.foundationdb.NetworkOptions#setTraceFormat(String) setTraceFormat()}
     */
    @Nonnull
    public String getOptionValue() {
        return optionValue;
    }

    /**
     * Get whether this is the default trace format. See {@link #DEFAULT} for more details.
     *
     * @return whether this indicates the client should use the default trace format
     */
    public boolean isDefaultValue() {
        return defaultValue;
    }
}
