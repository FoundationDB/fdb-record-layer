/*
 * FDBLatencySource.java
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

import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Indicates which FDB API calls are to have latency injected into them via {@link FDBDatabaseFactory#setLatencyInjector(Function)}.
 */
public enum FDBLatencySource {
    /**
     * See {@link FDBRecordContext#getReadVersion()}.
     */
    GET_READ_VERSION(FDBStoreTimer.Events.INJECTED_GET_READ_VERSION_LATENCY),

    /**
     * See {@link FDBRecordContext#commitAsync()}.
     */
    COMMIT_ASYNC(FDBStoreTimer.Events.INJECTED_COMMIT_LATENCY),
    ;

    @Nonnull
    private final StoreTimer.Event event;

    FDBLatencySource(@Nonnull StoreTimer.Event event) {
        this.event = event;
    }

    /**
     * Get the event used to track this latency source by {@link FDBStoreTimer}s.
     *
     * @return the event used to instrument injecting this latency source
     */
    @Nonnull
    public StoreTimer.Event getTimerEvent() {
        return event;
    }
}
