/*
 * TimeScanLimiter.java
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Track time remaining until a given time limit, after which record scans should not be allowed.
 *
 * @see com.apple.foundationdb.record.cursors.CursorLimitManager
 */
@API(API.Status.UNSTABLE)
public class TimeScanLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeScanLimiter.class);

    private final long startTime;
    private final long timeLimitMillis;
    protected boolean isTimedOut;

    public TimeScanLimiter(long startTime, long timeLimitMillis) {
        this.startTime = startTime;
        this.timeLimitMillis = (timeLimitMillis <= 0L ? ExecuteProperties.UNLIMITED_TIME : timeLimitMillis);
        this.isTimedOut = timeLimitMillis != ExecuteProperties.UNLIMITED_TIME && (System.currentTimeMillis() - startTime) >= timeLimitMillis;
    }

    public boolean tryRecordScan() {
        if (!isTimedOut && timeLimitMillis != ExecuteProperties.UNLIMITED_TIME) {
            final long now = System.currentTimeMillis();
            if ((now - startTime) >= timeLimitMillis) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("Cursor time limit exceeded",
                                    LogMessageKeys.CURSOR_ELAPSED_MILLIS, (now - startTime),
                                    LogMessageKeys.CURSOR_TIME_LIMIT_MILLIS, timeLimitMillis));
                }
                isTimedOut = true;
            }
        }
        return !isTimedOut;
    }
}
