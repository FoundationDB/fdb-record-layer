/*
 * RepairStatsResults.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A container for validation stats collected through the validation process.
 * {@link RecordRepairStatsRunner}
 */
@API(API.Status.EXPERIMENTAL)
public class RepairStatsResults {
    @Nonnull
    private final Map<String, AtomicInteger> stats;
    @Nullable
    private Throwable exceptionCaught;

    RepairStatsResults() {
        this.stats = new HashMap<>();
    }

    /**
     * Get the stats collected during the validation process.
     * @return A Map of error code to a count of the times that this code was encountered
     */
    @Nonnull
    public Map<String, Integer> getStats() {
        return stats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    /**
     * Return an exception (if caught) during the iteration (in which case the iteration may be incomplete).
     * @return an exception caught (if any)
     */
    @Nullable
    public Throwable getExceptionCaught() {
        return exceptionCaught;
    }

    void increment(String code) {
        stats.computeIfAbsent(code, ignore -> new AtomicInteger(0)).incrementAndGet();
    }

    void setExceptionCaught(final Throwable ex) {
        this.exceptionCaught = ex;
    }
}
