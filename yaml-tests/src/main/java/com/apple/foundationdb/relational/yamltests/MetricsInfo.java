/*
 * MetricsInfo.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Objects;

public final class MetricsInfo {
    @Nonnull
    private final PlannerMetricsProto.Info underlying;
    @Nonnull
    private final Path filePath;
    private final int lineNumber;

    MetricsInfo(@Nonnull PlannerMetricsProto.Info metricsInfo,
                @Nonnull Path filePath,
                int lineNumber) {
        this.underlying = metricsInfo;
        this.filePath = filePath;
        this.lineNumber = lineNumber;
    }

    @Nonnull
    public PlannerMetricsProto.Info getUnderlying() {
        return underlying;
    }

    @Nonnull
    public Path getFilePath() {
        return filePath;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    @Nonnull
    public String getExplain() {
        return underlying.getExplain();
    }

    @Nonnull
    public PlannerMetricsProto.CountersAndTimers getCountersAndTimers() {
        return underlying.getCountersAndTimers();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        final MetricsInfo that = (MetricsInfo)object;
        return lineNumber == that.lineNumber && Objects.equals(underlying, that.underlying) && Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(underlying, filePath, lineNumber);
    }
}
