/*
 * PlannerConfiguration.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * This contains a set of configurations given to the planner that fine-tunes its behavior.
 * <br>
 * @implNote Currently this only contains a list of <u>readable</u> indexes, but in the future
 * more configurations will be added, mostly reflecting what is already defined in {@link RecordQueryPlannerConfiguration}
 * and consolidate some of the configurations defined in {@link com.apple.foundationdb.relational.api.Options}.
 * <br>
 * Relevant Issues:
 * <ul>
 *     <li>TODO (Expose planner configuration parameters like index scan preference)</li>
 *     <li>TODO (Interaction between planner configurations and query cache)</li>
 * </ul>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class PlannerConfiguration {

    @Nonnull
    private static final PlannerConfiguration ALL_AVAILABLE_INDEXES = new PlannerConfiguration(Optional.empty());

    @Nonnull
    private final Optional<Set<String>> readableIndexes;

    private PlannerConfiguration(@Nonnull Optional<Set<String>> readableIndexes) {
        this.readableIndexes = readableIndexes;
    }

    @Nonnull
    public Optional<Set<String>> getReadableIndexes() {
        return readableIndexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlannerConfiguration that = (PlannerConfiguration) o;
        return Objects.equals(readableIndexes, that.readableIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readableIndexes);
    }

    @Nonnull
    public static PlannerConfiguration from(@Nonnull final Optional<Set<String>> readableIndexes) {
        if (readableIndexes.isEmpty()) {
            return ALL_AVAILABLE_INDEXES;
        }
        return new PlannerConfiguration(readableIndexes);
    }

    @Nonnull
    public static PlannerConfiguration ofAllAvailableIndexes() {
        return ALL_AVAILABLE_INDEXES;
    }
}
