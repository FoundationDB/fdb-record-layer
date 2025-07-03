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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * This contains a set of configurations given to the planner that fine-tunes its behavior.
 *
 * <p>
 * Note: Currently this only contains a list of <em>readable</em> indexes, but in the future
 * more configurations will be added, mostly reflecting what is already defined in {@link RecordQueryPlannerConfiguration}
 * and consolidate some of the configurations defined in {@link com.apple.foundationdb.relational.api.Options}.
 * </p>
 * <br>
 * Relevant Issues:
 * <ul>
 *     <li>TODO (Expose planner configuration parameters like index scan preference)</li>
 *     <li>TODO (Interaction between planner configurations and query cache)</li>
 * </ul>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public final class PlannerConfiguration {

    @Nonnull
    private final Optional<Set<String>> readableIndexes;

    @Nonnull
    private final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration;

    private final int memoizedHash;

    private PlannerConfiguration(@Nonnull final Optional<Set<String>> readableIndexes,
                                 @Nonnull final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration) {
        this.readableIndexes = readableIndexes;
        this.recordQueryPlannerConfiguration = recordQueryPlannerConfiguration;
        this.memoizedHash = computeHash();
    }

    @Nonnull
    public Optional<Set<String>> getReadableIndexes() {
        return readableIndexes;
    }

    @Nonnull
    public RecordQueryPlannerConfiguration getRecordQueryPlannerConfiguration() {
        return recordQueryPlannerConfiguration;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final var that = (PlannerConfiguration) other;
        return Objects.equals(readableIndexes, that.readableIndexes)
                && this.recordQueryPlannerConfiguration.getIndexFetchMethod()
                     .equals(that.recordQueryPlannerConfiguration.getIndexFetchMethod())
                && this.recordQueryPlannerConfiguration.getDisabledTransformationRules()
                     .equals(that.recordQueryPlannerConfiguration.getDisabledTransformationRules());
    }

    private int computeHash() {
        return Objects.hash(readableIndexes, recordQueryPlannerConfiguration.getIndexFetchMethod(),
                recordQueryPlannerConfiguration.getDisabledTransformationRules());
    }

    @Override
    public int hashCode() {
        return memoizedHash;
    }

    @Nonnull
    public static PlannerConfiguration of(@Nonnull final Optional<Set<String>> readableIndexesMaybe,
                                          @Nonnull final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration) {
        return new PlannerConfiguration(readableIndexesMaybe, recordQueryPlannerConfiguration);
    }

    @Nonnull
    public static PlannerConfiguration of(@Nonnull final Set<String> readableIndexes,
                                          @Nonnull final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration) {
        return new PlannerConfiguration(Optional.of(readableIndexes), recordQueryPlannerConfiguration);
    }

    @Nonnull
    public static PlannerConfiguration ofAllAvailableIndexes(@Nonnull final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration) {
        return new PlannerConfiguration(Optional.empty(), recordQueryPlannerConfiguration);
    }
}
