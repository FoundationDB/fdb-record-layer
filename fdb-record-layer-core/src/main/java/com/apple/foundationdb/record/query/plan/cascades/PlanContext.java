/*
 * PlanContext.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A basic context object that stores all metadata about a record store, such as the available indexes.
 * It provides access to this information to the planner and the
 * {@link AbstractCascadesRule#onMatch(PlannerRuleCall)} method.
 */
@API(API.Status.EXPERIMENTAL)
public interface PlanContext {
    PlanContext EMPTY_CONTEXT = new PlanContext() {
        @Nonnull
        @Override
        public RecordQueryPlannerConfiguration getPlannerConfiguration() {
            return RecordQueryPlannerConfiguration.defaultPlannerConfiguration();
        }

        @Nonnull
        @Override
        public Set<MatchCandidate> getMatchCandidates() {
            return Set.of();
        }
    };

    @Nonnull
    RecordQueryPlannerConfiguration getPlannerConfiguration();

    @Nonnull
    Set<MatchCandidate> getMatchCandidates();

    @Nonnull
    static PlanContext emptyContext() {
        return EMPTY_CONTEXT;
    }
}
