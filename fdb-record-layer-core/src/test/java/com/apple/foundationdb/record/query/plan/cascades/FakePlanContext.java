/*
 * FakePlanContext.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A mock implementation of a {@link PlanContext} used to test certain planner rules that don't need a full plan context.
 */
public class FakePlanContext implements PlanContext {
    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getPlannerConfiguration() {
        return RecordQueryPlannerConfiguration.builder().build();
    }

    @Nonnull
    @Override
    public Set<MatchCandidate> getMatchCandidates() {
        return ImmutableSet.of();
    }
}
