/*
 * PlannerPhase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * TBD.
 */
public enum PlannerPhase {
    PLANNING(PlanningRuleSet.getDefault(), PlannerStage.PHYSICAL),
    CANONICALIZATION(CanonicalizationRuleSet.getDefault(), PlannerStage.CANONICAL, PLANNING);

    @Nonnull
    private final CascadesRuleSet ruleSet;
    @Nonnull
    private final PlannerStage targetStage;
    @Nullable
    private final PlannerPhase nextPhase;

    PlannerPhase(@Nonnull final CascadesRuleSet ruleSet,
                 @Nonnull final PlannerStage targetStage) {
        this(ruleSet, targetStage, null);
    }

    PlannerPhase(@Nonnull final CascadesRuleSet ruleSet,
                 @Nonnull final PlannerStage targetStage,
                 @Nullable final PlannerPhase nextPhase) {
        this.ruleSet = ruleSet;
        this.targetStage = targetStage;
        this.nextPhase = nextPhase;
    }

    @Nonnull
    public CascadesRuleSet getRuleSet() {
        return ruleSet;
    }

    @Nonnull
    public PlannerStage getTargetStage() {
        return targetStage;
    }

    @Nonnull
    public PlannerPhase getNextPhase() {
        return Objects.requireNonNull(nextPhase);
    }

    public boolean hasNextPhase() {
        return nextPhase != null;
    }
}
