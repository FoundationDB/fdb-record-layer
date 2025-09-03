/*
 * PlannerStage.java
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

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * Enum to hold information about the stage of a reference. The stage implies who or what process is responsible for the
 * current state of the reference. The stage of a reference is set upon the creation of the reference and cannot be
 * freely changed. In fact, only the (re-)exploration of a reference can mutate the stage.
 *
 * <br>
 * The following rules apply:
 * <ul>
 *    <li>
 *        A {@link CascadesRuleCall} executes in the context of a stage.
 *    </li>
 *    <li>
 *        Only the (re-)exploration of a reference can mutate the stage towards a higher stage (based on the
 *        ordinal of the stage).
 *    </li>
 *    <li>
 *        Changing the stage of a reference removes all properties from the reference.
 *    </li>
 *    <li>
 *        Upon exploring the references a {@link RelationalExpression} ranges over, the following three cases may
 *        occur:
 *        <ol>
 *            <li>
 *                The child reference's stage is lower than the stage of the context. We change the child's stage
 *                to the stage of the context and explore the child reference.
 *            </li>
 *            <li>
 *                The child reference's stage is equal to the stage of the context. We (re-)explore the child
 *                reference according to normal exploration rules.
 *            </li>
 *            <li>
 *                The child reference's stage is higher than the stage of the context. We do not explore the child
 *                reference and consider all therein contained members final with respect to the context stage.
 *            </li>
 *        </ol>
 *    </li>
 * </ul>
 */
public enum PlannerStage {
    /**
     * Initial stage. The {@link Reference} is tagged with {@code INITIAL} iff
     * <ul>
     *     <li>
     *         it was created outside the Cascades planner by the client and has not undergone any planner-originated
     *         transformations OR
     *     </li>
     *     <li>
     *         it was explicitly created by a planner rule to signal the planner to start exploring the reference using
     *        the rewrite phase rather than a later {@link PlannerPhase}.
     *     </li>
     * </ul>
     */
    INITIAL(ExpressionPropertiesMap::defaultForRewritePhase),
    /**
     * Canonical stage. The {@link Reference} is tagged with {@code CANONICAL} iff it was created by the Cascades
     * planner and is the direct result of the rewrite phase of a query graph.
     */
    CANONICAL(ExpressionPropertiesMap::defaultForRewritePhase),
    /**
     * The {@link Reference} is tagged with {@code PLANNED} iff it was created by the Cascades planner and it
     * the direct result of the physical planning of a query. All final {@link RelationalExpression}s contained in
     * references of this stage are by convention also {@link RecordQueryPlan}s.
     */
    PLANNED(PlanPropertiesMap::new);

    @Nonnull
    private final Supplier<ExpressionPropertiesMap<? extends RelationalExpression>> propertiesMapCreator;

    PlannerStage(@Nonnull final Supplier<ExpressionPropertiesMap<? extends RelationalExpression>> propertiesMapCreator) {
        this.propertiesMapCreator = propertiesMapCreator;
    }

    @Nonnull
    public ExpressionPropertiesMap<? extends RelationalExpression> createPropertiesMap() {
        return propertiesMapCreator.get();
    }

    /**
     * Returns iff the {@link PlannerStage} passed in directly precedes this stage.
     * @param other another stage that is not this stage
     * @return {@code true} iff the {@link PlannerStage} passed in directly precedes this stage.
     */
    public boolean directlyPrecedes(@Nonnull final PlannerStage other) {
        return precedes(other) && ordinal() == other.ordinal() - 1;
    }

    /**
     * Returns iff the {@link PlannerStage} passed in precedes this stage.
     * @param other another stage that is not this stage
     * @return {@code true} iff the {@link PlannerStage} passed in precedes this stage.
     */
    public boolean precedes(@Nonnull final PlannerStage other) {
        Verify.verify(this != other);
        return ordinal() < other.ordinal();
    }

    /**
     * Returns iff the {@link PlannerStage} passed in directly succeeds this stage.
     * @param other another stage that is not this stage
     * @return {@code true} iff the {@link PlannerStage} passed in succeeds this stage.
     */
    public boolean directlySucceeds(@Nonnull final PlannerStage other) {
        return succeeds(other) && ordinal() == other.ordinal() + 1;
    }

    /**
     * Returns iff the {@link PlannerStage} passed in succeeds this stage.
     * @param other another stage that is not this stage
     * @return {@code true} iff the {@link PlannerStage} passed in directly succeeds this stage.
     */
    public boolean succeeds(@Nonnull final PlannerStage other) {
        Verify.verify(this != other);
        return ordinal() > other.ordinal();
    }
}
