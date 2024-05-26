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
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.Function;

/**
 * Enum to hold information about the stage of this reference. The stage implies who or what process
 * is responsible for the current state of the reference. The stage of a reference is set upon the creation of the
 * reference and cannot be freely changed. In fact, only the (re-)exploration of a reference can mutate the
 * stage.
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
     * The {@link Reference} is tagged with {@code QUERY} iff it was created outside the Cascades planner by the
     * client.
     */
    QUERY(members -> new ExpressionPropertiesMap<>(RelationalExpression.class, ImmutableSet.of(), members)),
    /**
     * The {@link Reference} is tagged with {@code CANONICAL} iff it was created by the Cascades planner and is
     * the direct result of the canonicalization of a query graph.
     */
    CANONICAL(members -> new ExpressionPropertiesMap<>(RelationalExpression.class, ImmutableSet.of(), members)),
    /**
     * The {@link Reference} is tagged with {@code PHYSICAL} iff it was created by the Cascades planner and it
     * the direct result of the physical planning of a query. All final {@link RelationalExpression}s contained in
     * references of this stage are by convention also {@link RecordQueryPlan}s.
     */
    PHYSICAL(PlanPropertiesMap::new);

    @Nonnull
    private final Function<Collection<? extends RelationalExpression>, ExpressionPropertiesMap<? extends RelationalExpression>> propertiesMapCreator;

    PlannerStage(@Nonnull final Function<Collection<? extends RelationalExpression>, ExpressionPropertiesMap<? extends RelationalExpression>> propertiesMapCreator) {
        this.propertiesMapCreator = propertiesMapCreator;
    }

    @Nonnull
    public ExpressionPropertiesMap<? extends RelationalExpression> createPropertiesMap(@Nonnull final Collection<? extends RelationalExpression> members) {
        return propertiesMapCreator.apply(members);
    }
}
