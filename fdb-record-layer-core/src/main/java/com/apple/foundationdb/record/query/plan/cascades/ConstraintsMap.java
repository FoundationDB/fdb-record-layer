/*
 * ConstraintsMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.google.common.base.Verify;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * <p>
 * A map to keep track of constraining attributes for an expression reference. References ({@link Reference})
 * are a central part of the memoization effort within the {@link CascadesPlanner}. Member expressions contained in
 * a reference all yield the same final result not taking into account physical properties such as sorted-ness,
 * existence of duplicates, etc. A set of plan members of a reference can be partitioned into subsets by physical
 * attributes in order to be used by conscious rules that are dependent on a particular set of properties. Many times
 * we need to create all possible plans in the anticipation that these plans are all consumed by other implementation
 * rules later on. That is not guaranteed, though, and in reality a large set of plans does not get used due to an
 * incompatible physical attribute. In any case, the planner must explore plan variations once created even though
 * the resulting structures may not get used at all. In essence, we would like to avoid the creation of useless plans
 * if at all possible.
 * </p>
 * <p>
 * A map of interesting constraints is used by a reference to "push down" such a constraint through the data
 * flow graph. The "pushing down" is facilitated by rules that are executed by the {@link CascadesPlanner} much like
 * other rules yielding new {@link RelationalExpression}s or {@link PartialMatch}es. The only, and very important,
 * difference in concept is that the information is passed downwards instead of bubbling up like all the other structures.
 * </p>
 * <p>
 * The planner needs to make sure that expression references are explored, and if necessary, re-explored if new
 * interesting properties are declared through the execution of other planner tasks.
 * Planner rules ({@link CascadesRuleCall}) can declare to interest in the change of a constraint which then signals to
 * the planner to trigger a re-exploration using that rule if an constraint changes on that reference.
 * </p>
 * <p>
 * The map implementation uses a logical clock and a watermark system to understand if a re-exploration becomes
 * necessary.
 * </p>
 */
public class ConstraintsMap {
    /**
     * Current tick of the map. Gets updated through {@link #bumpTick()} and indicates that a change in the properties
     * map needs to trigger (re-)exploration of appropriate rules.
     */
    private long currentTick;

    /**
     * The watermark goal tick. This tick is between the {@link #watermarkCommittedTick} and the {@link #currentTick}.
     * This needs to be tracked as this tick is used to understand that a particular change to the interesting properties
     * has caused a proper (re-)exploration of the affected sub dag. If other pushes happen through other rules,
     * {@link #currentTick} will get further bumped and needs to cause further exploration. This tick tells that
     * algorithm that exploration has at least happened to the goal tick.
     */
    private long watermarkGoalTick;

    /**
     * This is the committed tick. All exploration is up-to-date to at least that tick.
     */
    private long watermarkCommittedTick;

    /**
     * The map for the constraints of a reference. Note that this map is stable with respect to iteration.
     */
    private final Map<PlannerConstraint<?>, ConstraintEntry> attributeToConstraintMap;

    /**
     * Default constructor.
     */
    public ConstraintsMap() {
        this.currentTick = 0L;
        this.watermarkGoalTick = -1L;
        this.watermarkCommittedTick = -1L;
        this.attributeToConstraintMap = Maps.newLinkedHashMap();
    }

    public long getCurrentTick() {
        return currentTick;
    }

    public long getWatermarkGoalTick() {
        return watermarkGoalTick;
    }

    public long getWatermarkCommittedTick() {
        return watermarkCommittedTick;
    }

    /**
     * Method to return if a particular attribute is contained in the map.
     * @param attribute the attribute key to check
     * @param <T> the type of the attribute
     * @return {@code true} is the attribute is contained in this map, {@code false} otherwise.
     */
    public <T> boolean containsAttribute(@Nonnull final PlannerConstraint<T> attribute) {
        return attributeToConstraintMap.containsKey(attribute);
    }

    /**
     * Method to return an optional constraint given the kind of constraint attribute key passed in.
     * @param attribute the attribute key
     * @param <T> the type of the attribute
     * @return {@code Optional.of(property)} if this map contains the property selected by the attribute key handed in,
     *         {@code Optional.empty()} otherwise
     */
    @Nonnull
    public <T> Optional<T> getConstraintOptional(@Nonnull final PlannerConstraint<T> attribute) {
        if (containsAttribute(attribute)) {
            return Optional.of(attribute.narrowConstraint(attributeToConstraintMap.get(attribute).property));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Method to return a constraint given an attribute key passed in or to throw an exception.
     * @param attribute the attribute key
     * @param exceptionSupplier a supplier to be called if an attribute could not be found
     * @param <T> the type of the attribute
     * @param <X> the type of throwable to throw if an attribute cannot be found
     * @return the constraint if this map contains the attribute given by the key handed in
     * @throws X the exception generated by the supplier passed in
     */
    @Nonnull
    public <T, X extends Throwable> T getPropertyOrThrow(@Nonnull final PlannerConstraint<T> attribute, @Nonnull final Supplier<? extends X> exceptionSupplier) throws X {
        if (containsAttribute(attribute)) {
            return attribute.narrowConstraint(attributeToConstraintMap.get(attribute).property);
        } else {
            throw exceptionSupplier.get();
        }
    }

    /**
     * Method to return an optional constraint given an attribute key passed in or an alternative default
     * constraint.
     * @param attribute the attribute key
     * @param defaultConstraint a constraint to be returned if an attribute could not be found
     * @param <T> the type of the attribute
     * @return the constraint contained in this map if there is such a constraint as addressed by the attribute key passed
     *         in or {@code defaultConstraint} if this map does not contain the attribute.
     */
    @Nonnull
    public <T> T getPropertyOrElse(@Nonnull final PlannerConstraint<T> attribute, @Nonnull final T defaultConstraint) {
        if (containsAttribute(attribute)) {
            return attribute.narrowConstraint(attributeToConstraintMap.get(attribute).property);
        } else {
            return defaultConstraint;
        }
    }

    /**
     * Push a constraint into this map. The new constraint may get added to the constraints map as is
     * if this is the first time a constraint of that kind was added. If there is a pre-existing constraint (with
     * respect to the attribute key) already contained in the map, we delegate to {@link PlannerConstraint#combine(Object, Object)}
     * to properly combine the existing and the new constraint. If the new constraint is distinct from the old one
     * (or there is no prior constraint determined the given attribute key), we update the internal state in order to
     * cause (re-)exploration of the current reference.
     * @param attribute the attribute key
     * @param constraint the new constraint to be pushed
     * @param <T> the type of the attribute
     * @return an optional containing the combined constraint if the push operation was successful, or
     *         {@code Optional.empty()} otherwise.
     */
    public <T> Optional<T> pushProperty(@Nonnull final PlannerConstraint<T> attribute, @Nonnull T constraint) {
        if (containsAttribute(attribute)) {
            final ConstraintEntry constraintEntry = attributeToConstraintMap.get(attribute);
            return attribute.combine(attribute.narrowConstraint(constraintEntry.property), constraint)
                    .map(combinedProperty -> {
                        bumpTick();
                        constraintEntry.property = combinedProperty;
                        constraintEntry.lastUpdatedTick = currentTick;
                        return combinedProperty;
                    });
        } else {
            bumpTick();
            attributeToConstraintMap.put(attribute, new ConstraintEntry(constraint));
            return Optional.of(constraint);
        }
    }

    /**
     * Method to establish whether this map is explored.
     * @return {@code true} if further (re-)exploration is required, {@code false} otherwise.
     */
    public boolean isExplored() {
        Verify.verify(watermarkGoalTick <= currentTick);
        return watermarkGoalTick == currentTick;
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored. That means that
     * the reference has started (re-)exploration but has not yet finished it.
     * @return {@code true} if the associated reference is currently being explored, {@code false} otherwise.
     */
    public boolean isExploring() {
        return watermarkGoalTick > watermarkCommittedTick;
    }

    /**
     * This method indicates whether a reference containing this map has not been explored yet.
     * @return {@code true} if the associated reference has not been explored yet, {@code false} otherwise.
     */
    public boolean hasNeverBeenExplored() {
        return watermarkCommittedTick < 0L;
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored for the first time (a full
     * exploration). That means that the reference has started exploration but has not yet finished it.
     * @return {@code true} if the associated reference is currently being fully explored, {@code false} otherwise.
     */
    public boolean isFullyExploring() {
        return hasNeverBeenExplored() && isExploring();
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored because of a change to
     * a property as defined by a set of attributes passed in. That means that the reference has started exploration
     * but has not yet finished it.
     * @param attributes a set of attribute keys
     * @return {@code true} if the associated reference is currently being fully explored, {@code false} otherwise.
     */
    public boolean isExploredForAttributes(@Nonnull final Set<PlannerConstraint<?>> attributes) {
        if (hasNeverBeenExplored()) {
            // never been planned
            return false;
        } else {
            for (final PlannerConstraint<?> interestingKey : attributes) {
                if (containsAttribute(interestingKey)) {
                    final ConstraintEntry constraintEntry = attributeToConstraintMap.get(interestingKey);
                    if (constraintEntry.lastUpdatedTick > watermarkCommittedTick) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    /**
     * Method to be called when exploration of a group is started.
     */
    public void startExploration() {
        watermarkGoalTick = currentTick;
    }

    /**
     * Method to be called when exploration of a group is completed.
     */
    public void commitExploration() {
        watermarkCommittedTick = watermarkGoalTick;
    }

    /**
     * Method to immediately indicate the group has been explored. This corresponds to calling {@link #startExploration()}
     * and then {@link #commitExploration()}.
     */
    public void setExplored() {
        watermarkGoalTick = currentTick;
        watermarkCommittedTick = currentTick;
    }

    /**
     * Advance the current planner stage to a new one that succeeds the current one. Note that this method is
     * destructive. Much like the owning {@link Reference} of this map (when advanced to a new stage) removes earlier
     * exploratory members and cleans out its {@link ExpressionPropertiesMap}, the constraints map also has to be
     * cleaned up before we can explore the group again. For the constraints map, specifically, we need to remove all
     * constraints that were pushed during an earlier {@link PlannerStage} but preserve all constraints that were pushed
     * during the one we are advancing to. This is necessary because constraints can be pushed into the owning
     * {@link Reference} of this constraints map before that reference itself is advanced when it starts exploration
     * again. We know that those earlier pushes came from a tick smaller than {@link #watermarkCommittedTick} while all
     * pushes that happened after {@link #watermarkCommittedTick} come from the current stage (and should be preserved).
     */
    public void advancePlannerStage() {
        // remove all constraints previously pushed and already committed
        for (final var iterator = this.attributeToConstraintMap.entrySet().iterator(); iterator.hasNext(); ) {
            final var entry = iterator.next();
            final var constraintEntry = entry.getValue();
            if (constraintEntry.lastUpdatedTick < watermarkCommittedTick) {
                iterator.remove();
            }
        }
        // we want to go back to -1L for both to indicate that we have never been explored before (in this new stage).
        this.watermarkGoalTick = -1L;
        this.watermarkCommittedTick = -1L;
    }

    @CanIgnoreReturnValue
    private long bumpTick() {
        return ++currentTick;
    }

    /**
     * Method to inherit all state from another constraints map.
     * @param otherConstraintsMap the other constraints map
     */
    public void inheritFromOther(@Nonnull final ConstraintsMap otherConstraintsMap) {
        this.currentTick = otherConstraintsMap.currentTick;
        this.watermarkGoalTick = otherConstraintsMap.watermarkGoalTick;
        this.watermarkCommittedTick = otherConstraintsMap.watermarkCommittedTick;
        attributeToConstraintMap.clear();
        attributeToConstraintMap.putAll(otherConstraintsMap.attributeToConstraintMap);
    }

    private class ConstraintEntry {
        private long lastUpdatedTick;
        @Nonnull
        private Object property;

        public ConstraintEntry(@Nonnull final Object property) {
            this.lastUpdatedTick = currentTick;
            this.property = property;
        }
    }
}
